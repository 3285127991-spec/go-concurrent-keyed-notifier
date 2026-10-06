package notifier_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	notifier "keyednotifier"
)

// waitRes carries the outcome of a Wait call across a channel.
type waitRes struct {
	event notifier.Event
	err   error
}

// await receives one Wait outcome, failing the test instead of hanging
// forever if the implementation deadlocks.
func await(t *testing.T, ch <-chan waitRes) waitRes {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Wait to return")
		return waitRes{}
	}
}

func waitAsync(n *notifier.Notifier, ctx context.Context, key string, after uint64) <-chan waitRes {
	ch := make(chan waitRes, 1)
	go func() {
		ev, err := n.Wait(ctx, key, after)
		ch <- waitRes{event: ev, err: err}
	}()
	return ch
}

func TestPublishIncrementsVersion(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	for want := uint64(1); want <= 3; want++ {
		ev, err := n.Publish("a", want)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if ev.Version != want {
			t.Fatalf("version = %d, want %d", ev.Version, want)
		}
		if ev.Deleted {
			t.Fatal("Publish event must not be marked deleted")
		}
	}

	// A different key has its own counter starting at 1.
	ev, err := n.Publish("b", "x")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.Version != 1 {
		t.Fatalf("version for new key = %d, want 1", ev.Version)
	}

	got, ok := n.Get("a")
	if !ok || got.Version != 3 || got.Value != uint64(3) {
		t.Fatalf("Get(a) = %+v, %v; want version 3 value 3", got, ok)
	}
}

func TestDeleteAndRepublish(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	del, err := n.Delete("k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if del.Version != 2 || !del.Deleted {
		t.Fatalf("Delete event = %+v, want version 2 deleted", del)
	}
	if _, ok := n.Get("k"); ok {
		t.Fatal("Get must report a deleted key as missing")
	}
	if n.Len() != 0 {
		t.Fatalf("Len = %d, want 0 after delete", n.Len())
	}

	// Republishing after delete continues the version sequence.
	ev, err := n.Publish("k", "v2")
	if err != nil {
		t.Fatalf("Publish after delete: %v", err)
	}
	if ev.Version != 3 || ev.Deleted {
		t.Fatalf("republish event = %+v, want version 3 not deleted", ev)
	}
	got, ok := n.Get("k")
	if !ok || got.Version != 3 || got.Value != "v2" {
		t.Fatalf("Get(k) = %+v, %v; want version 3 value v2", got, ok)
	}
}

func TestWaitImmediate(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	if _, err := n.Publish("k", "v"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ev, err := n.Wait(context.Background(), "k", 0)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.Version != 1 || ev.Value != "v" {
		t.Fatalf("Wait = %+v, want version 1 value v", ev)
	}

	// A delete event also satisfies an already-eligible Wait.
	if _, err := n.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ev, err = n.Wait(context.Background(), "k", 1)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.Version != 2 || !ev.Deleted {
		t.Fatalf("Wait = %+v, want version 2 deleted", ev)
	}
}

func TestWaitBlocksUntilPublish(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	resCh := waitAsync(n, context.Background(), "k", 1)

	// Whether or not the waiter registered first, it must observe version 2.
	if _, err := n.Publish("k", "v2"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	res := await(t, resCh)
	if res.err != nil {
		t.Fatalf("Wait: %v", res.err)
	}
	if res.event.Version != 2 || res.event.Value != "v2" {
		t.Fatalf("Wait = %+v, want version 2 value v2", res.event)
	}
}

func TestWaitMultipleWaiters(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	const waiters = 4
	start := make(chan struct{})
	results := make(chan waitRes, waiters)
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ev, err := n.Wait(context.Background(), "k", 1)
			results <- waitRes{event: ev, err: err}
		}()
	}
	close(start)

	// One publish must atomically wake every satisfied waiter.
	if _, err := n.Publish("k", "v2"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	wg.Wait()
	close(results)
	count := 0
	for res := range results {
		count++
		if res.err != nil {
			t.Fatalf("Wait: %v", res.err)
		}
		if res.event.Version != 2 || res.event.Value != "v2" {
			t.Fatalf("Wait = %+v, want version 2 value v2", res.event)
		}
	}
	if count != waiters {
		t.Fatalf("got %d results, want %d", count, waiters)
	}
}

func TestWaitContextCancel(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	ctx, cancel := context.WithCancel(context.Background())
	resCh := waitAsync(n, ctx, "k", 0)
	cancel()
	res := await(t, resCh)
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("Wait err = %v, want context.Canceled", res.err)
	}

	// Canceling one waiter must not affect other waiters on the same key.
	other := waitAsync(n, context.Background(), "k", 0)
	if _, err := n.Publish("k", "v"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	res = await(t, other)
	if res.err != nil || res.event.Version != 1 {
		t.Fatalf("Wait = %+v, %v; want version 1", res.event, res.err)
	}
}

func TestWaitCancelNotifyRace(t *testing.T) {
	// Cancellation and notification racing each other must yield exactly one
	// outcome: either the event or the context error, never both, never a
	// lost notification, never a deadlock.
	for i := 0; i < 50; i++ {
		n := notifier.New()
		ctx, cancel := context.WithCancel(context.Background())
		resCh := waitAsync(n, ctx, "k", 0)
		go cancel()
		if _, err := n.Publish("k", "v"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		res := await(t, resCh)
		if res.err != nil {
			if !errors.Is(res.err, context.Canceled) {
				t.Fatalf("iteration %d: err = %v, want context.Canceled", i, res.err)
			}
		} else if res.event.Version != 1 || res.event.Value != "v" {
			t.Fatalf("iteration %d: event = %+v, want version 1 value v", i, res.event)
		}
		cancel()
		if err := n.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestSnapshotAndLen(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	for _, kv := range []struct{ k, v string }{{"b", "1"}, {"a", "2"}, {"c", "3"}} {
		if _, err := n.Publish(kv.k, kv.v); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if _, err := n.Delete("b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	snap := n.Snapshot()
	if len(snap) != 2 || snap[0].Key != "a" || snap[1].Key != "c" {
		t.Fatalf("Snapshot = %+v, want keys [a c]", snap)
	}
	if n.Len() != 2 {
		t.Fatalf("Len = %d, want 2", n.Len())
	}

	// Mutating the returned slice must not affect the notifier.
	snap[0].Key = "zzz"
	snap[0].Value = "mutated"
	snap[0].Version = 99
	fresh := n.Snapshot()
	if len(fresh) != 2 || fresh[0].Key != "a" || fresh[0].Value != "2" || fresh[0].Version != 1 {
		t.Fatalf("Snapshot after mutation = %+v, want unchanged [a c]", fresh)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	if _, err := n.Publish("", "v"); !errors.Is(err, notifier.ErrEmptyKey) {
		t.Fatalf("Publish err = %v, want ErrEmptyKey", err)
	}
	if _, err := n.Delete(""); !errors.Is(err, notifier.ErrEmptyKey) {
		t.Fatalf("Delete err = %v, want ErrEmptyKey", err)
	}
	if _, err := n.Wait(context.Background(), "", 0); !errors.Is(err, notifier.ErrEmptyKey) {
		t.Fatalf("Wait err = %v, want ErrEmptyKey", err)
	}
	if _, ok := n.Get(""); ok {
		t.Fatal("Get with empty key must report missing")
	}
}

func TestClose(t *testing.T) {
	n := notifier.New()

	if _, err := n.Publish("k", "v"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pending := waitAsync(n, context.Background(), "k", 5)

	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The pending waiter is woken with ErrClosed (or rejects immediately if
	// it had not registered yet); both paths must surface ErrClosed.
	if res := await(t, pending); !errors.Is(res.err, notifier.ErrClosed) {
		t.Fatalf("pending Wait err = %v, want ErrClosed", res.err)
	}

	// New modifications are rejected.
	if _, err := n.Publish("k", "x"); !errors.Is(err, notifier.ErrClosed) {
		t.Fatalf("Publish err = %v, want ErrClosed", err)
	}
	if _, err := n.Delete("k"); !errors.Is(err, notifier.ErrClosed) {
		t.Fatalf("Delete err = %v, want ErrClosed", err)
	}
	// An unsatisfied Wait is rejected...
	if _, err := n.Wait(context.Background(), "k", 5); !errors.Is(err, notifier.ErrClosed) {
		t.Fatalf("Wait err = %v, want ErrClosed", err)
	}
	// ...while an already-satisfied Wait still returns the latest event.
	ev, err := n.Wait(context.Background(), "k", 0)
	if err != nil || ev.Version != 1 {
		t.Fatalf("satisfied Wait after Close = %+v, %v; want version 1", ev, err)
	}
	// Reads keep working.
	if _, ok := n.Get("k"); !ok {
		t.Fatal("Get must keep working after Close")
	}
	if n.Len() != 1 || len(n.Snapshot()) != 1 {
		t.Fatal("Len/Snapshot must keep working after Close")
	}
	// Closing twice reports ErrClosed.
	if err := n.Close(); !errors.Is(err, notifier.ErrClosed) {
		t.Fatalf("second Close err = %v, want ErrClosed", err)
	}
}

func TestConcurrent(t *testing.T) {
	n := notifier.New()
	defer n.Close()

	const publishers = 8
	const perPublisher = 25
	versions := make(chan uint64, publishers*perPublisher)

	var wg sync.WaitGroup
	for g := 0; g < publishers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				ev, err := n.Publish("hot", i)
				if err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
				versions <- ev.Version
			}
		}()
	}
	// Concurrent readers exercise the read paths under the race detector.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				n.Get("hot")
				n.Snapshot()
				n.Len()
			}
		}()
	}
	// Concurrent waiters on another key, woken by a single publish.
	const waiters = 4
	results := make(chan waitRes, waiters)
	for w := 0; w < waiters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev, err := n.Wait(context.Background(), "cold", 0)
			results <- waitRes{event: ev, err: err}
		}()
	}
	if _, err := n.Publish("cold", true); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	wg.Wait()
	close(versions)
	close(results)

	seen := make(map[uint64]bool, publishers*perPublisher)
	for v := range versions {
		if seen[v] {
			t.Fatalf("version %d delivered twice", v)
		}
		seen[v] = true
	}
	for v := uint64(1); v <= publishers*perPublisher; v++ {
		if !seen[v] {
			t.Fatalf("version %d missing", v)
		}
	}
	if got, ok := n.Get("hot"); !ok || got.Version != publishers*perPublisher {
		t.Fatalf("Get(hot) = %+v, %v; want version %d", got, ok, publishers*perPublisher)
	}
	count := 0
	for res := range results {
		count++
		if res.err != nil || res.event.Version != 1 {
			t.Fatalf("waiter = %+v, %v; want version 1", res.event, res.err)
		}
	}
	if count != waiters {
		t.Fatalf("got %d waiter results, want %d", count, waiters)
	}
}
