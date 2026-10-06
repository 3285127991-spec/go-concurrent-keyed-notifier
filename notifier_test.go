package keyednotifier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type waitOutcome struct {
	ev  Event
	err error
}

func waitAsync(n *Notifier, ctx context.Context, key string, after uint64) <-chan waitOutcome {
	done := make(chan waitOutcome, 1)
	go func() {
		ev, err := n.Wait(ctx, key, after)
		done <- waitOutcome{ev, err}
	}()
	return done
}

func TestVersionIncrements(t *testing.T) {
	n := New()
	defer n.Close()

	for want := uint64(1); want <= 3; want++ {
		ev, err := n.Publish("a", fmt.Sprintf("v%d", want))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if ev.Version != want || ev.Deleted {
			t.Fatalf("event = %+v, want version=%d deleted=false", ev, want)
		}
	}
	// 不同 key 的版本相互独立，均从 1 开始。
	ev, err := n.Publish("b", "x")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.Version != 1 {
		t.Fatalf("independent key version = %d, want 1", ev.Version)
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	n := New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	del, err := n.Delete("k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if del.Version != 2 || !del.Deleted {
		t.Fatalf("delete event = %+v, want version=2 deleted=true", del)
	}
	if _, _, ok := n.Get("k"); ok {
		t.Fatal("Get after Delete should report missing")
	}
	// 删除事件对 Wait 可见。
	ev, err := n.Wait(context.Background(), "k", 1)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.Version != 2 || !ev.Deleted {
		t.Fatalf("Wait(after=1) = %+v, want deleted version=2 event", ev)
	}
	// 删除后重建：版本继续递增，不复用。
	re, err := n.Publish("k", "reborn")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if re.Version != 3 || re.Deleted {
		t.Fatalf("republish = %+v, want version=3 deleted=false", re)
	}
	if value, version, ok := n.Get("k"); !ok || value != "reborn" || version != 3 {
		t.Fatalf("Get = %q, %d, %v; want %q, 3, true", value, version, ok, "reborn")
	}
}

func TestWaitImmediate(t *testing.T) {
	n := New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// 最新事件已满足条件，立即返回，不阻塞。
	ev, err := n.Wait(context.Background(), "k", 0)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.Version != 1 || ev.Value != "v1" || ev.Deleted {
		t.Fatalf("event = %+v, want version=1 value=v1", ev)
	}
}

func TestWaitBlocking(t *testing.T) {
	n := New()
	defer n.Close()

	done := waitAsync(n, context.Background(), "k", 0)
	pub, err := n.Publish("k", "v1")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("Wait: %v", res.err)
	}
	if res.ev.Version != pub.Version || res.ev.Value != "v1" || res.ev.Deleted {
		t.Fatalf("waiter got %+v, want %+v", res.ev, pub)
	}
}

func TestMultipleWaiters(t *testing.T) {
	n := New()
	defer n.Close()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	const waiters = 3
	results := make(chan waitOutcome, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			ev, err := n.Wait(context.Background(), "k", 1)
			results <- waitOutcome{ev, err}
		}()
	}
	if _, err := n.Publish("k", "v2"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// 所有 waiter 都必须被同一个事件原子唤醒。
	for i := 0; i < waiters; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("waiter %d: %v", i, res.err)
		}
		if res.ev.Version != 2 || res.ev.Value != "v2" || res.ev.Deleted {
			t.Fatalf("waiter %d got %+v, want version=2 value=v2", i, res.ev)
		}
	}
}

func TestWaitCancel(t *testing.T) {
	t.Run("OnlyAffectsSelf", func(t *testing.T) {
		n := New()
		defer n.Close()

		ctx, cancel := context.WithCancel(context.Background())
		canceled := waitAsync(n, ctx, "k", 0)
		// 先取消再发布：被取消的 waiter 只能以取消结束。
		cancel()
		res := <-canceled
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("canceled waiter err = %v, want context.Canceled", res.err)
		}
		// 同 key 的其他 waiter 不受影响，正常收到通知。
		survivor := waitAsync(n, context.Background(), "k", 0)
		if _, err := n.Publish("k", "v1"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		res = <-survivor
		if res.err != nil || res.ev.Version != 1 || res.ev.Value != "v1" {
			t.Fatalf("survivor = %+v, %v; want version=1 value=v1", res.ev, res.err)
		}
	})

	t.Run("NotifyRace", func(t *testing.T) {
		n := New()
		defer n.Close()

		ctx, cancel := context.WithCancel(context.Background())
		done := waitAsync(n, ctx, "k", 0)
		// 取消与发布并发：只允许一种结果成立，且必须返回一次。
		go cancel()
		if _, err := n.Publish("k", "v1"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		res := <-done
		switch {
		case errors.Is(res.err, context.Canceled):
			// 取消获胜，合法。
		case res.err == nil && res.ev.Version == 1 && res.ev.Value == "v1" && !res.ev.Deleted:
			// 通知获胜，合法。
		default:
			t.Fatalf("unexpected outcome: %+v, %v", res.ev, res.err)
		}
	})
}

func TestSnapshotAndLen(t *testing.T) {
	n := New()
	defer n.Close()

	for _, kv := range [][2]string{{"c", "3"}, {"a", "1"}, {"b", "2"}} {
		if _, err := n.Publish(kv[0], kv[1]); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if _, err := n.Delete("b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := n.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	// 按 key 字典序，且不含已删除的 b。
	snap := n.Snapshot()
	if len(snap) != 2 || snap[0].Key != "a" || snap[1].Key != "c" {
		t.Fatalf("Snapshot = %+v, want keys [a c]", snap)
	}
	if snap[0].Value != "1" || snap[0].Version != 1 || snap[1].Value != "3" || snap[1].Version != 1 {
		t.Fatalf("Snapshot entries = %+v", snap)
	}
	// 返回值是独立副本，修改不影响内部状态。
	snap[0].Key = "zzz"
	snap[0].Value = "mutated"
	again := n.Snapshot()
	if again[0].Key != "a" || again[0].Value != "1" {
		t.Fatalf("Snapshot not independent: %+v", again)
	}
	if _, _, ok := n.Get("b"); ok {
		t.Fatal("Get(deleted) should report missing")
	}
	if _, _, ok := n.Get("missing"); ok {
		t.Fatal("Get(missing) should report missing")
	}
}

func TestInvalidKey(t *testing.T) {
	n := New()
	defer n.Close()

	if _, err := n.Publish("", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Publish empty key err = %v, want ErrEmptyKey", err)
	}
	if _, err := n.Delete(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete empty key err = %v, want ErrEmptyKey", err)
	}
	if _, err := n.Wait(context.Background(), "", 0); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Wait empty key err = %v, want ErrEmptyKey", err)
	}
	if _, _, ok := n.Get(""); ok {
		t.Fatal("Get empty key should report missing")
	}
	if got := n.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

func TestClose(t *testing.T) {
	n := New()

	if _, err := n.Publish("k", "v1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	blocked := waitAsync(n, context.Background(), "k", 1)
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 仍在等待的 Wait 被唤醒并返回关闭错误。
	if res := <-blocked; !errors.Is(res.err, ErrClosed) {
		t.Fatalf("blocked Wait err = %v, want ErrClosed", res.err)
	}
	// 关闭后拒绝新的修改与等待。
	if _, err := n.Publish("k", "v2"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close err = %v, want ErrClosed", err)
	}
	if _, err := n.Delete("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close err = %v, want ErrClosed", err)
	}
	if _, err := n.Wait(context.Background(), "k", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Wait after Close err = %v, want ErrClosed", err)
	}
	if err := n.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close err = %v, want ErrClosed", err)
	}
	// 读取在关闭后仍可用。
	if value, version, ok := n.Get("k"); !ok || value != "v1" || version != 1 {
		t.Fatalf("Get after Close = %q, %d, %v", value, version, ok)
	}
	if got := n.Len(); got != 1 {
		t.Fatalf("Len after Close = %d, want 1", got)
	}
}

func TestBasicConcurrency(t *testing.T) {
	n := New()
	defer n.Close()

	const keys = 4
	const pubsPerKey = 5
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%d", k)
		// 每个 key 一个写者：版本应严格为 1..pubsPerKey。
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := uint64(1); v <= pubsPerKey; v++ {
				ev, err := n.Publish(key, fmt.Sprintf("v%d", v))
				if err != nil {
					t.Errorf("Publish(%s): %v", key, err)
					return
				}
				if ev.Version != v {
					t.Errorf("Publish(%s) version = %d, want %d", key, ev.Version, v)
					return
				}
			}
		}()
		// 每个 key 一个等待者：最终都应看到第 pubsPerKey 版。
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev, err := n.Wait(context.Background(), key, pubsPerKey-1)
			if err != nil {
				t.Errorf("Wait(%s): %v", key, err)
				return
			}
			if ev.Version != pubsPerKey || ev.Deleted {
				t.Errorf("Wait(%s) = %+v, want version=%d", key, ev, pubsPerKey)
			}
		}()
	}
	wg.Wait()

	if got := n.Len(); got != keys {
		t.Fatalf("Len = %d, want %d", got, keys)
	}
	snap := n.Snapshot()
	if len(snap) != keys {
		t.Fatalf("Snapshot len = %d, want %d", len(snap), keys)
	}
	for i, e := range snap {
		if i > 0 && snap[i-1].Key >= e.Key {
			t.Fatalf("Snapshot not sorted: %+v", snap)
		}
		if e.Version != pubsPerKey {
			t.Fatalf("Snapshot entry = %+v, want version=%d", e, pubsPerKey)
		}
	}
}
