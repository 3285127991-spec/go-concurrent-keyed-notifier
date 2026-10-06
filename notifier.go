// Package notifier implements a concurrency-safe keyed notifier.
//
// Each key has a strictly increasing version counter that starts at 1 and
// is never reused. Publish and Delete both produce a new Event with the next
// version; Delete marks the event as Deleted. Wait blocks until a key
// reaches a version greater than a caller-supplied version.
package notifier

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrEmptyKey is returned when an operation is attempted with an empty key.
	ErrEmptyKey = errors.New("notifier: key must not be empty")
	// ErrClosed is returned by mutating operations and Wait after Close.
	ErrClosed = errors.New("notifier: closed")
)

// Event is a single state change of a key.
type Event struct {
	Key     string
	Value   any
	Version uint64
	Deleted bool
}

// waitResult is the single message delivered to a registered waiter.
type waitResult struct {
	event Event
	err   error
}

// waiter is a registered Wait call. Its channel has capacity 1 and receives
// exactly one message, sent under the notifier lock at the moment the waiter
// is removed from the entry's waiter set.
type waiter struct {
	afterVersion uint64
	ch           chan waitResult
}

// entry holds the latest event for one key plus its registered waiters.
// Entries are never removed so versions are never reused.
type entry struct {
	latest  Event
	waiters map[*waiter]struct{}
}

// Notifier is a concurrency-safe keyed notifier. The zero value is not
// usable; construct it with New.
type Notifier struct {
	mu      sync.Mutex
	entries map[string]*entry
	closed  bool
}

// New returns a ready-to-use Notifier.
func New() *Notifier {
	return &Notifier{entries: make(map[string]*entry)}
}

// Publish stores value under key, advances the key's version, and atomically
// wakes every waiter on key whose afterVersion is now exceeded.
func (n *Notifier) Publish(key string, value any) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return Event{}, ErrClosed
	}
	e := n.entryLocked(key)
	e.latest = Event{Key: key, Value: value, Version: e.latest.Version + 1}
	n.notifyLocked(e)
	return e.latest, nil
}

// Delete marks key as deleted with a fresh version and atomically wakes
// satisfied waiters. The key no longer counts as existing for Get, Snapshot
// and Len, but its version counter is kept and never reused.
func (n *Notifier) Delete(key string) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return Event{}, ErrClosed
	}
	e := n.entryLocked(key)
	e.latest = Event{Key: key, Version: e.latest.Version + 1, Deleted: true}
	n.notifyLocked(e)
	return e.latest, nil
}

// Wait returns the latest event for key once its version is greater than
// afterVersion. If the current latest event already satisfies the condition
// it returns immediately. Otherwise it blocks until Publish or Delete
// satisfies the condition, ctx is canceled, or the notifier is closed.
// Canceling ctx affects only this call.
func (n *Notifier) Wait(ctx context.Context, key string, afterVersion uint64) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	e := n.entries[key]
	if e != nil && e.latest.Version > afterVersion {
		latest := e.latest
		n.mu.Unlock()
		return latest, nil
	}
	if n.closed {
		n.mu.Unlock()
		return Event{}, ErrClosed
	}
	if e == nil {
		e = n.entryLocked(key)
	}
	w := &waiter{afterVersion: afterVersion, ch: make(chan waitResult, 1)}
	e.waiters[w] = struct{}{}
	n.mu.Unlock()

	select {
	case res := <-w.ch:
		return res.event, res.err
	case <-ctx.Done():
		n.mu.Lock()
		if _, ok := e.waiters[w]; ok {
			// Cancellation wins: the waiter is still registered, so no
			// notification has been or will be delivered to it.
			delete(e.waiters, w)
			n.mu.Unlock()
			return Event{}, ctx.Err()
		}
		n.mu.Unlock()
		// A notification was already delivered under the lock; exactly one
		// message is buffered in w.ch.
		res := <-w.ch
		return res.event, res.err
	}
}

// Get returns the latest event for key if it currently exists (i.e. its
// latest event is not a deletion).
func (n *Notifier) Get(key string) (Event, bool) {
	if key == "" {
		return Event{}, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	e := n.entries[key]
	if e == nil || e.latest.Deleted {
		return Event{}, false
	}
	return e.latest, true
}

// Snapshot returns an independent slice of the latest events of all
// currently existing keys, sorted by key.
func (n *Notifier) Snapshot() []Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	keys := make([]string, 0, len(n.entries))
	for key, e := range n.entries {
		if !e.latest.Deleted {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	events := make([]Event, 0, len(keys))
	for _, key := range keys {
		events = append(events, n.entries[key].latest)
	}
	return events
}

// Len returns the number of currently existing (not deleted) keys.
func (n *Notifier) Len() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, e := range n.entries {
		if !e.latest.Deleted {
			count++
		}
	}
	return count
}

// Close marks the notifier closed. Pending Wait calls return ErrClosed and
// subsequent Publish, Delete and Wait calls are rejected. Get, Snapshot and
// Len remain usable. Closing twice returns ErrClosed.
func (n *Notifier) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	n.closed = true
	for _, e := range n.entries {
		for w := range e.waiters {
			delete(e.waiters, w)
			w.ch <- waitResult{err: ErrClosed}
		}
	}
	return nil
}

// entryLocked returns the entry for key, creating it on first use.
// n.mu must be held.
func (n *Notifier) entryLocked(key string) *entry {
	e := n.entries[key]
	if e == nil {
		e = &entry{waiters: make(map[*waiter]struct{})}
		n.entries[key] = e
	}
	return e
}

// notifyLocked wakes every waiter on e whose afterVersion is exceeded by the
// latest version. n.mu must be held, so the version bump and the wakeups are
// atomic with respect to Wait and cancellation.
func (n *Notifier) notifyLocked(e *entry) {
	for w := range e.waiters {
		if e.latest.Version > w.afterVersion {
			delete(e.waiters, w)
			w.ch <- waitResult{event: e.latest}
		}
	}
}
