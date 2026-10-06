// Package keyednotifier 提供并发安全的按 key 版本化通知器。
//
// 每个 key 的 Version 从 1 开始严格递增且永不复用；Delete 也生成新
// Version（Deleted=true），删除后重新 Publish 继续使用更大的 Version。
// Wait 等待某 key 出现 Version 大于指定值的下一状态变化。
package keyednotifier

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrEmptyKey 表示传入了空 key。
	ErrEmptyKey = errors.New("keyednotifier: key must be a non-empty string")
	// ErrClosed 表示 Notifier 已关闭。
	ErrClosed = errors.New("keyednotifier: notifier is closed")
)

// Event 表示某个 key 的一次状态变化。
type Event struct {
	Key     string
	Value   string
	Version uint64
	Deleted bool
}

// Entry 是 Snapshot 返回的存活（未删除）键值对。
type Entry struct {
	Key     string
	Value   string
	Version uint64
}

// waitResult 是唤醒 waiter 时投递的结果：事件或关闭错误，二者必居其一。
type waitResult struct {
	event Event
	err   error
}

// waiter 的 channel 容量为 1，且只会被发送一次（发送后即从注册表移除），
// 因此发送永不阻塞，取消与通知竞争时也只有一种结果成立。
type waiter struct {
	afterVersion uint64
	ch           chan waitResult
}

type keyState struct {
	latest  Event
	waiters map[uint64]*waiter
	nextID  uint64
}

func newKeyState() *keyState {
	return &keyState{waiters: make(map[uint64]*waiter)}
}

// Notifier 是并发安全的 keyed notifier。零值不可用，请使用 New 创建。
type Notifier struct {
	mu     sync.Mutex
	closed bool
	keys   map[string]*keyState
}

// New 创建一个空的 Notifier。
func New() *Notifier {
	return &Notifier{keys: make(map[string]*keyState)}
}

// Publish 发布 key 的新值，Version 递增 1，并原子唤醒该 key 所有
// 已满足条件的 waiter。返回生成的事件。
func (n *Notifier) Publish(key, value string) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return Event{}, ErrClosed
	}
	ks := n.keyStateLocked(key)
	ev := Event{Key: key, Value: value, Version: ks.latest.Version + 1}
	ks.latest = ev
	notifyLocked(ks, ev)
	return ev, nil
}

// Delete 删除 key，同样生成一个严格递增的新 Version（Deleted=true），
// 并原子唤醒该 key 所有已满足条件的 waiter。返回生成的事件。
func (n *Notifier) Delete(key string) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return Event{}, ErrClosed
	}
	ks := n.keyStateLocked(key)
	ev := Event{Key: key, Version: ks.latest.Version + 1, Deleted: true}
	ks.latest = ev
	notifyLocked(ks, ev)
	return ev, nil
}

// Get 返回 key 当前的值与版本。key 不存在或已删除时 ok 为 false。
func (n *Notifier) Get(key string) (value string, version uint64, ok bool) {
	if key == "" {
		return "", 0, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	ks, exists := n.keys[key]
	if !exists || ks.latest.Version == 0 || ks.latest.Deleted {
		return "", 0, false
	}
	return ks.latest.Value, ks.latest.Version, true
}

// Wait 等待 key 出现 Version > afterVersion 的下一状态变化。
//
// 若当前最新事件已满足条件则立即返回；否则注册等待者并阻塞，直到被
// Publish/Delete 唤醒、ctx 取消或 Notifier 关闭。ctx 取消只影响本次
// 调用；取消与通知并发时只有一种结果成立。关闭后返回 ErrClosed。
func (n *Notifier) Wait(ctx context.Context, key string, afterVersion uint64) (Event, error) {
	if key == "" {
		return Event{}, ErrEmptyKey
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return Event{}, ErrClosed
	}
	ks, exists := n.keys[key]
	if exists && ks.latest.Version > afterVersion {
		ev := ks.latest
		n.mu.Unlock()
		return ev, nil
	}
	if !exists {
		ks = newKeyState()
		n.keys[key] = ks
	}
	w := &waiter{afterVersion: afterVersion, ch: make(chan waitResult, 1)}
	id := ks.nextID
	ks.nextID++
	ks.waiters[id] = w
	n.mu.Unlock()

	select {
	case res := <-w.ch:
		return res.event, res.err
	case <-ctx.Done():
		n.mu.Lock()
		if _, ok := ks.waiters[id]; ok {
			// 尚未被通知：移除自身，取消生效。
			delete(ks.waiters, id)
			n.mu.Unlock()
			return Event{}, ctx.Err()
		}
		n.mu.Unlock()
		// 通知已在锁内发出并移除了本 waiter，结果必然已在缓冲
		// channel 中，接收不会阻塞；通知生效，取消不生效。
		res := <-w.ch
		return res.event, res.err
	}
}

// Snapshot 按 key 字典序返回所有未删除键值对的独立副本。
func (n *Notifier) Snapshot() []Entry {
	n.mu.Lock()
	entries := make([]Entry, 0, len(n.keys))
	for key, ks := range n.keys {
		if ks.latest.Version == 0 || ks.latest.Deleted {
			continue
		}
		entries = append(entries, Entry{Key: key, Value: ks.latest.Value, Version: ks.latest.Version})
	}
	n.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

// Len 返回当前未删除的 key 数量。
func (n *Notifier) Len() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, ks := range n.keys {
		if ks.latest.Version > 0 && !ks.latest.Deleted {
			count++
		}
	}
	return count
}

// Close 关闭 Notifier：拒绝后续修改，并唤醒所有仍在等待的 Wait，
// 使其返回 ErrClosed。读取（Get/Snapshot/Len）在关闭后仍可用。
// 重复 Close 返回 ErrClosed。
func (n *Notifier) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	n.closed = true
	for _, ks := range n.keys {
		for id, w := range ks.waiters {
			w.ch <- waitResult{err: ErrClosed}
			delete(ks.waiters, id)
		}
	}
	return nil
}

// keyStateLocked 返回 key 对应的状态，不存在则创建。调用时必须持有锁。
func (n *Notifier) keyStateLocked(key string) *keyState {
	ks, ok := n.keys[key]
	if !ok {
		ks = newKeyState()
		n.keys[key] = ks
	}
	return ks
}

// notifyLocked 原子唤醒所有 afterVersion 小于 ev.Version 的 waiter。
// 调用时必须持有锁。
func notifyLocked(ks *keyState, ev Event) {
	for id, w := range ks.waiters {
		if w.afterVersion < ev.Version {
			w.ch <- waitResult{event: ev}
			delete(ks.waiters, id)
		}
	}
}
