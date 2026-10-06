// Command notifier-demo demonstrates the keyednotifier library: Publish,
// Wait (immediate and blocking), Delete followed by republish, and Close.
package main

import (
	"context"
	"fmt"
	"sync"

	notifier "keyednotifier"
)

func main() {
	n := notifier.New()
	ctx := context.Background()

	// Publish creates the key at version 1.
	ev, _ := n.Publish("foo", "hello")
	fmt.Printf("Publish(foo)        -> version=%d value=%v\n", ev.Version, ev.Value)

	// Wait with an already-satisfied version returns immediately.
	ev, _ = n.Wait(ctx, "foo", 0)
	fmt.Printf("Wait(foo, after=0)  -> version=%d value=%v (immediate)\n", ev.Version, ev.Value)

	// A blocking Wait unblocks when a later version is published.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ev, err := n.Wait(ctx, "foo", 2)
		if err != nil {
			fmt.Printf("Wait(foo, after=2)  -> %v\n", err)
			return
		}
		fmt.Printf("Wait(foo, after=2)  -> version=%d value=%v (unblocked)\n", ev.Version, ev.Value)
	}()

	// Delete bumps the version and marks the event deleted...
	ev, _ = n.Delete("foo")
	fmt.Printf("Delete(foo)         -> version=%d deleted=%v\n", ev.Version, ev.Deleted)

	// ...and republishing continues the version sequence, waking the waiter.
	ev, _ = n.Publish("foo", "world")
	fmt.Printf("Publish(foo) again  -> version=%d value=%v\n", ev.Version, ev.Value)
	wg.Wait()

	// Close rejects further modifications.
	_ = n.Close()
	if _, err := n.Publish("foo", "x"); err != nil {
		fmt.Printf("Publish after Close -> %v\n", err)
	}
}
