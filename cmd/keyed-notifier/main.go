// Command keyed-notifier demonstrates the keyednotifier library:
// Publish, Wait (immediate and blocking), Delete-then-recreate, and Close.
package main

import (
	"context"
	"fmt"
	"log"

	"keyednotifier"
)

func main() {
	n := keyednotifier.New()

	fmt.Println("== Publish ==")
	ev, err := n.Publish("alpha", "hello")
	must(err)
	fmt.Printf("publish alpha %-10q -> version=%d\n", ev.Value, ev.Version)
	ev, err = n.Publish("alpha", "hello v2")
	must(err)
	fmt.Printf("publish alpha %-10q -> version=%d\n", ev.Value, ev.Version)

	fmt.Println("== Wait ==")
	// The latest event already satisfies afterVersion=1: returns immediately.
	ev, err = n.Wait(context.Background(), "alpha", 1)
	must(err)
	fmt.Printf("wait(alpha, after=1)  -> version=%d value=%q (already satisfied)\n", ev.Version, ev.Value)

	// Blocking wait: woken when beta is published.
	done := make(chan keyednotifier.Event, 1)
	go func() {
		ev, err := n.Wait(context.Background(), "beta", 0)
		if err != nil {
			log.Printf("wait(beta): %v", err)
			return
		}
		done <- ev
	}()
	fmt.Println("wait(beta, after=0)   -> blocking ...")
	ev, err = n.Publish("beta", "wake up")
	must(err)
	fmt.Printf("publish beta %-10q -> version=%d\n", ev.Value, ev.Version)
	ev = <-done
	fmt.Printf("wait(beta, after=0)   -> version=%d value=%q (woken by publish)\n", ev.Version, ev.Value)

	fmt.Println("== Delete and recreate ==")
	ev, err = n.Delete("alpha")
	must(err)
	fmt.Printf("delete alpha          -> version=%d deleted=%v\n", ev.Version, ev.Deleted)
	if _, _, ok := n.Get("alpha"); !ok {
		fmt.Println("get alpha             -> missing (deleted)")
	}
	ev, err = n.Publish("alpha", "reborn")
	must(err)
	fmt.Printf("publish alpha %-10q -> version=%d (version keeps increasing)\n", ev.Value, ev.Version)
	fmt.Printf("snapshot=%+v len=%d\n", n.Snapshot(), n.Len())

	fmt.Println("== Close ==")
	must(n.Close())
	if _, err := n.Publish("alpha", "x"); err != nil {
		fmt.Printf("publish after close   -> rejected: %v\n", err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
