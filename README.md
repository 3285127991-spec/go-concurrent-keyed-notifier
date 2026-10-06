# keyednotifier

A concurrency-safe keyed notifier for Go. Pure standard library, no
third-party dependencies, no network, no persistence.

Each key has a version counter starting at 1 that strictly increases and is
never reused. `Publish` and `Delete` both advance the version and atomically
wake every waiter whose condition is satisfied. `Delete` produces an event
with `Deleted=true`; republishing the key continues with a larger version.

## API

```go
n := notifier.New()

ev, err := n.Publish("foo", "hello")  // -> version 1
ev, err := n.Delete("foo")            // -> version 2, Deleted=true
ev, ok  := n.Get("foo")               // latest event, only if not deleted
ev, err := n.Wait(ctx, "foo", 2)      // next event with Version > 2
events  := n.Snapshot()               // live keys, sorted by key, independent copy
count   := n.Len()                    // number of live (not deleted) keys
err      = n.Close()                  // reject writes, wake waiters with ErrClosed
```

- Keys must be non-empty strings (`ErrEmptyKey`).
- `Wait` returns immediately if the latest event already exceeds
  `afterVersion`; otherwise it blocks until a matching `Publish`/`Delete`,
  context cancellation (affects only that call), or `Close` (`ErrClosed`).
- `Get`, `Snapshot` and `Len` treat deleted keys as absent.
- After `Close`, `Publish`/`Delete`/`Wait` return `ErrClosed`; `Get`,
  `Snapshot` and `Len` keep working.

## Run the demo CLI

```sh
go run ./cmd/notifier-demo
```

## Run the tests

```sh
go test -race ./...
```

## Layout

- `notifier.go` - the library (package `notifier`)
- `notifier_test.go` - tests for versioning, delete/republish, immediate and
  blocking `Wait`, multiple waiters, cancel/notify race, snapshot/len,
  invalid keys, close, and basic concurrency
- `cmd/notifier-demo/` - minimal CLI demo
