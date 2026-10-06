# keyednotifier

并发安全的按 key 版本化通知器。纯 Go 标准库实现，无第三方依赖，包含库、最小 CLI 与自动化测试。

## 语义

- `Publish(key, value)` / `Delete(key)`：每次修改都为该 key 生成严格递增、永不复用的 `Version`（从 1 开始）；`Delete` 产生 `Deleted=true` 的事件，删除后重新 `Publish` 继续使用更大的版本。
- `Wait(ctx, key, afterVersion)`：等待该 key 出现 `Version > afterVersion` 的下一状态变化；最新事件已满足则立即返回，否则阻塞等待。`Publish`/`Delete` 原子唤醒该 key 所有已满足条件的 waiter；`ctx` 取消只影响本次调用，取消与通知并发时只有一种结果成立。
- `Get` / `Snapshot` / `Len`：只统计当前未删除的 key；`Snapshot` 按 key 字典序返回独立副本。
- `Close`：拒绝后续修改，唤醒所有仍在等待的 `Wait` 并返回 `ErrClosed`；读取在关闭后仍可用，重复 `Close` 返回 `ErrClosed`。
- key 必须为非空字符串，否则返回 `ErrEmptyKey`。

## 用法

```go
n := keyednotifier.New()
defer n.Close()

ev, _ := n.Publish("alpha", "hello")      // Version=1
del, _ := n.Delete("alpha")              // Version=2, Deleted=true
ev, _ = n.Publish("alpha", "reborn")     // Version=3

// 等待 alpha 出现 Version > 3 的下一次变化
ev, err := n.Wait(ctx, "alpha", 3)

value, version, ok := n.Get("alpha")
entries := n.Snapshot() // 按 key 字典序
count := n.Len()
```

## 运行

```sh
go test -race ./...        # 运行全部测试（含竞态检测）
go run ./cmd/keyed-notifier  # 运行 CLI 演示
```

## 布局

- `notifier.go`：库实现（`package keyednotifier`）
- `notifier_test.go`：自动化测试
- `cmd/keyed-notifier/main.go`：最小 CLI 演示

