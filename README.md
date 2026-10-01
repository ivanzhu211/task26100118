# mirrordesk

进程内屏幕帧转发组件（Go 库）。把不可变 JPEG 帧按会话转发给调用方提供的
接收端，支持 50 个并发会话，慢接收端既不积压待发帧，也不影响其他会话。

## 核心语义

- 每会话最多持有一帧：从接受到完整输出期间，后续 `Offer` 直接丢弃，
  不保留候选帧；帧完整输出后才接受下一帧。
- 输出格式：4 字节无符号大端长度 + 原始帧数据；除 `Close` 外，已接受帧
  不截断、不替换、不交错。
- 限速：令牌桶，1 Mbps = 1 000 000 bit/s，帧头同样计费，只按 `TryWrite`
  实际接收的字节扣费。初始突发 65536 字节，`SetRate` 不会重新赠送突发。
  任意区间输出字节数 ≤ 65536 + 区间内按有效速率积分的额度。
- 四个操作可并发调用；不同会话不共享锁；不在持锁状态下等待可写、
  等待额度或等待后台任务退出。

## API

```go
h := mirrordesk.New() // 或 mirrordesk.New(mirrordesk.WithClock(clk))

err := h.Attach("sess-1", 20, sink) // 初始上限 5~50 Mbps
ok  := h.Offer("sess-1", jpeg)      // 立即返回是否接受，绝不等待
err  = h.SetRate("sess-1", 8)       // 对未发送字节生效，并唤醒发送任务
err  = h.Close("sess-1")            // 幂等；返回后不再向 sink 输出
```

## Sink 契约（由调用方实现）

```go
type Sink interface {
    // 立即返回实际接收的前 n 字节；允许部分接收或返回 0；不得阻塞。
    TryWrite(p []byte) (n int)
    // 阻塞至可写或 ctx 取消；不得丢失已出现的可写状态。
    WaitWritable(ctx context.Context) error
}
```

`fakeSink`（见 `sink_test.go`）是一个可控测试实现：可切换可写状态、
限制单次 `TryWrite` 接收量以制造部分写入，并记录全部接收字节。

## 测试

时间由 `ManualClock` 控制，接收端进度由 `fakeSink` 控制，测试不依赖
长时间真实休眠：

```sh
go test ./...            # 全部单元测试
go test -race ./...      # 竞态检测
```

覆盖：慢接收端隔离与逐字节还原、部分写入与返回 0 交替下的跨调速字节
上界、未输出字节不扣费、调速后及时恢复、等待可写/等待额度状态下与
`Offer`/`SetRate`/`Close` 交错、关闭后无输出且无残留任务、幂等关闭、
50 会话并发。
