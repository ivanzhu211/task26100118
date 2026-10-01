# mirrordesk

MirrorDesk 的进程内屏幕帧转发组件。每个会话把调用方提交的 JPEG 帧
（4 字节大端长度头 + 原始帧数据）写给同进程内的接收端，支持数十个
（50+）同时活动的会话，且慢接收端不会制造待发帧积压、不会拖住其他会话。

## 核心性质

- **零积压**：每个会话最多持有一帧。一帧从接受到全部输出期间，其他
  `Offer` 直接丢弃（不保留候选帧）；帧完整输出后才接受下一帧。
- **会话隔离**：每个会话一个发送协程、一套锁；无全局锁串行化，
  慢会话不影响其他会话。
- **带宽上限**：令牌桶容量 65536 字节（即初始突发额度）。任意时间区间
  内成功输出的字节数 ≤ 65536 + 该区间按有效速率积分得到的额度
  （1 Mbps = 1_000_000 bit/s，帧头同帧体一样计费，且只扣除
  `TryWrite` 实际接收的字节）。`SetRate` 对未发送字节即时生效，
  但不会重新赠送突发额度。
- **持续消耗**：接收端可写且有帧可发时，发送协程持续消耗可用额度，
  不会靠恒定低速或拒收来满足上限。
- **可关闭**：`Close` 幂等；返回后不再向该 sink 输出任何字节，
  会话持有的帧与发送协程随即释放。除 `Close` 外，已接受帧不会被
  截断、替换或与下一帧交错。

## API

```go
hub := mirrordesk.NewHub(nil) // nil 使用真实时钟；测试可注入 ManualClock

err := hub.Attach("session-1", 20, sink) // 初始上限 5~50 Mbps
ok := hub.Offer("session-1", jpegBytes)  // 立即返回：接受或丢弃，帧 ≤ 2 MiB
ok  = hub.SetRate("session-1", 40)      // 调整上限，对未发送字节生效
hub.Close("session-1")                   // 幂等；返回后不再有输出
```

四个操作都可并发调用，接受、丢弃、调速与关闭具有可解释的线性化顺序；
与 `Close` 交错的 `Offer` 可能先被接受，但关闭后不会继续输出。

## 接收端契约（Sink）

接收端由调用方在同一进程内提供，组件不做真实传输：

```go
type Sink interface {
    // 立即返回实际接收的前 n 字节；允许部分接收或返回 0。
    TryWrite(p []byte) (n int)
    // 等待可写或 ctx 取消；不得丢失已经出现的可写状态。
    WaitWritable(ctx context.Context)
}
```

约定：`TryWrite` 返回 0 表示暂时不可写；`WaitWritable` 返回后
`TryWrite` 应能接收至少 1 字节（允许偶发的虚假唤醒，调用方会重试）。

## 测试

测试不依赖长时间真实休眠：时间由 `ManualClock` 显式推进，接收端进度
由可控的 `testSink` 驱动（部分接收、返回 0、可写状态切换）。

```sh
go test ./...        # 全部测试
go test -race .      # 竞态检测
go test -count=5 .   # 重复执行验证稳定性
```

覆盖场景：

- `TestSlowReceiverDoesNotBlockOthers`：会话 A 在帧体部分输出后停止接收，
  连续 100 次新帧均被丢弃；会话 B 照常完成；A 恢复后输出逐字节还原原帧。
- `TestRateLimitAcrossSetRate`：接收端交替部分接收与返回 0，额度耗尽时
  连续降低、提高上限；重放令牌桶核验跨调速区间的字节上界、未输出字节
  不扣费、提高上限后及时恢复推进。
- `TestCloseWhileWaitingWritable` / `TestCloseWhileWaitingCredit` /
  `TestConcurrentOpsInterleave`：发送任务分别等待可写与等待额度时交错
  Offer/SetRate/Close；Close 必完成，之后无新输出、无残留任务，重复
  关闭仍成功。
