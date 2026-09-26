# go-job-leases

可从工作进程崩溃中恢复的后台任务租约队列。任务支持定时执行、最大尝试次数与确定性退避重试；
工作者通过有时限的租约领取任务，崩溃后过期租约会被安全地重新分配；所有状态变化持久化，
完成记录与待通知 outbox 在同一事务中原子写入。

开发环境：Go 1.23.0。

## 核心概念

- **任务（Job）**：有外部任务号、负载、可执行时间 `RunAt`、最大尝试次数 `MaxAttempts` 和重试策略。
- **尝试号（Attempt）**：每成功领取一次，任务的尝试号单调递增（1, 2, 3, …）。
- **租约（Lease）**：领取时发放，包含租约号、尝试号、工作者 ID 和过期时间。心跳、完成、失败
  都必须携带**当前租约号 + 尝试号**。
- **任务状态**：`pending`（等待执行，含退避中）→ `leased`（有效租约持有）→ 终态
  `completed` 或 `dead`（死信）。
- **Outbox**：完成任务时，业务结果与一条待通知消息在同一个持久化事务中写入；
  通知由外部投递器取出 `PendingOutbox()`，投递成功后调用 `MarkOutboxSent()` 确认。

## 生命周期与规则

### 提交（幂等）

`Queue.Submit` 以外部任务号为幂等键：

| 情况 | 行为 |
| --- | --- |
| 外部任务号不存在 | 创建新任务，返回 `Created=true` |
| 同号 + 相同负载 | 返回原任务，`Created=false`（幂等重放） |
| 同号 + **不同负载** | 返回 `KindConflict` 错误 |

### 领取

`Queue.Lease` 批量领取**已到期**（`RunAt <= now`）的任务：

- 每次领取生成新租约，尝试号 +1；
- 未到执行时间的任务不可领取；
- 仍被有效租约持有的任务不可领取（多个工作者并发领取时，存储层串行事务保证
  同一次尝试绝不会发给两个工作者）；
- 工作者崩溃后租约到期，任务自动重新变为可领取，下一个工作者拿到的是**新的尝试号**。

### 心跳

`Queue.Heartbeat` 为当前租约延长过期时间。租约过期、租约号或尝试号不匹配均被拒绝。

### 完成

`Queue.Complete` 将任务标记为完成：

- 必须持有**当前且未过期**的租约、尝试号匹配；
- 完成记录与 outbox 通知原子写入（要么都可见，要么都不可见）；
- 租约过期后，迟到的旧尝试完成请求会被拒绝（`KindLease`），**不能覆盖**已发放的新尝试；
- 对同一租约重放完成请求：返回首次结果（`Replayed=true`），**不会重复写入 outbox**。

### 失败

`Queue.Fail` 报告一次尝试失败：

- `Retryable=true` 且未用尽 `MaxAttempts`：按确定性退避策略重新排期
  （`InitialDelay * Multiplier^(attempt-1)`，封顶 `MaxDelay`）；
- `Retryable=false`，或重试次数用尽：立即进入死信（`dead`）。

## 错误分类

所有错误均为 `*jobleases.Error`，用 `jobleases.IsKind(err, kind)` 判断：

| Kind | 含义 |
| --- | --- |
| `KindInvalidArgument` | 参数问题（字段缺失、租约时长非正、尝试号非正等） |
| `KindNotFound` | 任务或 outbox 条目不存在 |
| `KindConflict` | 幂等冲突：同外部任务号提交了不同负载 |
| `KindLease` | 租约问题：租约号/尝试号不匹配，或租约已过期 |
| `KindState` | 状态问题：任务已完成/已死信、当前未被领取等 |

## 持久化与崩溃恢复

提供两种 `Store` 实现：

- **`FileStore`**：每次状态变更把完整快照写入临时文件、`fsync` 后原子 `rename` 覆盖，
  并 `fsync` 目录。任何时刻进程崩溃，磁盘上要么是旧快照要么是新快照，重启后
  `NewFileStore(path)` 即可恢复全部状态（任务、租约、尝试号、完成记录、outbox）。
- **`MemoryStore`**：纯内存实现，用于测试。

`Store.Update` 是串行化事务：回调内对快照的修改只有在回调返回 nil 且持久化成功后才生效；
回调返回错误则整笔回滚。

## 快速上手

```go
store, err := jobleases.NewFileStore("data/queue.json")
if err != nil {
    log.Fatal(err)
}
q := jobleases.NewQueue(store)

// 提交（1s 初始退避、2 倍递增、封顶 1min、最多 3 次）
submitted, err := q.Submit(jobleases.SubmitInput{
    ExternalID:  "order-123",
    Payload:     []byte(`{"action":"charge"}`),
    MaxAttempts: 3,
    Retry:       jobleases.RetryPolicy{
        InitialDelay: time.Second,
        Multiplier:   2,
        MaxDelay:     time.Minute,
    },
})
if err != nil { /* 用 IsKind 分类处理 */ }

// 工作者领取
jobs, err := q.Lease(jobleases.LeaseInput{
    WorkerID: "worker-1", Count: 10, LeaseFor: 30 * time.Second,
})
for _, j := range jobs {
    lease := j.Lease
    // ... 执行任务 ...
    if err := doWork(j); err != nil {
        q.Fail(jobleases.FailInput{
            JobID: j.ID, LeaseID: lease.ID, Attempt: lease.Attempt,
            Reason: err.Error(), Retryable: true,
        })
        continue
    }
    q.Complete(jobleases.CompleteInput{
        JobID: j.ID, LeaseID: lease.ID, Attempt: lease.Attempt,
        Result: []byte("ok"),
    })
}

// 投递 outbox 通知（可在另一个循环中进行）
entries, _ := q.PendingOutbox()
for _, e := range entries {
    if err := notify(e); err == nil {
        q.MarkOutboxSent(e.ID)
    }
}
```

## API 一览

| 方法 | 说明 |
| --- | --- |
| `Submit(SubmitInput)` | 幂等提交任务 |
| `Lease(LeaseInput)` | 批量领取已到期任务，获得租约与递增尝试号 |
| `Heartbeat(HeartbeatInput)` | 租约续约 |
| `Complete(CompleteInput)` | 成功完成（原子写结果 + outbox；支持重放） |
| `Fail(FailInput)` | 报告失败，按策略重排或进入死信 |
| `Get(jobID)` / `GetByExternalID(extID)` | 任务查询 |
| `Stats()` | 队列状态汇总（pending/leased/completed/dead/due_now） |
| `PendingOutbox()` / `MarkOutboxSent(id)` | outbox 查询与确认 |

## 运行测试

    go test ./...

带竞态检测：

    go test -race ./...

测试覆盖：幂等提交与冲突、到期/未来任务领取、并发领取不重复发放同一尝试、
租约过期重新领取与递增尝试号、迟到旧尝试完成被拒、心跳续约、退避重排与死信、
不可重试失败立即死信、完成与 outbox 原子写入及重放不重复通知、队列状态统计、
以及 FileStore 崩溃后重开恢复全部状态。
