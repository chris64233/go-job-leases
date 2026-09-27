# go-job-leases

可从工作进程崩溃中恢复的后台任务租约队列（Go 1.23，无第三方依赖）。

## 核心语义

- **幂等提交**：任务以外部任务号（`TaskID`）为幂等键。同号同负载返回原任务；
  同号不同负载返回 `KindConflict` 冲突。
- **租约领取**：工作者批量领取已到期任务，每次领取获得一个有期限的租约和
  单调递增的尝试号。并发领取由互斥锁 + 持久化保证，同一次尝试只会发给一个
  工作者；未到执行时间或仍被有效租约持有的任务不可领取。
- **租约过期**：租约过期后任务重新变为可领取（尝试号递增）；若尝试次数已用尽
  则进入死信。
- **确定性重试**：可重试失败按 `RetryPolicy`（指数退避 `Base * 2^(n-1)`，封顶
  `MaxDelay`，无随机抖动）重排到 `now + Delay(attempt)`；次数用尽或不可重试
  失败进入死信。
- **崩溃恢复**：领取、心跳、完成、失败等所有状态变化先落盘（临时文件 + fsync +
  rename 原子替换）再返回，进程崩溃后从 `FileStore` 重建队列即可恢复。
- **完成 + outbox 原子写入**：业务完成记录与待通知 outbox 消息在同一次持锁
  修改中写入并一起持久化。完成请求重放时返回首次的完成记录，不重复通知。
- **迟到操作防护**：心跳、完成、失败都必须携带当前租约号与尝试号。租约过期后
  的迟到完成返回 `KindLeaseMismatch`，不会覆盖正在进行的新尝试。
- **前置任务依赖**：提交时用可选的 `dependsOn...` 引用**已存在**的前置任务
  （可多个，自动去重；自依赖/成环返回 `KindCycleDependency`）。
  - 有未满足前置的任务处于 `waiting`，不会被领取；
  - 前置**全部完成**后，在完成操作的同一次持锁 + 同一次持久化中唯一地转为
    `pending`（并发完成也只转换一次），可立即被领取，不会提前租出或重复入队；
  - 任一前置最终进入死信（耗尽重试、不可重试失败、租约过期且次数用尽），后继
    立即转为 `blocked` 终态并在 `BlockedReason` 记录直接阻断它的前置、前置终态
    与原因，阻断沿依赖链逐层级联，任务不会一直停在等待中；
  - 提交时前置已在终态的，直接以 `pending`（全完成）或 `blocked`（有失败）起步；
  - 依赖列表、未决集合、释放时间 `ReleasedAt`、阻断原因都在快照中持久化，重启恢复。

## API 概览

```go
store, _ := jobleases.NewFileStore("/var/lib/myqueue")
q, _ := jobleases.NewQueue(store) // 崩溃后重新调用即恢复

// 提交（幂等）
q.Submit("order-123", payload, runAt, 3 /* maxAttempts */)

// 提交带前置依赖的任务：build、test 全部完成后 deploy 才可被领取
q.Submit("build", payload, time.Now(), 3)
q.Submit("test", payload, time.Now(), 3, "build")
q.Submit("deploy", payload, time.Now(), 3, "build", "test") // 多前置，自动去重

// 领取（批量，带租约）
leases, _ := q.Claim(10, 30*time.Second)

// 心跳续租 / 完成（自动释放后继）/ 失败（死信自动阻断后继）
q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt, 30*time.Second)
q.Complete(l.TaskID, l.LeaseID, l.Attempt, result)
q.Fail(l.TaskID, l.LeaseID, l.Attempt, "cause", true /* retryable */)

// 查询：Get 返回的快照含 Status / Dependencies / ReleasedAt / BlockedReason
j, _ := q.Get("deploy")
if j.Status == jobleases.StatusBlocked {
    log.Print(j.BlockedReason.String()) // 例如：upstream test is dead: boom
}
q.Stats()       // 队列状态概览
q.Outbox()      // 待通知消息
q.AckOutbox(id) // 通知投递成功后确认
```

## 错误分类

所有错误都是 `*jobleases.Error`，用 `KindOf(err)` 区分：

| Kind | 含义 |
|---|---|
| `KindInvalidArgument` | 参数问题（空任务号、非法批量大小等） |
| `KindNotFound` | 任务或 outbox 消息不存在 |
| `KindConflict` | 幂等冲突：同任务号不同负载 |
| `KindCycleDependency` | 依赖成环（含自依赖） |
| `KindLeaseMismatch` | 租约号/尝试号不匹配，或租约已过期（迟到操作） |
| `KindInvalidState` | 任务已是终态（完成/死信/阻断） |
| `KindInternal` | 持久化等内部错误 |

## 持久化

`Store` 接口抽象持久化：`FileStore` 将全量状态以 JSON 快照原子写入
（写临时文件 → fsync → rename），`MemoryStore` 用于测试。`NewQueue` 启动时
从 `Store` 加载状态完成恢复。

## 测试

    go test ./...
    go test -race ./...

覆盖：提交幂等与冲突、到期/租约排除、并发领取不重复、心跳续租与迟到拒绝、
完成 + outbox 原子性与重放、迟到完成不覆盖新尝试、重试重排与死信、
崩溃恢复（FileStore 重建）、统计与 outbox 确认；
依赖专项：等待/释放、多前置 fan-in、并发完成只释放一次、死信级联阻断与原因、
终态前置提交、环与参数校验、依赖与阻断原因的崩溃恢复。
