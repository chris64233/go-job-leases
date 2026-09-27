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
- **前置任务依赖**：`SubmitWithDeps` 可引用已存在的前置任务（不存在返回
  `KindNotFound`，自我依赖/循环依赖返回 `KindDependency`）。全部前置完成前
  任务处于 `waiting` 不可领取；最后一个前置完成时，在同一次持锁修改中原子
  转为 `pending` 并记录 `ReleasedAt`——多个前置并发完成也只转换一次，不会
  提前租出或重复入队。任一前置死信（含租约过期耗尽）或被阻断时，等待中的
  后续任务转为终态 `blocked` 并记录 `BlockedReason`（级联阻断），不会永远
  停留在等待中。依赖关系、阻断原因与释放时间均随任务持久化，重启后恢复。
- **迟到操作防护**：心跳、完成、失败都必须携带当前租约号与尝试号。租约过期后
  的迟到完成返回 `KindLeaseMismatch`，不会覆盖正在进行的新尝试。

## API 概览

```go
store, _ := jobleases.NewFileStore("/var/lib/myqueue")
q, _ := jobleases.NewQueue(store) // 崩溃后重新调用即恢复

// 提交（幂等）
q.Submit("order-123", payload, runAt, 3 /* maxAttempts */)

// 提交带前置依赖的任务：全部前置完成后才可领取
q.SubmitWithDeps("notify-123", payload, runAt, 3, []string{"order-123", "charge-123"})

// 领取（批量，带租约）
leases, _ := q.Claim(10, 30*time.Second)

// 心跳续租 / 完成 / 失败，均需携带租约号与尝试号
q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt, 30*time.Second)
q.Complete(l.TaskID, l.LeaseID, l.Attempt, result)
q.Fail(l.TaskID, l.LeaseID, l.Attempt, "cause", true /* retryable */)

// 查询
q.Get(taskID)   // 单个任务快照
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
| `KindLeaseMismatch` | 租约号/尝试号不匹配，或租约已过期（迟到操作） |
| `KindInvalidState` | 任务已是终态（完成/死信/阻断） |
| `KindDependency` | 依赖问题：自我依赖或循环依赖 |
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
崩溃恢复（FileStore 重建）、统计与 outbox 确认、前置依赖（等待/释放/
阻断级联/并发完成只释放一次/依赖校验/重启恢复）。
