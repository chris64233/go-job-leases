package jobleases

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Queue 是后台任务租约队列。所有状态变化先加锁修改内存状态、再原子持久化，
// 持久化成功后才返回，因此进程崩溃后从 Store 恢复即可继续。
type Queue struct {
	mu     sync.Mutex
	store  Store
	st     *state
	policy RetryPolicy
	now    func() time.Time
}

// Option 队列可选配置。
type Option func(*Queue)

// WithRetryPolicy 覆盖默认重试策略。
func WithRetryPolicy(p RetryPolicy) Option {
	return func(q *Queue) { q.policy = p }
}

// WithClock 注入时钟，便于测试；默认 time.Now。
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// NewQueue 从 Store 恢复状态并返回队列。崩溃前的领取、完成、死信、依赖关系、
// 阻断原因等状态都会还原。
func NewQueue(store Store, opts ...Option) (*Queue, error) {
	st, err := store.Load()
	if err != nil {
		return nil, internalErr("load", err)
	}
	st.normalize()
	q := &Queue{store: store, st: st, policy: DefaultRetryPolicy, now: time.Now}
	for _, o := range opts {
		o(q)
	}
	return q, nil
}

// persist 在持锁状态下落盘；失败时返回内部错误，调用方不应继续。
func (q *Queue) persist(op string) error {
	if err := q.store.Save(q.st); err != nil {
		return internalErr(op, err)
	}
	return nil
}

func (q *Queue) nextID(prefix string) string {
	q.st.Seq++
	return fmt.Sprintf("%s-%d", prefix, q.st.Seq)
}

// Submit 提交任务。以 TaskID 幂等：同号同负载返回已存在的任务；
// 同号不同负载返回 KindConflict 冲突。
//
// dependsOn 列出前置任务号：这些任务必须全部进入完成态，本任务才会从
// waiting 释放为 pending 并可被领取；任一前置最终进入死信（或被级联阻断），
// 本任务立即转为 blocked 终态并记录阻断原因。前置任务必须已经存在，
// 依赖列表会去重（保留首次出现顺序），自依赖及任何会形成环的引用都以
// KindCycleDependency 拒绝。
func (q *Queue) Submit(taskID string, payload []byte, runAt time.Time, maxAttempts int, dependsOn ...string) (*Job, error) {
	const op = "submit"
	if taskID == "" {
		return nil, invalidArg(op, "task id is required")
	}
	if maxAttempts < 1 {
		return nil, invalidArg(op, "max attempts must be >= 1")
	}
	// 先做不需要持锁的去空/去重。
	deps := make([]string, 0, len(dependsOn))
	seen := make(map[string]bool, len(dependsOn))
	for _, d := range dependsOn {
		if d == "" {
			return nil, invalidArg(op, "dependency task id must not be empty")
		}
		if d == taskID {
			return nil, cycleErr(op, "task cannot depend on itself")
		}
		if seen[d] {
			continue
		}
		seen[d] = true
		deps = append(deps, d)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.st.Jobs[taskID]; ok {
		if !bytes.Equal(existing.Payload, payload) {
			return nil, conflict(op, "task id already exists with a different payload")
		}
		return existing, nil
	}
	for _, d := range deps {
		if _, ok := q.st.Jobs[d]; !ok {
			return nil, invalidArg(op, fmt.Sprintf("dependency %q does not exist", d))
		}
	}
	if q.wouldCycle(taskID, deps) {
		return nil, cycleErr(op, "dependency chain would form a cycle")
	}
	now := q.now()
	job := &Job{
		TaskID:       taskID,
		Payload:      payload,
		RunAt:        runAt,
		MaxAttempts:  maxAttempts,
		Dependencies: deps,
		Status:       StatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if len(deps) > 0 {
		job.Unresolved = make(map[string]bool, len(deps))
		for _, dep := range deps {
			switch q.st.Jobs[dep].Status {
			case StatusCompleted:
				// 提交时前置已完成：不再占用未决集合。
			case StatusDead, StatusBlocked:
				// 提交时前置已是失败终态：直接阻断。
				job.Status = StatusBlocked
				job.BlockedReason = blockedReason(q.st.Jobs[dep], now)
				job.Unresolved = nil
			default:
				job.Unresolved[dep] = true
			}
			if job.Status == StatusBlocked {
				break
			}
		}
		if job.Status != StatusBlocked && len(job.Unresolved) > 0 {
			job.Status = StatusWaiting
		}
		if job.Status == StatusPending {
			job.ReleasedAt = now
		}
	}
	q.st.Jobs[taskID] = job
	for _, dep := range deps {
		q.st.Dependents[dep] = append(q.st.Dependents[dep], taskID)
	}
	if err := q.persist(op); err != nil {
		delete(q.st.Jobs, taskID)
		for _, dep := range deps {
			q.st.Dependents[dep] = removeString(q.st.Dependents[dep], taskID)
		}
		return nil, err
	}
	return job, nil
}

// wouldCycle 判断新任务以 deps 为前置是否会在依赖图中成环（调用方持锁）：
// 从 deps 出发沿 Dependencies 做 DFS，若能到达 taskID 则成环。
func (q *Queue) wouldCycle(taskID string, deps []string) bool {
	stack := append([]string(nil), deps...)
	visited := make(map[string]bool)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == taskID {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if j, ok := q.st.Jobs[cur]; ok {
			stack = append(stack, j.Dependencies...)
		}
	}
	return false
}

// blockedReason 依据失败终态的上游任务构造阻断原因。
func blockedReason(upstream *Job, now time.Time) *BlockedReason {
	r := &BlockedReason{Upstream: upstream.TaskID, UpstreamStatus: upstream.Status, At: now}
	switch upstream.Status {
	case StatusBlocked:
		if upstream.BlockedReason != nil {
			r.Cause = upstream.BlockedReason.String()
		} else {
			r.Cause = "upstream is blocked"
		}
	default: // StatusDead
		if upstream.LastError != "" {
			r.Cause = upstream.LastError
		} else {
			r.Cause = "upstream is dead-lettered"
		}
	}
	return r
}

// releaseDependents 在某个任务完成后处理其直接后继：仅处理仍处于 waiting 的
// 后继；其未决前置全部完成时，在同一次持锁修改中转为 pending（转换只发生
// 一次）。不跨层递归——后继的后继等待该后继真正完成后才会被释放。
func (q *Queue) releaseDependents(completedID string, now time.Time) {
	for _, id := range q.st.Dependents[completedID] {
		d := q.st.Jobs[id]
		if d.Status != StatusWaiting {
			continue
		}
		if d.Unresolved != nil {
			delete(d.Unresolved, completedID)
		}
		if len(d.Unresolved) > 0 {
			continue
		}
		d.Status = StatusPending
		d.Unresolved = nil
		d.ReleasedAt = now
		d.UpdatedAt = now
	}
}

// cascadeBlocked 在某个任务进入 dead/blocked 终态后，把所有仍在 waiting 的
// 直接/间接后继转为 blocked 并记录直接上游的阻断原因，逐层向下传播。
// 每个后继只会被阻断一次：已离开 waiting（含已被别的上游阻断）的不再处理。
func (q *Queue) cascadeBlocked(upstreamID string, now time.Time) {
	stack := []string{upstreamID}
	for len(stack) > 0 {
		up := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		upJob := q.st.Jobs[up]
		for _, id := range q.st.Dependents[up] {
			d := q.st.Jobs[id]
			if d.Status != StatusWaiting {
				continue
			}
			d.Status = StatusBlocked
			d.Unresolved = nil
			d.BlockedReason = blockedReason(upJob, now)
			d.UpdatedAt = now
			stack = append(stack, id)
		}
	}
}

func removeString(xs []string, s string) []string {
	for i, x := range xs {
		if x == s {
			return append(xs[:i], xs[i+1:]...)
		}
	}
	return xs
}

// Claim 批量领取已到期任务。每次领取为任务发放递增的尝试号与有期限的租约，
// 并立即持久化。未到执行时间或仍被有效租约持有的任务不会被领取；
// 租约过期且尝试次数已用尽的任务转入死信。
func (q *Queue) Claim(batchSize int, leaseDuration time.Duration) ([]Lease, error) {
	const op = "claim"
	if batchSize < 1 {
		return nil, invalidArg(op, "batch size must be >= 1")
	}
	if leaseDuration <= 0 {
		return nil, invalidArg(op, "lease duration must be > 0")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	// 按 (RunAt, TaskID) 排序，保证领取顺序确定。
	jobs := make([]*Job, 0, len(q.st.Jobs))
	for _, j := range q.st.Jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, k int) bool {
		if !jobs[i].RunAt.Equal(jobs[k].RunAt) {
			return jobs[i].RunAt.Before(jobs[k].RunAt)
		}
		return jobs[i].TaskID < jobs[k].TaskID
	})
	var leases []Lease
	for _, j := range jobs {
		if len(leases) >= batchSize {
			break
		}
		if j.Status == StatusLeased && !now.Before(j.LeaseExpiry) {
			// 租约已过期：尝试次数用尽则死信，否则回到待领取。
			j.LeaseID, j.LeaseAttempt, j.LeaseExpiry = "", 0, time.Time{}
			if j.Attempts >= j.MaxAttempts {
				j.Status = StatusDead
				j.LastError = "attempts exhausted: lease expired"
				j.UpdatedAt = now
				q.cascadeBlocked(j.TaskID, now)
				continue
			}
			j.Status = StatusPending
			j.UpdatedAt = now
		}
		if j.Status != StatusPending || now.Before(j.RunAt) || j.Attempts >= j.MaxAttempts {
			continue
		}
		j.Attempts++
		j.LeaseID = q.nextID("lease")
		j.LeaseAttempt = j.Attempts
		j.LeaseExpiry = now.Add(leaseDuration)
		j.Status = StatusLeased
		j.UpdatedAt = now
		leases = append(leases, Lease{
			TaskID:    j.TaskID,
			LeaseID:   j.LeaseID,
			Attempt:   j.LeaseAttempt,
			ExpiresAt: j.LeaseExpiry,
			Payload:   j.Payload,
		})
	}
	if err := q.persist(op); err != nil {
		return nil, err
	}
	return leases, nil
}

// checkLease 校验操作携带的租约是否为当前有效租约。
// 租约号/尝试号不匹配或租约已过期（迟到操作）都返回 KindLeaseMismatch。
func checkLease(op string, j *Job, leaseID string, attempt int, now time.Time) error {
	if j.Status != StatusLeased || j.LeaseID != leaseID || j.LeaseAttempt != attempt {
		return leaseErr(op, "lease does not match the current attempt")
	}
	if !now.Before(j.LeaseExpiry) {
		return leaseErr(op, "lease has expired")
	}
	return nil
}

func (q *Queue) lookupActive(op, taskID string) (*Job, error) {
	j, ok := q.st.Jobs[taskID]
	if !ok {
		return nil, notFound(op, "task not found")
	}
	switch j.Status {
	case StatusCompleted:
		return nil, stateErr(op, "task already completed")
	case StatusDead:
		return nil, stateErr(op, "task is dead-lettered")
	case StatusBlocked:
		return nil, stateErr(op, "task is blocked by an upstream dependency")
	}
	return j, nil
}

// Heartbeat 续租：携带当前租约与尝试号，将租约延长 extend。
// 返回新的过期时间。
func (q *Queue) Heartbeat(taskID, leaseID string, attempt int, extend time.Duration) (time.Time, error) {
	const op = "heartbeat"
	if leaseID == "" {
		return time.Time{}, invalidArg(op, "lease id is required")
	}
	if extend <= 0 {
		return time.Time{}, invalidArg(op, "extend must be > 0")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, err := q.lookupActive(op, taskID)
	if err != nil {
		return time.Time{}, err
	}
	now := q.now()
	if err := checkLease(op, j, leaseID, attempt, now); err != nil {
		return time.Time{}, err
	}
	j.LeaseExpiry = now.Add(extend)
	j.UpdatedAt = now
	if err := q.persist(op); err != nil {
		return time.Time{}, err
	}
	return j.LeaseExpiry, nil
}

// Complete 完成任务。业务完成记录与 outbox 待通知消息在同一次持锁修改中
// 原子写入并持久化。任务已完成时视为重放：返回首次的完成记录，不重复通知。
// 租约过期后的迟到完成返回 KindLeaseMismatch，不会覆盖新尝试。
func (q *Queue) Complete(taskID, leaseID string, attempt int, result []byte) (*CompletionRecord, error) {
	const op = "complete"
	if leaseID == "" {
		return nil, invalidArg(op, "lease id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.st.Jobs[taskID]
	if !ok {
		return nil, notFound(op, "task not found")
	}
	if j.Status == StatusCompleted {
		// 幂等重放：返回首次结果，不再写 outbox。
		return j.Completion, nil
	}
	if j.Status == StatusDead {
		return nil, stateErr(op, "task is dead-lettered")
	}
	if j.Status == StatusBlocked {
		return nil, stateErr(op, "task is blocked by an upstream dependency")
	}
	now := q.now()
	if err := checkLease(op, j, leaseID, attempt, now); err != nil {
		return nil, err
	}
	rec := &CompletionRecord{
		TaskID:      taskID,
		LeaseID:     leaseID,
		Attempt:     attempt,
		Result:      result,
		CompletedAt: now,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(op, err)
	}
	j.Status = StatusCompleted
	j.Completion = rec
	j.LeaseID, j.LeaseAttempt, j.LeaseExpiry = "", 0, time.Time{}
	j.UpdatedAt = now
	q.st.Outbox = append(q.st.Outbox, OutboxMessage{
		ID:        q.nextID("msg"),
		TaskID:    taskID,
		Kind:      "job.completed",
		Body:      body,
		CreatedAt: now,
	})
	// 与完成记录同一次持锁修改释放后继：最后一个前置完成与后继转为可领取
	// 之间不存在中间态，既不会被提前租出，也不会重复入队。
	q.releaseDependents(taskID, now)
	if err := q.persist(op); err != nil {
		return nil, err
	}
	return rec, nil
}

// Fail 上报失败。retryable 且尝试次数未用尽时按既定策略重排到
// now+Delay(attempt)；否则进入死信。租约校验规则与 Complete 相同。
func (q *Queue) Fail(taskID, leaseID string, attempt int, cause string, retryable bool) error {
	const op = "fail"
	if leaseID == "" {
		return invalidArg(op, "lease id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, err := q.lookupActive(op, taskID)
	if err != nil {
		return err
	}
	now := q.now()
	if err := checkLease(op, j, leaseID, attempt, now); err != nil {
		return err
	}
	j.LastError = cause
	j.LeaseID, j.LeaseAttempt, j.LeaseExpiry = "", 0, time.Time{}
	j.UpdatedAt = now
	if retryable && j.Attempts < j.MaxAttempts {
		j.Status = StatusPending
		j.RunAt = now.Add(q.policy.Delay(attempt))
	} else {
		j.Status = StatusDead
		// 死信与后继阻断同一次持锁修改：后继不会停在 waiting，
		// 阻断原因直接指向该死信任务及其 LastError。
		q.cascadeBlocked(taskID, now)
	}
	return q.persist(op)
}

// Get 查询单个任务的当前快照。
func (q *Queue) Get(taskID string) (*Job, error) {
	const op = "get"
	if taskID == "" {
		return nil, invalidArg(op, "task id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.st.Jobs[taskID]
	if !ok {
		return nil, notFound(op, "task not found")
	}
	cp := *j
	if len(j.Dependencies) > 0 {
		cp.Dependencies = append([]string(nil), j.Dependencies...)
	}
	if len(j.Unresolved) > 0 {
		cp.Unresolved = make(map[string]bool, len(j.Unresolved))
		for d := range j.Unresolved {
			cp.Unresolved[d] = true
		}
	}
	return &cp, nil
}

// Stats 返回队列状态概览。
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	for _, j := range q.st.Jobs {
		switch j.Status {
		case StatusWaiting:
			s.Waiting++
		case StatusPending:
			s.Pending++
		case StatusLeased:
			s.Leased++
		case StatusCompleted:
			s.Completed++
		case StatusBlocked:
			s.Blocked++
		case StatusDead:
			s.Dead++
		}
	}
	s.OutboxPending = len(q.st.Outbox)
	return s
}

// Outbox 返回待通知消息列表（按写入顺序）。
func (q *Queue) Outbox() []OutboxMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]OutboxMessage, len(q.st.Outbox))
	copy(out, q.st.Outbox)
	return out
}

// AckOutbox 通知投递成功后确认删除消息。
func (q *Queue) AckOutbox(messageID string) error {
	const op = "ack-outbox"
	if messageID == "" {
		return invalidArg(op, "message id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, m := range q.st.Outbox {
		if m.ID == messageID {
			q.st.Outbox = append(q.st.Outbox[:i], q.st.Outbox[i+1:]...)
			return q.persist(op)
		}
	}
	return notFound(op, "outbox message not found")
}
