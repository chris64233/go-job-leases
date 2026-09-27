package jobleases

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
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

// NewQueue 从 Store 恢复状态并返回队列。崩溃前的领取、完成、死信等状态都会还原。
func NewQueue(store Store, opts ...Option) (*Queue, error) {
	st, err := store.Load()
	if err != nil {
		return nil, internalErr("load", err)
	}
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

// Submit 提交无前置依赖的任务，等价于 SubmitWithDeps(..., nil)。
func (q *Queue) Submit(taskID string, payload []byte, runAt time.Time, maxAttempts int) (*Job, error) {
	return q.SubmitWithDeps(taskID, payload, runAt, maxAttempts, nil)
}

// SubmitWithDeps 提交任务并声明前置任务依赖。以 TaskID 幂等：同号同负载同依赖
// 返回已存在的任务；同号不同负载或不同依赖返回 KindConflict 冲突。
//
// 前置任务必须已存在（KindNotFound），不允许自我依赖与循环依赖
// （KindDependency）。提交时：任一前置已死信/被阻断则任务直接阻断并记录原因；
// 全部前置已完成则直接可领取；否则进入等待，直到最后一个前置完成。
func (q *Queue) SubmitWithDeps(taskID string, payload []byte, runAt time.Time, maxAttempts int, deps []string) (*Job, error) {
	const op = "submit"
	if taskID == "" {
		return nil, invalidArg(op, "task id is required")
	}
	if maxAttempts < 1 {
		return nil, invalidArg(op, "max attempts must be >= 1")
	}
	deps = normalizeDeps(deps)
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.st.Jobs[taskID]; ok {
		if !bytes.Equal(existing.Payload, payload) || !slices.Equal(existing.DependsOn, deps) {
			return nil, conflict(op, "task id already exists with a different payload or dependencies")
		}
		return existing, nil
	}
	for _, dep := range deps {
		if dep == taskID {
			return nil, depErr(op, "task cannot depend on itself")
		}
		if _, ok := q.st.Jobs[dep]; !ok {
			return nil, notFound(op, fmt.Sprintf("dependency %q not found", dep))
		}
	}
	if reachesJob(q.st, taskID, deps) {
		return nil, depErr(op, "dependency cycle detected")
	}
	now := q.now()
	job := &Job{
		TaskID:      taskID,
		Payload:     payload,
		RunAt:       runAt,
		MaxAttempts: maxAttempts,
		DependsOn:   deps,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	switch blockedBy, allDone := depStates(q.st, deps); {
	case blockedBy != nil:
		// 前置已终态失败：直接阻断，不进入等待。
		job.Status = StatusBlocked
		job.BlockedReason = blockedReason(blockedBy)
	case allDone:
		// 无依赖或前置已全部完成：直接可领取。
		job.Status = StatusPending
		job.ReleasedAt = now
	default:
		job.Status = StatusWaiting
	}
	q.st.Jobs[taskID] = job
	if err := q.persist(op); err != nil {
		delete(q.st.Jobs, taskID)
		return nil, err
	}
	return job, nil
}

// normalizeDeps 排序去重，使依赖列表可比较、可持久化。
func normalizeDeps(deps []string) []string {
	if len(deps) == 0 {
		return nil
	}
	out := slices.Clone(deps)
	slices.Sort(out)
	return slices.Compact(out)
}

// depStates 检查前置任务状态：返回第一个已终态失败（死信/阻断）的前置，
// 以及是否全部已完成。
func depStates(st *state, deps []string) (blockedBy *Job, allDone bool) {
	allDone = true
	for _, dep := range deps {
		j := st.Jobs[dep]
		switch j.Status {
		case StatusDead, StatusBlocked:
			if blockedBy == nil {
				blockedBy = j
			}
			allDone = false
		case StatusCompleted:
		default:
			allDone = false
		}
	}
	return blockedBy, allDone
}

// depsSatisfied 报告全部前置是否均已完成。
func depsSatisfied(st *state, deps []string) bool {
	for _, dep := range deps {
		if st.Jobs[dep].Status != StatusCompleted {
			return false
		}
	}
	return true
}

// reachesJob 从 deps 出发沿依赖边遍历，检测是否能到达 taskID（循环依赖）。
func reachesJob(st *state, taskID string, deps []string) bool {
	seen := make(map[string]bool)
	stack := slices.Clone(deps)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if id == taskID {
			return true
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		if j, ok := st.Jobs[id]; ok {
			stack = append(stack, j.DependsOn...)
		}
	}
	return false
}

// blockedReason 生成阻断原因：哪个前置任务以何种终态失败。
func blockedReason(dep *Job) string {
	if dep.Status == StatusBlocked {
		return fmt.Sprintf("dependency %q blocked: %s", dep.TaskID, dep.BlockedReason)
	}
	return fmt.Sprintf("dependency %q dead-lettered: %s", dep.TaskID, dep.LastError)
}

// releaseDependents 在任务完成后调用：将所有依赖已全满足的等待中后续任务
// 转为待领取并记录释放时间。只在持锁状态、与完成同一次持久化中生效，
// 因此多个前置并发完成时后续任务只会转换一次。
func (q *Queue) releaseDependents(completed *Job, now time.Time) {
	for _, j := range q.st.Jobs {
		if j.Status != StatusWaiting || !slices.Contains(j.DependsOn, completed.TaskID) {
			continue
		}
		if depsSatisfied(q.st, j.DependsOn) {
			j.Status = StatusPending
			j.ReleasedAt = now
			j.UpdatedAt = now
		}
	}
}

// blockDependents 在任务进入死信/阻断时调用：递归阻断仍在等待的后续任务
// 并记录原因。已可领取/进行中的后续任务不受影响（其前置当时已满足）。
func (q *Queue) blockDependents(failed *Job, now time.Time) {
	reason := blockedReason(failed)
	for _, j := range q.st.Jobs {
		if j.Status != StatusWaiting || !slices.Contains(j.DependsOn, failed.TaskID) {
			continue
		}
		j.Status = StatusBlocked
		j.BlockedReason = reason
		j.UpdatedAt = now
		q.blockDependents(j, now)
	}
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
				q.blockDependents(j, now)
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
		return nil, stateErr(op, "task is blocked by a failed dependency")
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
		return nil, stateErr(op, "task is blocked by a failed dependency")
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
	// 与完成同一次持锁修改中原子释放依赖已满足的后续任务。
	q.releaseDependents(j, now)
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
		q.blockDependents(j, now)
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
	cp.DependsOn = slices.Clone(j.DependsOn)
	return &cp, nil
}

// Stats 返回队列状态概览。
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	for _, j := range q.st.Jobs {
		switch j.Status {
		case StatusPending:
			s.Pending++
		case StatusWaiting:
			s.Waiting++
		case StatusLeased:
			s.Leased++
		case StatusCompleted:
			s.Completed++
		case StatusDead:
			s.Dead++
		case StatusBlocked:
			s.Blocked++
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
