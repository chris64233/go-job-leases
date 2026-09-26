package jobleases

import (
	"bytes"
	"sort"
	"strconv"
	"time"
)

// Clock 返回当前时间，抽出来便于测试租约过期。
type Clock func() time.Time

// Queue 是后台任务租约队列。
type Queue struct {
	store Store
	now   Clock
}

// NewQueue 创建一个基于 store 的队列。
func NewQueue(store Store) *Queue {
	return &Queue{store: store, now: time.Now}
}

// NewQueueWithClock 使用自定义时钟创建队列（主要用于测试）。
func NewQueueWithClock(store Store, clock Clock) *Queue {
	if clock == nil {
		clock = time.Now
	}
	return &Queue{store: store, now: clock}
}

// SubmitInput 是提交任务的参数。
type SubmitInput struct {
	ExternalID  string      // 外部任务号，必填，幂等键
	Payload     []byte      // 任务负载，必填
	RunAt       time.Time   // 可执行时间；零值表示立即执行
	MaxAttempts int         // 最大尝试次数；<=0 视为 1
	Retry       RetryPolicy // 重试策略；零值使用 DefaultRetryPolicy
}

// SubmitOutput 是提交结果。Created 为 false 表示命中幂等、返回已有任务。
type SubmitOutput struct {
	Job     *Job
	Created bool
}

// Submit 提交一个任务。相同外部任务号 + 相同负载返回原任务；
// 相同外部任务号但负载不同返回 KindConflict。
func (q *Queue) Submit(in SubmitInput) (*SubmitOutput, error) {
	if in.ExternalID == "" {
		return nil, invalidArg("external_id is required")
	}
	if len(in.Payload) == 0 {
		return nil, invalidArg("payload is required")
	}
	maxAttempts := in.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	policy := in.Retry.normalize()
	runAt := in.RunAt
	if runAt.IsZero() {
		runAt = q.now()
	}
	payloadCopy := append([]byte(nil), in.Payload...)

	var out *SubmitOutput
	err := q.store.Update(func(s *snapshot) error {
		for _, j := range s.Jobs {
			if j.ExternalID != in.ExternalID {
				continue
			}
			if !bytes.Equal(j.Payload, in.Payload) {
				return conflict("job with external_id " + in.ExternalID + " exists with different payload")
			}
			out = &SubmitOutput{Job: cloneJob(j), Created: false}
			return nil
		}
		s.JobSeq++
		now := q.now()
		j := &Job{
			ID:          newID("job", s.JobSeq),
			ExternalID:  in.ExternalID,
			Payload:     payloadCopy,
			RunAt:       runAt,
			MaxAttempts: maxAttempts,
			Retry:       policy,
			Status:      StatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		s.Jobs[j.ID] = j
		out = &SubmitOutput{Job: cloneJob(j), Created: true}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LeaseInput 是领取任务的参数。
type LeaseInput struct {
	WorkerID string        // 领取方标识，必填
	Count    int           // 本次最多领取数量；<=0 视为 1
	LeaseFor time.Duration // 租约时长，必须为正
}

// Lease 批量领取已到期（RunAt <= now）且未被有效租约持有的任务。
// 每个任务获得新的递增尝试号和有时限租约。多个工作者并发领取时，
// Store 的串行事务保证同一次尝试不会发给两个人。
func (q *Queue) Lease(in LeaseInput) ([]*Job, error) {
	if in.WorkerID == "" {
		return nil, invalidArg("worker_id is required")
	}
	if in.LeaseFor <= 0 {
		return nil, invalidArg("lease duration must be positive")
	}
	count := in.Count
	if count <= 0 {
		count = 1
	}

	var leased []*Job
	err := q.store.Update(func(s *snapshot) error {
		now := q.now()
		candidates := make([]*Job, 0, count)
		for _, j := range s.Jobs {
			if q.isAvailable(j, now) {
				candidates = append(candidates, j)
			}
		}
		// 按可执行时间 FIFO 领取，同一时刻按创建顺序（ID 中含单调序列号）。
		sort.Slice(candidates, func(i, k int) bool {
			if !candidates[i].RunAt.Equal(candidates[k].RunAt) {
				return candidates[i].RunAt.Before(candidates[k].RunAt)
			}
			return candidates[i].ID < candidates[k].ID
		})
		if len(candidates) > count {
			candidates = candidates[:count]
		}
		for _, j := range candidates {
			s.LeaseSeq++
			j.Attempts++
			j.Status = StatusLeased
			j.Lease = &Lease{
				ID:        newID("lease", s.LeaseSeq),
				Attempt:   j.Attempts,
				WorkerID:  in.WorkerID,
				ExpiresAt: now.Add(in.LeaseFor),
			}
			j.UpdatedAt = now
			leased = append(leased, cloneJob(j))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

// isAvailable 判断任务此刻是否可被领取：pending 且已到期；
// 或处于 leased 但租约已过期（崩溃的工作者留下的过期租约可被重新领取）。
func (q *Queue) isAvailable(j *Job, now time.Time) bool {
	switch j.Status {
	case StatusPending:
		return !j.RunAt.After(now)
	case StatusLeased:
		return j.Lease == nil || !j.Lease.ExpiresAt.After(now)
	default:
		return false
	}
}

// HeartbeatInput 是心跳续约参数。
type HeartbeatInput struct {
	JobID    string
	LeaseID  string
	Attempt  int
	ExtendBy time.Duration // 在当前时间基础上延长的时长，必须为正
}

// Heartbeat 为有效租约续约。必须携带当前租约号和尝试号；
// 租约已过期、已被新尝试取代或任务已终结时返回 KindLease/KindState。
func (q *Queue) Heartbeat(in HeartbeatInput) (*Lease, error) {
	if in.JobID == "" || in.LeaseID == "" {
		return nil, invalidArg("job_id and lease_id are required")
	}
	if in.Attempt <= 0 {
		return nil, invalidArg("attempt must be positive")
	}
	if in.ExtendBy <= 0 {
		return nil, invalidArg("extend duration must be positive")
	}
	var out *Lease
	err := q.store.Update(func(s *snapshot) error {
		j, err := mustActiveLease(s, in.JobID, in.LeaseID, in.Attempt, q.now())
		if err != nil {
			return err
		}
		j.Lease.ExpiresAt = q.now().Add(in.ExtendBy)
		j.UpdatedAt = q.now()
		l := *j.Lease
		out = &l
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CompleteInput 是完成任务的参数。
type CompleteInput struct {
	JobID   string
	LeaseID string
	Attempt int
	Result  []byte
}

// CompleteOutput 返回完成后的任务。Replayed 为 true 表示这是重复的完成请求：
// 返回首次完成的结果，且不会再次写 outbox 通知。
type CompleteOutput struct {
	Job      *Job
	Replayed bool
}

// Complete 标记任务成功完成。业务结果与 outbox 通知在同一个持久化事务中原子写入。
// 只有持有当前有效租约且尝试号匹配才能完成；租约过期后新尝试已发放时，
// 迟到的旧完成请求会被拒绝（KindLease），不能覆盖新尝试。
// 对已完成任务重放相同租约的完成请求，返回首次结果且不重复通知。
func (q *Queue) Complete(in CompleteInput) (*CompleteOutput, error) {
	if in.JobID == "" || in.LeaseID == "" {
		return nil, invalidArg("job_id and lease_id are required")
	}
	if in.Attempt <= 0 {
		return nil, invalidArg("attempt must be positive")
	}
	resultCopy := append([]byte(nil), in.Result...)

	var out *CompleteOutput
	err := q.store.Update(func(s *snapshot) error {
		j, ok := s.Jobs[in.JobID]
		if !ok {
			return notFound("job " + in.JobID + " not found")
		}

		// 已完成：仅允许同一租约的完成请求重放，返回首次结果。
		if j.Status == StatusCompleted {
			if j.Result != nil && j.Result.LeaseID == in.LeaseID && j.Result.Attempt == in.Attempt {
				out = &CompleteOutput{Job: cloneJob(j), Replayed: true}
				return nil
			}
			return stateErr("job " + in.JobID + " is already completed")
		}
		if j.Status == StatusDead {
			return stateErr("job " + in.JobID + " is dead")
		}
		if _, err := mustActiveLease(s, in.JobID, in.LeaseID, in.Attempt, q.now()); err != nil {
			return err
		}

		now := q.now()
		j.Status = StatusCompleted
		j.Result = &CompletionRecord{
			LeaseID:     in.LeaseID,
			Attempt:     in.Attempt,
			Result:      resultCopy,
			CompletedAt: now,
		}
		j.Lease = nil
		j.UpdatedAt = now

		s.OutboxSeq++
		s.Outbox = append(s.Outbox, OutboxEntry{
			ID:         newID("outbox", s.OutboxSeq),
			JobID:      j.ID,
			ExternalID: j.ExternalID,
			Result:     append([]byte(nil), resultCopy...),
			CreatedAt:  now,
		})
		out = &CompleteOutput{Job: cloneJob(j), Replayed: false}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FailInput 是报告任务失败的参数。Retryable=false 表示立即进入死信。
type FailInput struct {
	JobID     string
	LeaseID   string
	Attempt   int
	Reason    string
	Retryable bool
}

// FailOutput 是失败处理结果。
type FailOutput struct {
	Job       *Job
	Dead      bool      // 是否用尽次数进入死信
	NextRunAt time.Time // 若将重试，下一次可执行时间
}

// Fail 报告一次尝试失败。可重试且未超过 MaxAttempts 时按重试策略重新排期；
// 不可重试或次数用尽时进入死信。必须持有当前有效租约且尝试号匹配。
func (q *Queue) Fail(in FailInput) (*FailOutput, error) {
	if in.JobID == "" || in.LeaseID == "" {
		return nil, invalidArg("job_id and lease_id are required")
	}
	if in.Attempt <= 0 {
		return nil, invalidArg("attempt must be positive")
	}

	var out *FailOutput
	err := q.store.Update(func(s *snapshot) error {
		j, ok := s.Jobs[in.JobID]
		if !ok {
			return notFound("job " + in.JobID + " not found")
		}
		if j.Status == StatusCompleted {
			return stateErr("job " + in.JobID + " is already completed")
		}
		if j.Status == StatusDead {
			return stateErr("job " + in.JobID + " is already dead")
		}
		if _, err := mustActiveLease(s, in.JobID, in.LeaseID, in.Attempt, q.now()); err != nil {
			return err
		}

		now := q.now()
		j.LastError = in.Reason
		j.Lease = nil
		j.UpdatedAt = now

		if !in.Retryable || j.Attempts >= j.MaxAttempts {
			j.Status = StatusDead
			out = &FailOutput{Job: cloneJob(j), Dead: true}
			return nil
		}
		delay := j.Retry.Delay(j.Attempts)
		j.Status = StatusPending
		j.RunAt = now.Add(delay)
		out = &FailOutput{Job: cloneJob(j), NextRunAt: j.RunAt}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// mustActiveLease 校验任务存在、处于 leased、租约号与尝试号匹配且租约未过期。
func mustActiveLease(s *snapshot, jobID, leaseID string, attempt int, now time.Time) (*Job, error) {
	j, ok := s.Jobs[jobID]
	if !ok {
		return nil, notFound("job " + jobID + " not found")
	}
	if j.Status != StatusLeased || j.Lease == nil {
		return nil, stateErr("job " + jobID + " is not leased (status=" + string(j.Status) + ")")
	}
	if j.Lease.ID != leaseID {
		return nil, leaseErr("lease " + leaseID + " is not the current lease for job " + jobID)
	}
	if j.Lease.Attempt != attempt {
		return nil, leaseErr("attempt mismatch: lease is for attempt " + strconv.Itoa(j.Lease.Attempt) + ", request carries " + strconv.Itoa(attempt))
	}
	if !j.Lease.ExpiresAt.After(now) {
		return nil, leaseErr("lease " + leaseID + " expired at " + j.Lease.ExpiresAt.Format(time.RFC3339Nano))
	}
	return j, nil
}

// Get 按 ID 查询任务，不存在返回 KindNotFound。
func (q *Queue) Get(jobID string) (*Job, error) {
	if jobID == "" {
		return nil, invalidArg("job_id is required")
	}
	var out *Job
	err := q.store.View(func(s *snapshot) error {
		j, ok := s.Jobs[jobID]
		if !ok {
			return notFound("job " + jobID + " not found")
		}
		out = cloneJob(j)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByExternalID 按外部任务号查询任务。
func (q *Queue) GetByExternalID(externalID string) (*Job, error) {
	if externalID == "" {
		return nil, invalidArg("external_id is required")
	}
	var out *Job
	err := q.store.View(func(s *snapshot) error {
		for _, j := range s.Jobs {
			if j.ExternalID == externalID {
				out = cloneJob(j)
				return nil
			}
		}
		return notFound("job with external_id " + externalID + " not found")
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Stats 返回队列当前状态汇总。
func (q *Queue) Stats() (QueueStats, error) {
	var st QueueStats
	now := q.now()
	err := q.store.View(func(s *snapshot) error {
		for _, j := range s.Jobs {
			switch j.Status {
			case StatusPending:
				st.Pending++
				if !j.RunAt.After(now) {
					st.DueNow++
				}
			case StatusLeased:
				if j.Lease != nil && j.Lease.ExpiresAt.After(now) {
					st.Leased++
				} else {
					// 租约已过期但尚未被重新领取，视为可领取。
					st.DueNow++
				}
			case StatusCompleted:
				st.Completed++
			case StatusDead:
				st.Dead++
			}
		}
		return nil
	})
	return st, err
}

// PendingOutbox 返回尚未投递的 outbox 通知（按写入顺序）。
func (q *Queue) PendingOutbox() ([]OutboxEntry, error) {
	var out []OutboxEntry
	err := q.store.View(func(s *snapshot) error {
		out = append(out, s.Outbox...)
		return nil
	})
	return out, err
}

// MarkOutboxSent 在通知成功投递后从 outbox 移除对应条目。
func (q *Queue) MarkOutboxSent(entryID string) error {
	if entryID == "" {
		return invalidArg("entry_id is required")
	}
	return q.store.Update(func(s *snapshot) error {
		for i, e := range s.Outbox {
			if e.ID == entryID {
				s.Outbox = append(s.Outbox[:i], s.Outbox[i+1:]...)
				return nil
			}
		}
		return notFound("outbox entry " + entryID + " not found")
	})
}
