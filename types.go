package jobleases

import "time"

// Status 是任务的生命周期状态。
type Status string

const (
	// StatusPending 等待执行（包括首次入队和失败重排后）。
	StatusPending Status = "pending"
	// StatusLeased 已被某工作者领取，租约有效期内。
	StatusLeased Status = "leased"
	// StatusCompleted 已成功完成，终态。
	StatusCompleted Status = "completed"
	// StatusDead 重试次数用尽或不可重试失败，进入死信，终态。
	StatusDead Status = "dead"
)

// RetryPolicy 确定性的退避重试策略：第 n 次失败后的延迟为
// InitialDelay * Multiplier^(n-1)，封顶 MaxDelay。无随机抖动，便于推演与测试。
type RetryPolicy struct {
	InitialDelay time.Duration `json:"initial_delay"`
	Multiplier   float64       `json:"multiplier"`
	MaxDelay     time.Duration `json:"max_delay"`
}

// DefaultRetryPolicy 是未指定策略时使用的默认值。
var DefaultRetryPolicy = RetryPolicy{
	InitialDelay: time.Second,
	Multiplier:   2,
	MaxDelay:     time.Minute,
}

// Delay 返回第 attempt 次尝试（1 起）失败后应等待的时间。
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.InitialDelay)
	for i := 1; i < attempt; i++ {
		d *= p.Multiplier
		if d >= float64(p.MaxDelay) {
			return p.MaxDelay
		}
	}
	if d > float64(p.MaxDelay) {
		return p.MaxDelay
	}
	return time.Duration(d)
}

func (p RetryPolicy) normalize() RetryPolicy {
	if p.InitialDelay <= 0 {
		p.InitialDelay = DefaultRetryPolicy.InitialDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultRetryPolicy.Multiplier
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = DefaultRetryPolicy.MaxDelay
	}
	if p.MaxDelay < p.InitialDelay {
		p.MaxDelay = p.InitialDelay
	}
	return p
}

// Lease 是一次领取获得的有时限执行权。
type Lease struct {
	ID        string    `json:"id"`
	Attempt   int       `json:"attempt"` // 本次尝试号，单调递增
	WorkerID  string    `json:"worker_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CompletionRecord 是任务成功完成的业务记录，与 outbox 通知原子写入。
type CompletionRecord struct {
	LeaseID     string    `json:"lease_id"`
	Attempt     int       `json:"attempt"`
	Result      []byte    `json:"result,omitempty"`
	CompletedAt time.Time `json:"completed_at"`
}

// Job 是一个后台任务。
type Job struct {
	ID          string      `json:"id"`
	ExternalID  string      `json:"external_id"` // 外部任务号，幂等键
	Payload     []byte      `json:"payload"`
	RunAt       time.Time   `json:"run_at"` // 可执行时间
	MaxAttempts int         `json:"max_attempts"`
	Retry       RetryPolicy `json:"retry"`

	Status   Status `json:"status"`
	Attempts int    `json:"attempts"` // 已发放的尝试次数
	Lease    *Lease `json:"lease,omitempty"`

	LastError string            `json:"last_error,omitempty"`
	Result    *CompletionRecord `json:"result,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// OutboxEntry 是待通知的完成消息，与完成记录同事务写入。
type OutboxEntry struct {
	ID         string    `json:"id"`
	JobID      string    `json:"job_id"`
	ExternalID string    `json:"external_id"`
	Result     []byte    `json:"result,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// QueueStats 是队列状态查询结果。
type QueueStats struct {
	Pending   int `json:"pending"`   // 等待执行（含未到执行时间）
	Leased    int `json:"leased"`    // 被有效租约持有
	Completed int `json:"completed"` // 已完成
	Dead      int `json:"dead"`      // 死信
	DueNow    int `json:"due_now"`   // 当前可领取（已到期且未被有效租约持有）
}
