package jobleases

import "time"

// JobStatus 任务生命周期状态。
type JobStatus string

const (
	// StatusPending 待领取（含失败重排后等待到期的任务）。
	StatusPending JobStatus = "pending"
	// StatusLeased 已被某次尝试领取，租约有效期内不可被其他工作者领取。
	StatusLeased JobStatus = "leased"
	// StatusCompleted 已完成，终态。
	StatusCompleted JobStatus = "completed"
	// StatusDead 死信，终态：不可重试失败或尝试次数用尽。
	StatusDead JobStatus = "dead"
)

// Job 是一条后台任务记录。同一外部任务号（TaskID）全队列唯一。
type Job struct {
	TaskID       string    `json:"task_id"`      // 外部任务号，幂等键
	Payload      []byte    `json:"payload"`      // 任务负载
	RunAt        time.Time `json:"run_at"`       // 可执行时间，到期前不可领取
	MaxAttempts  int       `json:"max_attempts"` // 最大尝试次数
	Attempts     int       `json:"attempts"`     // 已发放的尝试次数
	Status       JobStatus `json:"status"`
	LeaseID      string    `json:"lease_id,omitempty"`
	LeaseAttempt int       `json:"lease_attempt,omitempty"`
	LeaseExpiry  time.Time `json:"lease_expiry,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
	// Completion 完成记录，与 outbox 消息原子写入；完成请求重放时原样返回。
	Completion *CompletionRecord `json:"completion,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// Lease 是领取成功后返回给工作者的租约，心跳/完成/失败都必须携带。
type Lease struct {
	TaskID    string    `json:"task_id"`
	LeaseID   string    `json:"lease_id"`
	Attempt   int       `json:"attempt"` // 本次尝试号，单调递增
	ExpiresAt time.Time `json:"expires_at"`
	Payload   []byte    `json:"payload"`
}

// CompletionRecord 业务完成记录，与 outbox 通知原子持久化。
type CompletionRecord struct {
	TaskID      string    `json:"task_id"`
	LeaseID     string    `json:"lease_id"`
	Attempt     int       `json:"attempt"`
	Result      []byte    `json:"result"`
	CompletedAt time.Time `json:"completed_at"`
}

// OutboxMessage 待通知消息，与完成记录同事务写入，由通知器投递后确认删除。
type OutboxMessage struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	Kind      string    `json:"kind"` // 目前固定为 "job.completed"
	Body      []byte    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// Stats 队列状态概览。
type Stats struct {
	Pending       int `json:"pending"`
	Leased        int `json:"leased"`
	Completed     int `json:"completed"`
	Dead          int `json:"dead"`
	OutboxPending int `json:"outbox_pending"`
}
