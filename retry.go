package jobleases

import "time"

// RetryPolicy 是确定性的重试策略：第 attempt 次失败后的延迟为
// BaseDelay * 2^(attempt-1)，封顶 MaxDelay。不含随机抖动，
// 相同输入永远得到相同的重排时间，便于崩溃恢复后重现行为。
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// DefaultRetryPolicy 默认策略：1s 起步，指数退避，封顶 5min。
var DefaultRetryPolicy = RetryPolicy{BaseDelay: time.Second, MaxDelay: 5 * time.Minute}

// Delay 返回第 attempt 次尝试失败后的重试延迟（attempt 从 1 开始）。
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := p.BaseDelay
	if base <= 0 {
		base = time.Second
	}
	max := p.MaxDelay
	if max <= 0 {
		max = 5 * time.Minute
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}
