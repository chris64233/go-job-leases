package jobleases

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// clock 可手动推进的测试时钟。
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)} }
func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestQueue(t *testing.T, c *clock) *Queue {
	t.Helper()
	q, err := NewQueue(NewMemoryStore(), WithClock(c.Now))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	return q
}

func mustSubmit(t *testing.T, q *Queue, id string, payload []byte, runAt time.Time, maxAttempts int) *Job {
	t.Helper()
	j, err := q.Submit(id, payload, runAt, maxAttempts)
	if err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return j
}

func mustClaim(t *testing.T, q *Queue, n int, d time.Duration) []Lease {
	t.Helper()
	ls, err := q.Claim(n, d)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return ls
}

func requireKind(t *testing.T, err error, kind ErrorKind) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error of kind %v, got nil", kind)
	}
	k, ok := KindOf(err)
	if !ok || k != kind {
		t.Fatalf("expected error kind %v, got %v (ok=%v): %v", kind, k, ok, err)
	}
}

func TestSubmitIdempotent(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)

	j1 := mustSubmit(t, q, "task-1", []byte("payload"), c.Now(), 3)
	j2 := mustSubmit(t, q, "task-1", []byte("payload"), c.Now().Add(time.Hour), 5)
	if j1 != j2 {
		t.Fatalf("same id + same payload should return the original job")
	}
	if j2.MaxAttempts != 3 || !j2.RunAt.Equal(j1.RunAt) {
		t.Fatalf("idempotent replay must keep original parameters, got %+v", j2)
	}

	_, err := q.Submit("task-1", []byte("other"), c.Now(), 3)
	requireKind(t, err, KindConflict)
}

func TestSubmitValidation(t *testing.T) {
	q := newTestQueue(t, newClock())
	_, err := q.Submit("", []byte("p"), time.Now(), 1)
	requireKind(t, err, KindInvalidArgument)
	_, err = q.Submit("t", []byte("p"), time.Now(), 0)
	requireKind(t, err, KindInvalidArgument)
}

func TestClaimRespectsRunAtAndLease(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "future", []byte("p"), c.Now().Add(time.Hour), 3)
	mustSubmit(t, q, "due", []byte("p"), c.Now(), 3)

	ls := mustClaim(t, q, 10, time.Minute)
	if len(ls) != 1 || ls[0].TaskID != "due" {
		t.Fatalf("only the due job should be claimed, got %+v", ls)
	}
	if ls[0].Attempt != 1 {
		t.Fatalf("first attempt should be 1, got %d", ls[0].Attempt)
	}

	// 租约有效期内不能再领取。
	if ls := mustClaim(t, q, 10, time.Minute); len(ls) != 0 {
		t.Fatalf("leased job must not be claimed again, got %+v", ls)
	}

	// 租约未到期时推进到可执行时间也不行（future 到期、due 仍被租约持有）。
	c.Advance(2 * time.Hour)
	// 上一行同时让 due 的租约过期，因此两个都可领取。
	ls = mustClaim(t, q, 10, time.Minute)
	if len(ls) != 2 {
		t.Fatalf("expected 2 claimable jobs after lease expiry, got %d", len(ls))
	}
	attempts := map[string]int{}
	for _, l := range ls {
		attempts[l.TaskID] = l.Attempt
	}
	if attempts["due"] != 2 {
		t.Fatalf("re-claimed job should get attempt 2, got %d", attempts["due"])
	}
	if attempts["future"] != 1 {
		t.Fatalf("newly due job should get attempt 1, got %d", attempts["future"])
	}
}

func TestConcurrentClaimNoDuplicateAttempt(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	for i := 0; i < 20; i++ {
		mustSubmit(t, q, fmt.Sprintf("t-%02d", i), []byte("p"), c.Now(), 3)
	}
	const workers = 8
	var wg sync.WaitGroup
	results := make([][]Lease, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls, err := q.Claim(5, time.Minute)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results[w] = ls
		}(w)
	}
	wg.Wait()
	seen := map[string]int{}
	total := 0
	for _, ls := range results {
		for _, l := range ls {
			seen[l.TaskID]++
			total++
			if l.Attempt != 1 {
				t.Errorf("task %s claimed at attempt %d, want 1", l.TaskID, l.Attempt)
			}
		}
	}
	if total != 20 {
		t.Fatalf("expected 20 leases total, got %d", total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("task %s claimed %d times", id, n)
		}
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 3)
	ls := mustClaim(t, q, 1, time.Minute)
	l := ls[0]

	expiry, err := q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt, 5*time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !expiry.Equal(c.Now().Add(5 * time.Minute)) {
		t.Fatalf("unexpected expiry %v", expiry)
	}

	// 原租约期内推进 2 分钟：未心跳会过期，心跳后仍有效。
	c.Advance(2 * time.Minute)
	if got := mustClaim(t, q, 1, time.Minute); len(got) != 0 {
		t.Fatalf("heartbeat should keep the lease alive, got %+v", got)
	}

	// 错误的租约号 / 尝试号。
	_, err = q.Heartbeat(l.TaskID, "lease-999", l.Attempt, time.Minute)
	requireKind(t, err, KindLeaseMismatch)
	_, err = q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt+1, time.Minute)
	requireKind(t, err, KindLeaseMismatch)

	// 租约彻底过期后心跳为迟到操作。
	c.Advance(10 * time.Minute)
	_, err = q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt, time.Minute)
	requireKind(t, err, KindLeaseMismatch)
}

func TestCompleteAndOutboxAtomicAndReplayable(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 3)
	l := mustClaim(t, q, 1, time.Minute)[0]

	rec, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, []byte("result-1"))
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if rec.Attempt != 1 || string(rec.Result) != "result-1" {
		t.Fatalf("unexpected completion record %+v", rec)
	}
	outbox := q.Outbox()
	if len(outbox) != 1 || outbox[0].TaskID != "t" || outbox[0].Kind != "job.completed" {
		t.Fatalf("completion must atomically write one outbox message, got %+v", outbox)
	}

	// 重放：返回首次结果，不重复通知。
	rec2, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, []byte("result-2"))
	if err != nil {
		t.Fatalf("replayed complete should succeed: %v", err)
	}
	if string(rec2.Result) != "result-1" || rec2.CompletedAt != rec.CompletedAt {
		t.Fatalf("replay must return the first record, got %+v", rec2)
	}
	if len(q.Outbox()) != 1 {
		t.Fatalf("replay must not duplicate the outbox message")
	}

	// 终态上的心跳/失败是状态错误。
	_, err = q.Heartbeat(l.TaskID, l.LeaseID, l.Attempt, time.Minute)
	requireKind(t, err, KindInvalidState)
	requireKind(t, q.Fail(l.TaskID, l.LeaseID, l.Attempt, "x", true), KindInvalidState)
}

func TestLateCompleteCannotOverwriteNewAttempt(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 3)
	l1 := mustClaim(t, q, 1, time.Minute)[0]

	// 租约过期，任务被另一工作者以尝试 2 领取。
	c.Advance(2 * time.Minute)
	l2 := mustClaim(t, q, 1, time.Minute)[0]
	if l2.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", l2.Attempt)
	}

	// 尝试 1 的迟到完成不能覆盖新尝试。
	_, err := q.Complete(l1.TaskID, l1.LeaseID, l1.Attempt, []byte("stale"))
	requireKind(t, err, KindLeaseMismatch)
	j, err := q.Get("t")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if j.Status != StatusLeased || j.LeaseID != l2.LeaseID {
		t.Fatalf("late complete must not disturb the new attempt, got %+v", j)
	}

	// 新尝试正常完成。
	if _, err := q.Complete(l2.TaskID, l2.LeaseID, l2.Attempt, []byte("fresh")); err != nil {
		t.Fatalf("complete new attempt: %v", err)
	}
}

func TestFailRetryThenDeadLetter(t *testing.T) {
	c := newClock()
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	q, err := NewQueue(NewMemoryStore(), WithClock(c.Now), WithRetryPolicy(policy))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 2)

	l1 := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail(l1.TaskID, l1.LeaseID, l1.Attempt, "boom", true); err != nil {
		t.Fatalf("fail: %v", err)
	}
	j, _ := q.Get("t")
	if j.Status != StatusPending || !j.RunAt.Equal(c.Now().Add(time.Second)) {
		t.Fatalf("retryable failure should reschedule at now+1s, got %+v", j)
	}

	// 未到重排时间不可领取；到期后领取到尝试 2。
	if got := mustClaim(t, q, 1, time.Minute); len(got) != 0 {
		t.Fatalf("rescheduled job must wait for its run-at, got %+v", got)
	}
	c.Advance(time.Second)
	l2 := mustClaim(t, q, 1, time.Minute)[0]
	if l2.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", l2.Attempt)
	}

	// 次数用尽 → 死信。
	if err := q.Fail(l2.TaskID, l2.LeaseID, l2.Attempt, "boom again", true); err != nil {
		t.Fatalf("fail: %v", err)
	}
	j, _ = q.Get("t")
	if j.Status != StatusDead || j.LastError != "boom again" {
		t.Fatalf("exhausted retries should dead-letter, got %+v", j)
	}
}

func TestNonRetryableFailGoesDeadImmediately(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 5)
	l := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail(l.TaskID, l.LeaseID, l.Attempt, "fatal", false); err != nil {
		t.Fatalf("fail: %v", err)
	}
	j, _ := q.Get("t")
	if j.Status != StatusDead {
		t.Fatalf("non-retryable failure should dead-letter, got %s", j.Status)
	}
}

func TestExpiredLeaseWithExhaustedAttemptsGoesDead(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "t", []byte("p"), c.Now(), 1)
	mustClaim(t, q, 1, time.Minute) // 工作者崩溃，租约过期且次数已用尽
	c.Advance(2 * time.Minute)
	if got := mustClaim(t, q, 1, time.Minute); len(got) != 0 {
		t.Fatalf("no attempts left, nothing to claim, got %+v", got)
	}
	j, _ := q.Get("t")
	if j.Status != StatusDead {
		t.Fatalf("expired lease with exhausted attempts should dead-letter, got %s", j.Status)
	}
}

func TestStatsAndOutboxAck(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 1)
	mustSubmit(t, q, "b", []byte("p"), c.Now(), 1)
	ls := mustClaim(t, q, 2, time.Minute)
	if _, err := q.Complete(ls[0].TaskID, ls[0].LeaseID, ls[0].Attempt, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	s := q.Stats()
	if s.Leased != 1 || s.Completed != 1 || s.OutboxPending != 1 {
		t.Fatalf("unexpected stats %+v", s)
	}
	msg := q.Outbox()[0]
	if err := q.AckOutbox(msg.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.AckOutbox(msg.ID); err == nil {
		t.Fatalf("double ack should fail")
	} else {
		requireKind(t, err, KindNotFound)
	}
	if q.Stats().OutboxPending != 0 {
		t.Fatalf("outbox should be drained")
	}
}

func TestRetryPolicyDeterministic(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: 10 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Fatalf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestArgumentAndLookupErrors(t *testing.T) {
	q := newTestQueue(t, newClock())
	if _, err := q.Claim(0, time.Minute); err == nil {
		t.Fatalf("batch size 0 should be rejected")
	} else {
		requireKind(t, err, KindInvalidArgument)
	}
	if _, err := q.Claim(1, 0); err == nil {
		t.Fatalf("zero lease duration should be rejected")
	} else {
		requireKind(t, err, KindInvalidArgument)
	}
	_, err := q.Get("missing")
	requireKind(t, err, KindNotFound)
	_, err = q.Complete("missing", "lease-1", 1, nil)
	requireKind(t, err, KindNotFound)
	_, err = q.Complete("missing", "", 1, nil)
	requireKind(t, err, KindInvalidArgument)
	requireKind(t, q.Fail("missing", "lease-1", 1, "x", true), KindNotFound)
}

// TestErrorUnwrapsToTypedError 确保错误可类型断言为 *Error。
func TestErrorUnwrapsToTypedError(t *testing.T) {
	q := newTestQueue(t, newClock())
	_, err := q.Get("nope")
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindNotFound {
		t.Fatalf("expected *Error with KindNotFound, got %v", err)
	}
}
