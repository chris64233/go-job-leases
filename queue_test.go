package jobleases

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestQueue() (*Queue, *fakeClock) {
	c := newFakeClock()
	return NewQueueWithClock(NewMemoryStore(), c.now), c
}

func mustSubmit(t *testing.T, q *Queue, ext string, payload []byte, runAt time.Time, maxAttempts int) *Job {
	t.Helper()
	out, err := q.Submit(SubmitInput{ExternalID: ext, Payload: payload, RunAt: runAt, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatalf("Submit(%s): %v", ext, err)
	}
	if !out.Created {
		t.Fatalf("Submit(%s): expected Created=true", ext)
	}
	return out.Job
}

func TestSubmitIdempotency(t *testing.T) {
	q, _ := newTestQueue()
	payload := []byte(`{"x":1}`)

	first, err := q.Submit(SubmitInput{ExternalID: "ext-1", Payload: payload, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if !first.Created {
		t.Fatal("first submit should be Created")
	}

	// 同号同负载：返回原任务。
	second, err := q.Submit(SubmitInput{ExternalID: "ext-1", Payload: append([]byte(nil), payload...), MaxAttempts: 3})
	if err != nil {
		t.Fatalf("idempotent Submit: %v", err)
	}
	if second.Created || second.Job.ID != first.Job.ID {
		t.Fatalf("idempotent submit should return original job: created=%v id1=%s id2=%s",
			second.Created, first.Job.ID, second.Job.ID)
	}

	// 同号换负载：冲突。
	_, err = q.Submit(SubmitInput{ExternalID: "ext-1", Payload: []byte("different")})
	if !IsKind(err, KindConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	// 参数问题。
	if _, err := q.Submit(SubmitInput{Payload: payload}); !IsKind(err, KindInvalidArgument) {
		t.Fatalf("missing external_id should be invalid argument, got %v", err)
	}
	if _, err := q.Submit(SubmitInput{ExternalID: "x"}); !IsKind(err, KindInvalidArgument) {
		t.Fatalf("missing payload should be invalid argument, got %v", err)
	}
}

func TestLeaseDueAndFuture(t *testing.T) {
	q, clock := newTestQueue()
	due := mustSubmit(t, q, "due", []byte("d"), time.Time{}, 1)
	future := mustSubmit(t, q, "future", []byte("f"), clock.now().Add(time.Hour), 1)

	got, err := q.Lease(LeaseInput{WorkerID: "w1", Count: 10, LeaseFor: 24 * time.Hour})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 1 || got[0].ID != due.ID {
		t.Fatalf("expected only the due job, got %d", len(got))
	}
	if got[0].Attempts != 1 || got[0].Lease == nil || got[0].Lease.Attempt != 1 {
		t.Fatalf("expected attempt 1 with lease, got %+v", got[0])
	}
	if got[0].Lease.WorkerID != "w1" {
		t.Fatalf("worker id mismatch: %s", got[0].Lease.WorkerID)
	}

	// 有效租约持有期间不可重复领取。
	got, err = q.Lease(LeaseInput{WorkerID: "w2", Count: 10, LeaseFor: time.Minute})
	if err != nil {
		t.Fatalf("second Lease: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("leased job should not be leasable again, got %d", len(got))
	}

	// 时间走到未来任务到期后可以领取（第一个任务的租约仍然有效）。
	clock.advance(time.Hour + time.Second)
	got, err = q.Lease(LeaseInput{WorkerID: "w2", Count: 10, LeaseFor: time.Minute})
	if err != nil {
		t.Fatalf("third Lease: %v", err)
	}
	if len(got) != 1 || got[0].ID != future.ID {
		t.Fatalf("expected the future job to become due, got %d", len(got))
	}

	// 参数问题。
	if _, err := q.Lease(LeaseInput{LeaseFor: time.Minute}); !IsKind(err, KindInvalidArgument) {
		t.Fatalf("missing worker_id should be invalid argument, got %v", err)
	}
	if _, err := q.Lease(LeaseInput{WorkerID: "w", LeaseFor: 0}); !IsKind(err, KindInvalidArgument) {
		t.Fatalf("non-positive lease duration should be invalid argument, got %v", err)
	}
}

func TestConcurrentLeaseNoDuplicateAttempt(t *testing.T) {
	q, _ := newTestQueue()
	const n = 50
	for i := 0; i < n; i++ {
		mustSubmit(t, q, "ext-"+strconv.Itoa(i), []byte("p"), time.Time{}, 1)
	}

	const workers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]int{} // jobID -> attempt
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := q.Lease(LeaseInput{WorkerID: "w", Count: 5, LeaseFor: time.Minute})
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, j := range got {
					if a, ok := seen[j.ID]; ok {
						t.Errorf("job %s leased twice: attempts %d and %d", j.ID, a, j.Lease.Attempt)
					}
					seen[j.ID] = j.Lease.Attempt
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("expected %d distinct leased jobs, got %d", n, len(seen))
	}
}

func TestExpiredLeaseRelesedWithIncrementingAttempt(t *testing.T) {
	q, clock := newTestQueue()
	job := mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 3)

	first, err := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	l1 := first[0].Lease

	// 租约未过期，不能领取。
	if got, _ := q.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute}); len(got) != 0 {
		t.Fatalf("job should still be held, got %d", len(got))
	}

	clock.advance(time.Minute + time.Second)
	second, err := q.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute})
	if err != nil {
		t.Fatalf("re-lease after expiry: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("expected expired job to be re-leased, got %d", len(second))
	}
	l2 := second[0].Lease
	if l2.ID == l1.ID || l2.Attempt != 2 {
		t.Fatalf("expected new lease with attempt 2, got %+v", l2)
	}

	// 迟到的旧尝试完成/心跳/失败都必须被拒绝。
	_, err = q.Complete(CompleteInput{JobID: job.ID, LeaseID: l1.ID, Attempt: 1, Result: []byte("late")})
	if !IsKind(err, KindLease) {
		t.Fatalf("late complete with stale lease should be lease error, got %v", err)
	}
	_, err = q.Heartbeat(HeartbeatInput{JobID: job.ID, LeaseID: l1.ID, Attempt: 1, ExtendBy: time.Minute})
	if !IsKind(err, KindLease) {
		t.Fatalf("late heartbeat with stale lease should be lease error, got %v", err)
	}
	_, err = q.Fail(FailInput{JobID: job.ID, LeaseID: l1.ID, Attempt: 1, Retryable: true})
	if !IsKind(err, KindLease) {
		t.Fatalf("late fail with stale lease should be lease error, got %v", err)
	}

	// 尝试号不匹配、租约号不匹配同样拒绝。
	_, err = q.Complete(CompleteInput{JobID: job.ID, LeaseID: l2.ID, Attempt: 1})
	if !IsKind(err, KindLease) {
		t.Fatalf("attempt mismatch should be lease error, got %v", err)
	}
	_, err = q.Complete(CompleteInput{JobID: job.ID, LeaseID: "lease_nope", Attempt: 2})
	if !IsKind(err, KindLease) {
		t.Fatalf("wrong lease id should be lease error, got %v", err)
	}

	// 新尝试可以正常完成。
	out, err := q.Complete(CompleteInput{JobID: job.ID, LeaseID: l2.ID, Attempt: 2, Result: []byte("ok")})
	if err != nil {
		t.Fatalf("complete on current lease: %v", err)
	}
	if out.Job.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s", out.Job.Status)
	}
}

func TestHeartbeat(t *testing.T) {
	q, clock := newTestQueue()
	job := mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 2)
	got, _ := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	l := got[0].Lease

	clock.advance(30 * time.Second)
	renewed, err := q.Heartbeat(HeartbeatInput{JobID: job.ID, LeaseID: l.ID, Attempt: 1, ExtendBy: time.Minute})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !renewed.ExpiresAt.Equal(clock.now().Add(time.Minute)) {
		t.Fatalf("heartbeat should extend from now, got expires=%v now=%v", renewed.ExpiresAt, clock.now())
	}

	// 原租约本应在 30s 后过期，但心跳续约后再过 50s 仍有效。
	clock.advance(50 * time.Second)
	if leased, _ := q.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute}); len(leased) != 0 {
		t.Fatal("job should still be held after heartbeat")
	}

	// 续约后再过 20s（距心跳 70s）租约过期。
	clock.advance(20 * time.Second)
	if leased, _ := q.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute}); len(leased) != 1 {
		t.Fatal("job should be releasable after heartbeat expiry")
	}

	// 参数问题。
	_, err = q.Heartbeat(HeartbeatInput{JobID: job.ID, LeaseID: l.ID, Attempt: 0, ExtendBy: time.Minute})
	if !IsKind(err, KindInvalidArgument) {
		t.Fatalf("bad attempt should be invalid argument, got %v", err)
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	p := RetryPolicy{InitialDelay: time.Second, Multiplier: 2, MaxDelay: 10 * time.Second}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // 封顶
		{10, 10 * time.Second},
	}
	for _, c := range cases {
		if got := p.Delay(c.attempt); got != c.want {
			t.Errorf("Delay(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestFailRescheduleAndDeadLetter(t *testing.T) {
	q, clock := newTestQueue()
	job := mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 3)

	failOnce := func(worker string, wantAttempt int) time.Time {
		t.Helper()
		got, err := q.Lease(LeaseInput{WorkerID: worker, LeaseFor: time.Minute})
		if err != nil || len(got) != 1 {
			t.Fatalf("Lease: %v len=%d", err, len(got))
		}
		if got[0].Lease.Attempt != wantAttempt {
			t.Fatalf("expected attempt %d, got %d", wantAttempt, got[0].Lease.Attempt)
		}
		out, err := q.Fail(FailInput{
			JobID: job.ID, LeaseID: got[0].Lease.ID, Attempt: got[0].Lease.Attempt,
			Reason: "boom", Retryable: true,
		})
		if err != nil {
			t.Fatalf("Fail: %v", err)
		}
		if out.Dead {
			t.Fatalf("attempt %d should not be dead", wantAttempt)
		}
		return out.NextRunAt
	}

	next := failOnce("w1", 1)
	if !next.Equal(clock.now().Add(time.Second)) {
		t.Fatalf("retry 1 should run in 1s, next=%v now=%v", next, clock.now())
	}
	// 未到重排时间不可领取。
	if got, _ := q.Lease(LeaseInput{WorkerID: "w", LeaseFor: time.Minute}); len(got) != 0 {
		t.Fatal("job should wait for backoff")
	}
	clock.advance(time.Second)
	next = failOnce("w2", 2)
	if !next.Equal(clock.now().Add(2 * time.Second)) {
		t.Fatalf("retry 2 should run in 2s, next=%v now=%v", next, clock.now())
	}
	clock.advance(2 * time.Second)

	// 第 3 次（最后一次）失败 -> 死信。
	got, _ := q.Lease(LeaseInput{WorkerID: "w3", LeaseFor: time.Minute})
	out, err := q.Fail(FailInput{
		JobID: job.ID, LeaseID: got[0].Lease.ID, Attempt: 3,
		Reason: "last boom", Retryable: true,
	})
	if err != nil {
		t.Fatalf("final Fail: %v", err)
	}
	if !out.Dead || out.Job.Status != StatusDead {
		t.Fatalf("expected dead after exhausting attempts, got dead=%v status=%s", out.Dead, out.Job.Status)
	}
	if out.Job.LastError != "last boom" {
		t.Fatalf("last error not recorded: %q", out.Job.LastError)
	}
	// 死信任务不可再领取。
	clock.advance(time.Hour)
	if leased, _ := q.Lease(LeaseInput{WorkerID: "w", LeaseFor: time.Minute}); len(leased) != 0 {
		t.Fatal("dead job should never be leased")
	}
	// 死信后再失败是状态错误。
	_, err = q.Fail(FailInput{JobID: job.ID, LeaseID: got[0].Lease.ID, Attempt: 3, Retryable: true})
	if !IsKind(err, KindState) {
		t.Fatalf("failing a dead job should be state error, got %v", err)
	}
}

func TestNonRetryableFailureGoesStraightToDead(t *testing.T) {
	q, _ := newTestQueue()
	job := mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 5)
	got, _ := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	out, err := q.Fail(FailInput{
		JobID: job.ID, LeaseID: got[0].Lease.ID, Attempt: 1,
		Reason: "permanent", Retryable: false,
	})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if !out.Dead {
		t.Fatal("non-retryable failure should dead-letter immediately")
	}
}

func TestCompleteWritesOutboxAtomicallyAndReplays(t *testing.T) {
	q, _ := newTestQueue()
	job := mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 2)
	got, _ := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	l := got[0].Lease

	out, err := q.Complete(CompleteInput{JobID: job.ID, LeaseID: l.ID, Attempt: 1, Result: []byte("result-v1")})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if out.Replayed {
		t.Fatal("first complete should not be a replay")
	}

	entries, err := q.PendingOutbox()
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(entries) != 1 || entries[0].JobID != job.ID || string(entries[0].Result) != "result-v1" {
		t.Fatalf("expected exactly one outbox entry, got %+v", entries)
	}

	// 重放同一租约的完成请求：返回首次结果，不重复通知。
	replay, err := q.Complete(CompleteInput{JobID: job.ID, LeaseID: l.ID, Attempt: 1, Result: []byte("result-v2")})
	if err != nil {
		t.Fatalf("replayed Complete: %v", err)
	}
	if !replay.Replayed {
		t.Fatal("second identical complete should be marked replayed")
	}
	if string(replay.Job.Result.Result) != "result-v1" {
		t.Fatalf("replay must return the first result, got %q", replay.Job.Result.Result)
	}
	entries, _ = q.PendingOutbox()
	if len(entries) != 1 {
		t.Fatalf("replay must not duplicate outbox entries, got %d", len(entries))
	}

	// 投递成功后可以清除 outbox。
	if err := q.MarkOutboxSent(entries[0].ID); err != nil {
		t.Fatalf("MarkOutboxSent: %v", err)
	}
	entries, _ = q.PendingOutbox()
	if len(entries) != 0 {
		t.Fatalf("outbox should be empty after ack, got %d", len(entries))
	}

	// 已完成任务再带别的租约完成是状态错误。
	_, err = q.Complete(CompleteInput{JobID: job.ID, LeaseID: "lease_other", Attempt: 2})
	if !IsKind(err, KindState) {
		t.Fatalf("completing with a different lease should be state error, got %v", err)
	}
}

func TestStatsAndLookups(t *testing.T) {
	q, clock := newTestQueue()

	// 每次只让一个到期任务存在，使 Count:1 的领取对象确定。
	done := mustSubmit(t, q, "done", []byte("p"), time.Time{}, 1)
	lg, _ := q.Lease(LeaseInput{WorkerID: "w", Count: 1, LeaseFor: time.Minute})
	if lg[0].ID != done.ID {
		t.Fatalf("expected to lease %s, got %s", done.ID, lg[0].ID)
	}
	if _, err := q.Complete(CompleteInput{JobID: done.ID, LeaseID: lg[0].Lease.ID, Attempt: 1}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	dead := mustSubmit(t, q, "dead", []byte("p"), time.Time{}, 1)
	lg, _ = q.Lease(LeaseInput{WorkerID: "w", Count: 1, LeaseFor: time.Minute})
	if lg[0].ID != dead.ID {
		t.Fatalf("expected to lease %s, got %s", dead.ID, lg[0].ID)
	}
	if _, err := q.Fail(FailInput{JobID: dead.ID, LeaseID: lg[0].Lease.ID, Attempt: 1, Retryable: false}); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	leased := mustSubmit(t, q, "leased", []byte("p"), time.Time{}, 1)
	lg, _ = q.Lease(LeaseInput{WorkerID: "w", Count: 1, LeaseFor: 24 * time.Hour})
	if lg[0].ID != leased.ID {
		t.Fatalf("expected to lease %s, got %s", leased.ID, lg[0].ID)
	}

	// 最后再放两个 pending：一个到期、一个一小时后到期。
	mustSubmit(t, q, "pending-due", []byte("p"), time.Time{}, 1)
	mustSubmit(t, q, "pending-future", []byte("p"), clock.now().Add(time.Hour), 1)

	st, err := q.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Pending != 2 || st.Leased != 1 || st.Completed != 1 || st.Dead != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if st.DueNow != 1 { // 只有 pending-due
		t.Fatalf("expected DueNow=1, got %d (%+v)", st.DueNow, st)
	}
	clock.advance(time.Hour + time.Second)
	st, _ = q.Stats()
	if st.DueNow != 2 {
		t.Fatalf("after future becomes due expected DueNow=2, got %d (%+v)", st.DueNow, st)
	}

	if _, err := q.Get("no-such-id"); !IsKind(err, KindNotFound) {
		t.Fatalf("Get missing should be not_found, got %v", err)
	}
	if j, err := q.GetByExternalID("pending-due"); err != nil || j == nil {
		t.Fatalf("GetByExternalID: %v %v", j, err)
	}
	if _, err := q.GetByExternalID("missing"); !IsKind(err, KindNotFound) {
		t.Fatalf("GetByExternalID missing should be not_found, got %v", err)
	}
}

func TestExpiredLeaseCountedAsDue(t *testing.T) {
	q, clock := newTestQueue()
	mustSubmit(t, q, "ext", []byte("p"), time.Time{}, 1)
	ls, _ := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	if len(ls) != 1 {
		t.Fatalf("expected 1 lease, got %d", len(ls))
	}
	st, _ := q.Stats()
	if st.Leased != 1 || st.DueNow != 0 {
		t.Fatalf("expected leased=1 due=0, got %+v", st)
	}
	clock.advance(time.Minute + time.Second)
	st, _ = q.Stats()
	if st.Leased != 0 || st.DueNow != 1 {
		t.Fatalf("expected expired lease counted as due, got %+v", st)
	}
}

func TestFileStoreCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	clock := newFakeClock()

	open := func() *Queue {
		t.Helper()
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		return NewQueueWithClock(store, clock.now)
	}

	q := open()
	job := mustSubmit(t, q, "ext-1", []byte("payload-1"), time.Time{}, 3)
	mustSubmit(t, q, "ext-2", []byte("payload-2"), clock.now().Add(time.Hour), 1)
	got, _ := q.Lease(LeaseInput{WorkerID: "w1", LeaseFor: time.Minute})
	if len(got) != 1 || got[0].ID != job.ID {
		t.Fatalf("expected to lease ext-1, got %d", len(got))
	}
	if _, err := q.Complete(CompleteInput{
		JobID: job.ID, LeaseID: got[0].Lease.ID, Attempt: 1, Result: []byte("done"),
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// 模拟进程崩溃：丢弃队列，重新从磁盘打开。
	q2 := open()
	j, err := q2.Get(job.ID)
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if j.Status != StatusCompleted || j.Result == nil || string(j.Result.Result) != "done" {
		t.Fatalf("state not recovered: %+v", j)
	}
	entries, _ := q2.PendingOutbox()
	if len(entries) != 1 || entries[0].ExternalID != "ext-1" {
		t.Fatalf("outbox not recovered, got %+v", entries)
	}

	// 重放完成：崩溃后重发的完成请求仍返回首次结果、不重复通知。
	replay, err := q2.Complete(CompleteInput{
		JobID: job.ID, LeaseID: j.Result.LeaseID, Attempt: j.Result.Attempt, Result: []byte("again"),
	})
	if err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if !replay.Replayed || string(replay.Job.Result.Result) != "done" {
		t.Fatalf("replay after restart should return first result, got %+v", replay)
	}
	entries, _ = q2.PendingOutbox()
	if len(entries) != 1 {
		t.Fatalf("outbox should stay at 1 after replay, got %d", len(entries))
	}

	// 未到期任务在崩溃恢复后仍不可领取。
	if leased, _ := q2.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute}); len(leased) != 0 {
		t.Fatalf("future job must remain pending, got %d", len(leased))
	}
	clock.advance(time.Hour + time.Second)
	leased, err := q2.Lease(LeaseInput{WorkerID: "w2", LeaseFor: time.Minute})
	if err != nil || len(leased) != 1 || leased[0].ExternalID != "ext-2" {
		t.Fatalf("future job should lease after time passes: %v %+v", err, leased)
	}

	// 再次重开，已发放的租约与尝试号仍然存在。
	q3 := open()
	got3, _ := q3.Lease(LeaseInput{WorkerID: "w3", LeaseFor: time.Minute})
	if len(got3) != 0 {
		t.Fatalf("lease should survive restart, got %d", len(got3))
	}
	j2, _ := q3.Get(leased[0].ID)
	if j2.Status != StatusLeased || j2.Lease == nil || j2.Lease.WorkerID != "w2" || j2.Attempts != 1 {
		t.Fatalf("lease state not recovered: %+v", j2)
	}
}
