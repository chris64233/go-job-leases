package jobleases

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDependencyBlocksClaimUntilComplete 单链依赖：前置完成前不可领取，
// 完成与释放发生在同一次操作里。
func TestDependencyBlocksClaimUntilComplete(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("pa"), c.Now(), 1)
	b, err := q.Submit("b", []byte("pb"), c.Now(), 1, "a")
	if err != nil {
		t.Fatalf("submit b: %v", err)
	}
	if b.Status != StatusWaiting || len(b.Dependencies) != 1 || len(b.Unresolved) != 1 {
		t.Fatalf("b should start waiting with one unresolved dep, got %+v", b)
	}
	la := mustClaim(t, q, 10, time.Minute)
	if len(la) != 1 || la[0].TaskID != "a" {
		t.Fatalf("only a should be claimable while b waits, got %+v", la)
	}
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("a already leased, nothing else should be claimable, got %+v", got)
	}
	if _, err := q.Complete("a", la[0].LeaseID, la[0].Attempt, nil); err != nil {
		t.Fatalf("complete a: %v", err)
	}
	jb, _ := q.Get("b")
	if jb.Status != StatusPending || !jb.ReleasedAt.Equal(c.Now()) || len(jb.Unresolved) != 0 {
		t.Fatalf("b should be released exactly when a completes, got %+v", jb)
	}
	lb := mustClaim(t, q, 10, time.Minute)
	if len(lb) != 1 || lb[0].TaskID != "b" || lb[0].Attempt != 1 {
		t.Fatalf("b should now be claimable on its first attempt, got %+v", lb)
	}
}

// TestFanInRequiresEveryDependency 多个前置：缺一不可；最后一个前置完成时
// 才唯一地转为 pending，转换只发生一次。
func TestFanInRequiresEveryDependency(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", nil, c.Now(), 1)
	mustSubmit(t, q, "b", nil, c.Now(), 1)
	mustSubmit(t, q, "c", nil, c.Now(), 1)
	j, err := q.Submit("down", nil, c.Now(), 1, "a", "b", "c")
	if err != nil {
		t.Fatalf("submit down: %v", err)
	}
	if j.Status != StatusWaiting || len(j.Unresolved) != 3 {
		t.Fatalf("down waits on three deps, got %+v", j)
	}
	// 一次领走三个前置的租约，随后按任意顺序完成，避免领取顺序干扰。
	upstream := mustClaim(t, q, 10, time.Minute)
	if len(upstream) != 3 {
		t.Fatalf("expected 3 upstream leases, got %d", len(upstream))
	}
	lease := map[string]Lease{}
	for _, l := range upstream {
		lease[l.TaskID] = l
	}

	complete := func(id string) {
		if _, err := q.Complete(id, lease[id].LeaseID, lease[id].Attempt, nil); err != nil {
			t.Fatalf("complete %s: %v", id, err)
		}
	}

	complete("a")
	if d, _ := q.Get("down"); d.Status != StatusWaiting || len(d.Unresolved) != 2 {
		t.Fatalf("down still waits after a, got %+v", d)
	}
	complete("b")
	if d, _ := q.Get("down"); d.Status != StatusWaiting || len(d.Unresolved) != 1 {
		t.Fatalf("down still waits after b, got %+v", d)
	}
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("down must not be claimable before c completes, got %+v", got)
	}
	complete("c")
	d, _ := q.Get("down")
	if d.Status != StatusPending || d.ReleasedAt.IsZero() || len(d.Unresolved) != 0 {
		t.Fatalf("down must transition to pending exactly once, got %+v", d)
	}
	// c 的完成请求重放返回首次结果，不改变后继状态。
	rec, err := q.Complete("c", lease["c"].LeaseID, lease["c"].Attempt, nil)
	if err != nil || rec.Attempt != 1 {
		t.Fatalf("replayed complete should return first record, got %v %v", rec, err)
	}
	if d2, _ := q.Get("down"); d2.Status != StatusPending {
		t.Fatalf("down must stay pending, got %s", d2.Status)
	}
	leases := mustClaim(t, q, 10, time.Minute)
	if len(leases) != 1 || leases[0].TaskID != "down" {
		t.Fatalf("down leased exactly once, got %+v", leases)
	}
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("down must not be leased twice, got %+v", got)
	}
}

// TestConcurrentCompletionReleasesOnce 多个前置被并发完成时，后继只能转换一次，
// 且领取与最后一个完成原子衔接：不会提前租出或重复入队。
func TestConcurrentCompletionReleasesOnce(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	const n = 16
	for i := 0; i < n; i++ {
		mustSubmit(t, q, fmt.Sprintf("dep-%02d", i), nil, c.Now(), 1)
	}
	deps := make([]string, n)
	for i := range deps {
		deps[i] = fmt.Sprintf("dep-%02d", i)
	}
	if d, err := q.Submit("down", nil, c.Now(), 1, deps...); err != nil {
		t.Fatalf("submit down: %v", err)
	} else if d.Status != StatusWaiting {
		t.Fatalf("down should wait, got %s", d.Status)
	}
	upstream := mustClaim(t, q, n, time.Minute)
	if len(upstream) != n {
		t.Fatalf("claim all upstreams, got %d", len(upstream))
	}

	var wg sync.WaitGroup
	for _, l := range upstream {
		wg.Add(1)
		go func(l Lease) {
			defer wg.Done()
			if _, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, nil); err != nil {
				t.Errorf("complete %s: %v", l.TaskID, err)
			}
		}(l)
	}
	wg.Wait()

	d, _ := q.Get("down")
	if d.Status != StatusPending || len(d.Unresolved) != 0 {
		t.Fatalf("down released once after concurrent completion, got %+v", d)
	}
	var total int
	for i := 0; i < 4; i++ {
		total += len(mustClaim(t, q, 8, time.Minute))
	}
	if total != 1 {
		t.Fatalf("down must enter the claimable set exactly once, got %d leases", total)
	}
}

// TestDeadUpstreamBlocksAndRecordsReason 前置进入死信：后继转为 blocked 终态并
// 记录直接原因；阻断逐层级联，每层都带上直接上游信息。
func TestDeadUpstreamBlocksAndRecordsReason(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", nil, c.Now(), 1)
	mustSubmitDep(t, q, "b", "a")
	mustSubmitDep(t, q, "c", "b")

	l := mustClaim(t, q, 10, time.Minute)[0]
	if l.TaskID != "a" {
		t.Fatalf("expected a, got %s", l.TaskID)
	}
	if err := q.Fail("a", l.LeaseID, l.Attempt, "fatal boom", false); err != nil {
		t.Fatalf("fail a: %v", err)
	}
	b, _ := q.Get("b")
	if b.Status != StatusBlocked || b.Unresolved != nil {
		t.Fatalf("b should be blocked, got %+v", b)
	}
	if b.BlockedReason == nil || b.BlockedReason.Upstream != "a" ||
		b.BlockedReason.UpstreamStatus != StatusDead || b.BlockedReason.Cause != "fatal boom" {
		t.Fatalf("b must record a's dead-letter cause, got %+v", b.BlockedReason)
	}
	if !b.BlockedReason.At.Equal(c.Now()) {
		t.Fatalf("blocked-at timestamp should be recorded, got %v", b.BlockedReason.At)
	}
	cc, _ := q.Get("c")
	if cc.Status != StatusBlocked || cc.BlockedReason == nil || cc.BlockedReason.Upstream != "b" ||
		cc.BlockedReason.UpstreamStatus != StatusBlocked {
		t.Fatalf("c must be transitively blocked by b, got %+v", cc)
	}

	// blocked 是终态：不可领取，也拒绝心跳/失败/完成。
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("blocked jobs must never be claimed, got %+v", got)
	}
	if _, err := q.Heartbeat("b", "lease-x", 1, time.Minute); err == nil {
		t.Fatalf("heartbeat on blocked job should fail")
	} else {
		requireKind(t, err, KindInvalidState)
	}
	requireKind(t, q.Fail("b", "lease-x", 1, "x", true), KindInvalidState)
	if _, err := q.Complete("b", "lease-x", 1, nil); err == nil {
		t.Fatalf("complete on blocked job should fail")
	} else {
		requireKind(t, err, KindInvalidState)
	}
}

// TestLeaseExpiryExhaustedBlocksDependents 租约过期且次数用尽走死信路径，
// 同样要阻断后继。
func TestLeaseExpiryExhaustedBlocksDependents(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", nil, c.Now(), 1)
	mustSubmitDep(t, q, "b", "a")
	mustClaim(t, q, 10, time.Minute) // 工作者崩溃
	c.Advance(2 * time.Minute)
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("nothing to claim after expiry exhaustion, got %+v", got)
	}
	a, _ := q.Get("a")
	b, _ := q.Get("b")
	if a.Status != StatusDead {
		t.Fatalf("a should be dead, got %s", a.Status)
	}
	if b.Status != StatusBlocked || b.BlockedReason == nil || b.BlockedReason.Upstream != "a" {
		t.Fatalf("b blocked via expiry dead-letter, got %+v", b)
	}
}

// TestSubmitWithTerminalDependency 提交时前置已在终态：全部完成则立即可领取；
// 已有死信/被阻断则立即 blocked 并记录原因，无需等待。
func TestSubmitWithTerminalDependency(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)

	// 先走死信链，避免其他 pending 任务干扰领取顺序。
	mustSubmit(t, q, "dead", nil, c.Now(), 1)
	lx := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail("dead", lx.LeaseID, lx.Attempt, "nope", false); err != nil {
		t.Fatalf("fail dead: %v", err)
	}
	blocked, err := q.Submit("after-dead", nil, c.Now(), 1, "dead")
	if err != nil {
		t.Fatalf("submit after-dead: %v", err)
	}
	if blocked.Status != StatusBlocked || blocked.BlockedReason == nil ||
		blocked.BlockedReason.Upstream != "dead" || blocked.BlockedReason.Cause != "nope" {
		t.Fatalf("job submitted after a dead dep must be blocked with cause, got %+v", blocked)
	}
	// 提交时引用已被阻断的任务，原因链应继续向上描述。
	afterBlocked, err := q.Submit("after-blocked", nil, c.Now(), 1, "after-dead")
	if err != nil {
		t.Fatalf("submit after-blocked: %v", err)
	}
	if afterBlocked.Status != StatusBlocked || afterBlocked.BlockedReason == nil ||
		afterBlocked.BlockedReason.UpstreamStatus != StatusBlocked {
		t.Fatalf("must inherit blocked chain, got %+v", afterBlocked)
	}

	mustSubmit(t, q, "done", nil, c.Now(), 1)
	ld := mustClaim(t, q, 1, time.Minute)[0]
	if _, err := q.Complete("done", ld.LeaseID, ld.Attempt, nil); err != nil {
		t.Fatalf("complete done: %v", err)
	}
	after, err := q.Submit("after-done", nil, c.Now(), 1, "done")
	if err != nil {
		t.Fatalf("submit after-done: %v", err)
	}
	if after.Status != StatusPending || after.ReleasedAt.IsZero() {
		t.Fatalf("job whose deps already completed should start pending, got %+v", after)
	}
}

// TestDependencyValidation 依赖参数校验：未知前置、空串、自依赖、重复去重。
func TestDependencyValidation(t *testing.T) {
	q := newTestQueue(t, newClock())
	mustSubmit(t, q, "a", nil, time.Time{}, 1)

	_, err := q.Submit("x", nil, time.Time{}, 1, "ghost")
	requireKind(t, err, KindInvalidArgument)
	_, err = q.Submit("y", nil, time.Time{}, 1, "a", "")
	requireKind(t, err, KindInvalidArgument)
	_, err = q.Submit("z", nil, time.Time{}, 1, "z")
	requireKind(t, err, KindCycleDependency)

	// 重复引用同一前置按一个依赖处理。
	j, err := q.Submit("dup", nil, time.Time{}, 1, "a", "a", "a")
	if err != nil {
		t.Fatalf("dedup submit: %v", err)
	}
	if len(j.Dependencies) != 1 || len(j.Unresolved) != 1 {
		t.Fatalf("duplicated dependency must collapse to one, got %+v", j)
	}

	// 当前提交模型下依赖只能指向已存在任务，天然不形成多节点环；
	// 自依赖是唯一可达的成环方式，单独确认错误类别。
	mustSubmitDep(t, q, "b", "a")
	_, err = q.Submit("w", nil, time.Time{}, 1, "a", "b", "w")
	requireKind(t, err, KindCycleDependency)
}

// TestDependencyIdempotentSubmit 已存在任务的重复提交不允许补改依赖。
func TestDependencyIdempotentSubmit(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", nil, c.Now(), 1)
	first, err := q.Submit("b", []byte("p"), c.Now(), 1, "a")
	if err != nil {
		t.Fatalf("submit b: %v", err)
	}
	again, err := q.Submit("b", []byte("p"), c.Now(), 1)
	if err != nil {
		t.Fatalf("idempotent resubmit: %v", err)
	}
	if again != first || len(again.Dependencies) != 1 {
		t.Fatalf("idempotent replay must keep original dependencies, got %+v", again)
	}
}

// TestDependencyStats waiting 与 blocked 计入统计。
func TestDependencyStats(t *testing.T) {
	q := newTestQueue(t, newClock())
	mustSubmit(t, q, "a", nil, time.Time{}, 1)
	mustSubmitDep(t, q, "w1", "a")
	mustSubmitDep(t, q, "w2", "a")
	s := q.Stats()
	if s.Pending != 1 || s.Waiting != 2 || s.Blocked != 0 {
		t.Fatalf("unexpected stats %+v", s)
	}
	l := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail("a", l.LeaseID, l.Attempt, "x", false); err != nil {
		t.Fatalf("fail: %v", err)
	}
	s = q.Stats()
	if s.Dead != 1 || s.Blocked != 2 {
		t.Fatalf("dead/blocked stats wrong: %+v", s)
	}
}

// TestDependencyCrashRecovery 依赖、未决集合、释放时间与阻断原因都必须持久化，
// 重启后行为与重启前一致。
func TestDependencyCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	c := newClock()
	mk := func() *Queue {
		st, err := NewFileStore(dir)
		if err != nil {
			t.Fatalf("file store: %v", err)
		}
		q, err := NewQueue(st, WithClock(c.Now))
		if err != nil {
			t.Fatalf("queue: %v", err)
		}
		return q
	}

	q1 := mk()
	mustSubmit(t, q1, "a", nil, c.Now(), 1)
	mustSubmit(t, q1, "b", nil, c.Now(), 1)
	mustSubmitDeps(t, q1, "c", "a", "b")
	mustSubmitDeps(t, q1, "d", "c")

	// 只领取 a（同 RunAt 时按任务号排序，claim(1) 返回 a）：c 仍在等待，
	// 快照中保留一个未决依赖。
	leaseA := mustClaim(t, q1, 1, time.Minute)[0]
	if leaseA.TaskID != "a" {
		t.Fatalf("expected lease for a, got %s", leaseA.TaskID)
	}
	if _, err := q1.Complete("a", leaseA.LeaseID, leaseA.Attempt, nil); err != nil {
		t.Fatalf("complete a: %v", err)
	}

	// 重启：等待状态、未决集合、依赖列表、反向索引都恢复。
	q2 := mk()
	cj, err := q2.Get("c")
	if err != nil {
		t.Fatalf("get c: %v", err)
	}
	if cj.Status != StatusWaiting || len(cj.Dependencies) != 2 || len(cj.Unresolved) != 1 {
		t.Fatalf("waiting state must survive restart, got %+v", cj)
	}
	if _, ok := cj.Unresolved["b"]; !ok {
		t.Fatalf("b should remain unresolved after restart, got %v", cj.Unresolved)
	}
	if dj, _ := q2.Get("d"); dj.Status != StatusWaiting {
		t.Fatalf("d should still wait, got %s", dj.Status)
	}

	// 完成 b：释放 c；再完成 c：释放 d。重启后释放时刻保留。
	leaseB := mustClaim(t, q2, 1, time.Minute)[0]
	if leaseB.TaskID != "b" {
		t.Fatalf("expected lease for b, got %s", leaseB.TaskID)
	}
	if _, err := q2.Complete("b", leaseB.LeaseID, leaseB.Attempt, nil); err != nil {
		t.Fatalf("complete b: %v", err)
	}
	leaseC := mustClaim(t, q2, 1, time.Minute)[0]
	if leaseC.TaskID != "c" {
		t.Fatalf("expected lease for c, got %s", leaseC.TaskID)
	}
	cBefore, _ := q2.Get("c")
	if _, err := q2.Complete("c", leaseC.LeaseID, leaseC.Attempt, nil); err != nil {
		t.Fatalf("complete c: %v", err)
	}

	q3 := mk()
	c2, _ := q3.Get("c")
	if c2.Status != StatusCompleted || !c2.ReleasedAt.Equal(cBefore.ReleasedAt) {
		t.Fatalf("c completion and release time must persist, got %+v", c2)
	}
	d2, _ := q3.Get("d")
	if d2.Status != StatusPending || d2.ReleasedAt.IsZero() {
		t.Fatalf("d must be released across restart via rebuilt reverse index, got %+v", d2)
	}
	if got := mustClaim(t, q3, 10, time.Minute); len(got) != 1 || got[0].TaskID != "d" {
		t.Fatalf("d claimable after restart, got %+v", got)
	}
}

// TestBlockedReasonCrashRecovery 阻断原因必须持久化，重启后仍可读、仍为终态。
func TestBlockedReasonCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	c := newClock()
	mk := func() *Queue {
		st, err := NewFileStore(dir)
		if err != nil {
			t.Fatalf("file store: %v", err)
		}
		q, err := NewQueue(st, WithClock(c.Now))
		if err != nil {
			t.Fatalf("queue: %v", err)
		}
		return q
	}
	q1 := mk()
	mustSubmit(t, q1, "a", nil, c.Now(), 1)
	mustSubmitDep(t, q1, "b", "a")
	l := mustClaim(t, q1, 1, time.Minute)[0]
	if err := q1.Fail("a", l.LeaseID, l.Attempt, "disk on fire", false); err != nil {
		t.Fatalf("fail: %v", err)
	}

	q2 := mk()
	b, _ := q2.Get("b")
	if b.Status != StatusBlocked || b.BlockedReason == nil {
		t.Fatalf("blocked state must survive restart, got %+v", b)
	}
	if b.BlockedReason.Upstream != "a" || b.BlockedReason.Cause != "disk on fire" ||
		b.BlockedReason.UpstreamStatus != StatusDead || b.BlockedReason.At.IsZero() {
		t.Fatalf("blocked reason must survive restart intact, got %+v", b.BlockedReason)
	}
	if got := mustClaim(t, q2, 10, time.Minute); len(got) != 0 {
		t.Fatalf("blocked job must remain unclaimable after restart, got %+v", got)
	}
}

func mustSubmitDep(t *testing.T, q *Queue, id, dep string) *Job {
	t.Helper()
	return mustSubmitDeps(t, q, id, dep)
}

func mustSubmitDeps(t *testing.T, q *Queue, id string, deps ...string) *Job {
	t.Helper()
	j, err := q.Submit(id, nil, time.Time{}, 1, deps...)
	if err != nil {
		t.Fatalf("submit %s deps %v: %v", id, deps, err)
	}
	return j
}
