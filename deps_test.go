package jobleases

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustSubmitDeps(t *testing.T, q *Queue, id string, deps []string) *Job {
	t.Helper()
	j, err := q.SubmitWithDeps(id, []byte("p"), q.now(), 3, deps)
	if err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return j
}

// completeLease 在已领取的租约列表中找到 id 并完成它。
func completeLease(t *testing.T, q *Queue, leases []Lease, id string) {
	t.Helper()
	for _, l := range leases {
		if l.TaskID == id {
			if _, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, nil); err != nil {
				t.Fatalf("complete %s: %v", id, err)
			}
			return
		}
	}
	t.Fatalf("task %s not in leases %+v", id, leases)
}

func TestSubmitWithDepsWaitsUntilAllComplete(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 3)
	mustSubmit(t, q, "b", []byte("p"), c.Now(), 3)
	j := mustSubmitDeps(t, q, "c", []string{"a", "b"})
	if j.Status != StatusWaiting {
		t.Fatalf("dependent should be waiting, got %s", j.Status)
	}
	// 等待中的任务不可领取。
	leases := mustClaim(t, q, 10, time.Minute)
	if len(leases) != 2 {
		t.Fatalf("waiting dependent must not be claimable, got %+v", leases)
	}

	completeLease(t, q, leases, "a")
	j, _ = q.Get("c")
	if j.Status != StatusWaiting {
		t.Fatalf("one dep done, still waiting, got %s", j.Status)
	}

	completeLease(t, q, leases, "b")
	j, _ = q.Get("c")
	if j.Status != StatusPending {
		t.Fatalf("all deps done, should be pending, got %s", j.Status)
	}
	if !j.ReleasedAt.Equal(c.Now()) {
		t.Fatalf("released-at should be recorded, got %v", j.ReleasedAt)
	}
	ls := mustClaim(t, q, 10, time.Minute)
	if len(ls) != 1 || ls[0].TaskID != "c" {
		t.Fatalf("released dependent should be claimable, got %+v", ls)
	}
}

func TestSubmitDepsValidation(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 3)

	// 前置不存在。
	_, err := q.SubmitWithDeps("x", []byte("p"), c.Now(), 3, []string{"missing"})
	requireKind(t, err, KindNotFound)
	// 自我依赖。
	_, err = q.SubmitWithDeps("b", []byte("p"), c.Now(), 3, []string{"b"})
	requireKind(t, err, KindDependency)
	// 依赖链（非循环）可以正常提交。
	mustSubmitDeps(t, q, "d1", []string{"a"})
	mustSubmitDeps(t, q, "d2", []string{"d1"})
	j := mustSubmitDeps(t, q, "d3", []string{"d2", "a"})
	if j.Status != StatusWaiting {
		t.Fatalf("chain dependent should be waiting, got %s", j.Status)
	}
	// 同号不同依赖是幂等冲突。
	_, err = q.SubmitWithDeps("d3", []byte("p"), c.Now(), 3, []string{"d2"})
	requireKind(t, err, KindConflict)
}

func TestSubmitDepsIdempotentReplay(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 3)
	j1 := mustSubmitDeps(t, q, "b", []string{"a"})
	// 同号同负载同依赖 → 返回原任务，忽略新参数。
	j2, err := q.SubmitWithDeps("b", []byte("p"), c.Now().Add(time.Hour), 5, []string{"a"})
	if err != nil || j1 != j2 {
		t.Fatalf("idempotent replay should return the original job, got %v %v", j2, err)
	}
	// 同号不同依赖 → 冲突。
	_, err = q.SubmitWithDeps("b", []byte("p"), c.Now(), 3, nil)
	requireKind(t, err, KindConflict)
}

func TestDepDeadLettersBlocksDependents(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 1)
	mustSubmitDeps(t, q, "b", []string{"a"})
	mustSubmitDeps(t, q, "c", []string{"b"}) // 级联：b 被阻断则 c 也被阻断

	l := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail(l.TaskID, l.LeaseID, l.Attempt, "fatal", false); err != nil {
		t.Fatalf("fail: %v", err)
	}
	jb, _ := q.Get("b")
	if jb.Status != StatusBlocked {
		t.Fatalf("dependent should be blocked, got %s", jb.Status)
	}
	if !strings.Contains(jb.BlockedReason, `"a"`) || !strings.Contains(jb.BlockedReason, "fatal") {
		t.Fatalf("blocked reason should name the failed dep, got %q", jb.BlockedReason)
	}
	jc, _ := q.Get("c")
	if jc.Status != StatusBlocked || !strings.Contains(jc.BlockedReason, `"b"`) {
		t.Fatalf("blocking should cascade, got %s %q", jc.Status, jc.BlockedReason)
	}
	// 阻断是终态：不可领取，操作报状态错误。
	if got := mustClaim(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatalf("blocked tasks must not be claimable, got %+v", got)
	}
	requireKind(t, q.Fail("b", "lease-1", 1, "x", true), KindInvalidState)
}

func TestDepLeaseExpiryDeadBlocksDependents(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 1)
	mustSubmitDeps(t, q, "b", []string{"a"})
	mustClaim(t, q, 1, time.Minute) // 工作者崩溃，租约过期且次数用尽 → 死信
	c.Advance(2 * time.Minute)
	mustClaim(t, q, 1, time.Minute) // 触发死信转换
	jb, _ := q.Get("b")
	if jb.Status != StatusBlocked {
		t.Fatalf("dep dead via lease expiry should block dependent, got %s", jb.Status)
	}
}

func TestSubmitWithAlreadyFinishedDeps(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	mustSubmit(t, q, "done", []byte("p"), c.Now(), 3)
	mustSubmit(t, q, "dead", []byte("p"), c.Now(), 1)
	leases := mustClaim(t, q, 10, time.Minute)
	completeLease(t, q, leases, "done")
	for _, l := range leases {
		if l.TaskID == "dead" {
			if err := q.Fail(l.TaskID, l.LeaseID, l.Attempt, "boom", false); err != nil {
				t.Fatalf("fail: %v", err)
			}
		}
	}

	// 前置已全部完成：直接可领取。
	j := mustSubmitDeps(t, q, "after-done", []string{"done"})
	if j.Status != StatusPending || !j.ReleasedAt.Equal(c.Now()) {
		t.Fatalf("deps already satisfied, should be pending, got %+v", j)
	}
	// 前置已死信：直接阻断并记录原因。
	j = mustSubmitDeps(t, q, "after-dead", []string{"dead"})
	if j.Status != StatusBlocked || !strings.Contains(j.BlockedReason, `"dead"`) {
		t.Fatalf("dep already dead, should be blocked, got %+v", j)
	}
}

func TestConcurrentDepCompletionReleasesOnce(t *testing.T) {
	c := newClock()
	q := newTestQueue(t, c)
	const deps = 8
	ids := make([]string, deps)
	for i := range ids {
		ids[i] = fmt.Sprintf("dep-%d", i)
		mustSubmit(t, q, ids[i], []byte("p"), c.Now(), 3)
	}
	mustSubmitDeps(t, q, "join", ids)

	leases := mustClaim(t, q, deps, time.Minute)
	if len(leases) != deps {
		t.Fatalf("expected %d dep leases, got %d", deps, len(leases))
	}
	// 并发完成全部前置。
	var wg sync.WaitGroup
	for _, l := range leases {
		wg.Add(1)
		go func(l Lease) {
			defer wg.Done()
			if _, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, nil); err != nil {
				t.Errorf("complete %s: %v", l.TaskID, err)
			}
		}(l)
	}
	wg.Wait()

	j, _ := q.Get("join")
	if j.Status != StatusPending {
		t.Fatalf("join should be pending, got %s", j.Status)
	}
	// 只转换一次：后续领取只发放一个租约、尝试号为 1，且不会重复入队。
	got := mustClaim(t, q, 10, time.Minute)
	if len(got) != 1 || got[0].TaskID != "join" || got[0].Attempt != 1 {
		t.Fatalf("join must be leased exactly once, got %+v", got)
	}
	if again := mustClaim(t, q, 10, time.Minute); len(again) != 0 {
		t.Fatalf("join must not be leased twice, got %+v", again)
	}
}

func TestDependencyStateSurvivesRestart(t *testing.T) {
	c := newClock()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("file store: %v", err)
	}
	q, err := NewQueue(store, WithClock(c.Now))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	mustSubmit(t, q, "a", []byte("p"), c.Now(), 1)
	mustSubmitDeps(t, q, "b", []string{"a"})
	mustSubmitDeps(t, q, "c", []string{"b"})
	// a 死信 → b、c 阻断。
	l := mustClaim(t, q, 1, time.Minute)[0]
	if err := q.Fail(l.TaskID, l.LeaseID, l.Attempt, "boom", false); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// 一个已完成的前置 + 等待中的多级依赖。
	mustSubmit(t, q, "ok", []byte("p"), c.Now(), 3)
	l = mustClaim(t, q, 1, time.Minute)[0]
	if _, err := q.Complete(l.TaskID, l.LeaseID, l.Attempt, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mustSubmitDeps(t, q, "d", []string{"ok"})
	mustSubmitDeps(t, q, "e", []string{"ok", "d"})

	// 模拟重启：从同一 FileStore 重建。
	q2, err := NewQueue(store, WithClock(c.Now))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	jb, _ := q2.Get("b")
	if jb.Status != StatusBlocked || jb.BlockedReason == "" {
		t.Fatalf("blocked state and reason must survive restart, got %+v", jb)
	}
	jc, _ := q2.Get("c")
	if jc.Status != StatusBlocked {
		t.Fatalf("cascaded block must survive restart, got %s", jc.Status)
	}
	jd, _ := q2.Get("d")
	if jd.Status != StatusPending || jd.ReleasedAt.IsZero() {
		t.Fatalf("released dependent must survive restart, got %+v", jd)
	}
	if !slices.Equal(jd.DependsOn, []string{"ok"}) {
		t.Fatalf("dependencies must survive restart, got %v", jd.DependsOn)
	}
	je, _ := q2.Get("e")
	if je.Status != StatusWaiting {
		t.Fatalf("waiting dependent must survive restart, got %s", je.Status)
	}
	// 恢复后等待中的任务仍随前置完成而释放。
	l = mustClaim(t, q2, 1, time.Minute)[0]
	if l.TaskID != "d" {
		t.Fatalf("expected to claim d, got %s", l.TaskID)
	}
	if _, err := q2.Complete(l.TaskID, l.LeaseID, l.Attempt, nil); err != nil {
		t.Fatalf("complete d: %v", err)
	}
	je, _ = q2.Get("e")
	if je.Status != StatusPending {
		t.Fatalf("dependent should release after restart, got %s", je.Status)
	}
	s := q2.Stats()
	if s.Blocked != 2 || s.Completed != 2 || s.Waiting != 0 || s.Pending != 1 {
		t.Fatalf("unexpected stats after restart %+v", s)
	}
}
