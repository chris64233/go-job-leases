package jobleases

import (
	"testing"
	"time"
)

// TestCrashRecovery 模拟进程崩溃：队列一提交并领取任务后，
// 在同一 FileStore 上重建队列，验证租约、尝试号、outbox 全部恢复。
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	c := newClock()

	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("file store: %v", err)
	}
	q1, err := NewQueue(store1, WithClock(c.Now))
	if err != nil {
		t.Fatalf("queue 1: %v", err)
	}
	mustSubmit(t, q1, "t1", []byte("p1"), c.Now(), 3)
	mustSubmit(t, q1, "t2", []byte("p2"), c.Now(), 3)
	ls := mustClaim(t, q1, 2, time.Minute)
	if _, err := q1.Complete(ls[0].TaskID, ls[0].LeaseID, ls[0].Attempt, []byte("done")); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// 模拟崩溃：不执行任何清理，直接在同一目录上重建。

	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("file store 2: %v", err)
	}
	q2, err := NewQueue(store2, WithClock(c.Now))
	if err != nil {
		t.Fatalf("queue 2: %v", err)
	}

	j1, err := q2.Get("t1")
	if err != nil {
		t.Fatalf("get t1: %v", err)
	}
	if j1.Status != StatusCompleted || j1.Completion == nil || string(j1.Completion.Result) != "done" {
		t.Fatalf("completed job not recovered: %+v", j1)
	}
	j2, err := q2.Get("t2")
	if err != nil {
		t.Fatalf("get t2: %v", err)
	}
	if j2.Status != StatusLeased || j2.Attempts != 1 || j2.LeaseID == "" {
		t.Fatalf("leased job not recovered: %+v", j2)
	}
	if len(q2.Outbox()) != 1 {
		t.Fatalf("outbox not recovered")
	}

	// 恢复后完成重放仍返回首次结果、不重复通知。
	rec, err := q2.Complete("t1", ls[0].LeaseID, ls[0].Attempt, []byte("other"))
	if err != nil {
		t.Fatalf("replayed complete after recovery: %v", err)
	}
	if string(rec.Result) != "done" {
		t.Fatalf("replay after recovery must return the first result, got %q", rec.Result)
	}
	if len(q2.Outbox()) != 1 {
		t.Fatalf("replay after recovery must not duplicate outbox")
	}

	// 恢复后租约仍有效：不能被重复领取；过期后可再次领取且尝试号递增。
	if got := mustClaim(t, q2, 10, time.Minute); len(got) != 0 {
		t.Fatalf("live lease must survive recovery, got %+v", got)
	}
	c.Advance(2 * time.Minute)
	got := mustClaim(t, q2, 10, time.Minute)
	if len(got) != 1 || got[0].TaskID != "t2" || got[0].Attempt != 2 {
		t.Fatalf("expected t2 attempt 2 after lease expiry, got %+v", got)
	}
}

// TestFileStoreIdempotencyAcrossRestart 提交幂等键在重启后仍然生效。
func TestFileStoreIdempotencyAcrossRestart(t *testing.T) {
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
	mustSubmit(t, q1, "dup", []byte("p"), c.Now(), 2)

	q2 := mk()
	j, err := q2.Submit("dup", []byte("p"), c.Now(), 2)
	if err != nil {
		t.Fatalf("idempotent resubmit after restart: %v", err)
	}
	if j.TaskID != "dup" {
		t.Fatalf("unexpected job %+v", j)
	}
	if _, err := q2.Submit("dup", []byte("changed"), c.Now(), 2); err == nil {
		t.Fatalf("payload conflict after restart should be rejected")
	} else {
		requireKind(t, err, KindConflict)
	}
	if got := q2.Stats(); got.Pending != 1 {
		t.Fatalf("conflict must not create a second job, stats %+v", got)
	}
}
