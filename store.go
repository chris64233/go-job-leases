package jobleases

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// snapshot 是持久化到磁盘的完整状态。所有状态变化都通过整体快照原子落盘，
// 因此进程在任意时刻崩溃后重启都能恢复到最后一次已确认的状态。
type snapshot struct {
	Jobs        map[string]*Job `json:"jobs"`
	Outbox      []OutboxEntry   `json:"outbox"`
	JobSeq      int64           `json:"job_seq"`
	LeaseSeq    int64           `json:"lease_seq"`
	OutboxSeq   int64           `json:"outbox_seq"`
	PersistedAt time.Time       `json:"persisted_at"`
}

func newSnapshot() *snapshot {
	return &snapshot{Jobs: make(map[string]*Job)}
}

// Store 是队列状态的持久化抽象。Update 中的修改以事务方式串行执行，
// fn 返回错误时整笔修改回滚。实现必须保证 Update 返回成功后状态已持久化，
// 进程崩溃后可以恢复。
type Store interface {
	Update(fn func(s *snapshot) error) error
	View(fn func(s *snapshot) error) error
}

// MemoryStore 仅保存在内存中，用于测试。不具备崩溃恢复能力。
type MemoryStore struct {
	mu sync.Mutex
	s  snapshot
}

// NewMemoryStore 创建一个空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{s: *newSnapshot()}
}

func (m *MemoryStore) Update(fn func(s *snapshot) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	working := m.s
	jobsCopy := make(map[string]*Job, len(m.s.Jobs))
	for k, v := range m.s.Jobs {
		jobsCopy[k] = cloneJob(v)
	}
	working.Jobs = jobsCopy
	working.Outbox = cloneOutbox(m.s.Outbox)
	if err := fn(&working); err != nil {
		return err
	}
	m.s = working
	return nil
}

func (m *MemoryStore) View(fn func(s *snapshot) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn(&m.s)
}

// FileStore 把状态以 JSON 快照形式写入文件：每次提交先写临时文件并 fsync，
// 再原子 rename 覆盖目标文件，最后 fsync 目录。任何时刻崩溃，磁盘上要么是
// 旧快照要么是新快照，不会出现半写状态。
type FileStore struct {
	mu   sync.Mutex
	path string
	s    snapshot
}

// NewFileStore 打开（或创建）path 处的快照文件。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, invalidArg("store path is empty")
	}
	f := &FileStore{path: path, s: *newSnapshot()}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := f.persistLocked(); err != nil {
				return nil, err
			}
			return f, nil
		}
		return nil, fmt.Errorf("read snapshot %s: %w", path, err)
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &f.s); err != nil {
			return nil, fmt.Errorf("corrupt snapshot %s: %w", path, err)
		}
		if f.s.Jobs == nil {
			f.s.Jobs = make(map[string]*Job)
		}
	}
	return f, nil
}

func (f *FileStore) Update(fn func(s *snapshot) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	jobsCopy := make(map[string]*Job, len(f.s.Jobs))
	for k, v := range f.s.Jobs {
		jobsCopy[k] = cloneJob(v)
	}
	working := f.s
	working.Jobs = jobsCopy
	working.Outbox = cloneOutbox(f.s.Outbox)

	if err := fn(&working); err != nil {
		return err
	}
	working.PersistedAt = time.Now()
	if err := f.persistWith(&working); err != nil {
		return err
	}
	f.s = working
	return nil
}

func (f *FileStore) View(fn func(s *snapshot) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fn(&f.s)
}

// persistLocked 在已持锁且使用 f.s 的场景下落盘（初始化）。
func (f *FileStore) persistLocked() error {
	f.s.PersistedAt = time.Now()
	return f.persistWith(&f.s)
}

func (f *FileStore) persistWith(s *snapshot) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		cleanup()
		return fmt.Errorf("rename snapshot: %w", err)
	}
	// fsync 目录，保证 rename 在崩溃后仍然生效。
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open snapshot dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("fsync snapshot dir: %w", err)
	}
	return d.Close()
}

func cloneJob(j *Job) *Job {
	if j == nil {
		return nil
	}
	c := *j
	c.Payload = append([]byte(nil), j.Payload...)
	if j.Lease != nil {
		l := *j.Lease
		c.Lease = &l
	}
	if j.Result != nil {
		r := *j.Result
		r.Result = append([]byte(nil), j.Result.Result...)
		c.Result = &r
	}
	return &c
}

func cloneOutbox(in []OutboxEntry) []OutboxEntry {
	if in == nil {
		return nil
	}
	out := make([]OutboxEntry, len(in))
	copy(out, in)
	return out
}

func newID(prefix string, seq int64) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%010d_%s", prefix, seq, hex.EncodeToString(b[:]))
}
