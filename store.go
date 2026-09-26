package jobleases

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// state 是队列的全部可持久化状态。
type state struct {
	Jobs   map[string]*Job `json:"jobs"` // 以外部任务号为键
	Outbox []OutboxMessage `json:"outbox"`
	Seq    uint64          `json:"seq"` // 单调序号，用于生成租约号与消息号
}

func newState() *state {
	return &state{Jobs: make(map[string]*Job)}
}

// Store 持久化队列状态。Save 必须原子生效：要么全部落盘，要么不生效。
type Store interface {
	Load() (*state, error)
	Save(s *state) error
}

// MemoryStore 内存实现，用于测试与不关心崩溃恢复的场景。
type MemoryStore struct {
	mu sync.Mutex
	s  *state
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{s: newState()} }

func (m *MemoryStore) Load() (*state, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.s, nil
}

func (m *MemoryStore) Save(s *state) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.s = s
	return nil
}

// FileStore 以 JSON 快照持久化到单个文件：先写临时文件、fsync、再 rename，
// 保证崩溃时要么看到旧快照要么看到新快照，不会看到半份数据。
type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("jobleases: create store dir: %w", err)
	}
	return &FileStore{path: filepath.Join(dir, "state.json")}, nil
}

func (f *FileStore) Load() (*state, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return nil, err
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("jobleases: decode state: %w", err)
	}
	if s.Jobs == nil {
		s.Jobs = make(map[string]*Job)
	}
	return &s, nil
}

func (f *FileStore) Save(s *state) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fp.Write(data); err != nil {
		fp.Close()
		return err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return err
	}
	if err := fp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
