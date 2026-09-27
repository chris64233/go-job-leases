package jobleases

import "fmt"

// ErrorKind 区分错误的类别，调用方可据此决定重试、报警或修正参数。
type ErrorKind int

const (
	// KindInvalidArgument 参数问题：缺少任务号、批量大小非法、租约号为空等。
	KindInvalidArgument ErrorKind = iota
	// KindNotFound 任务或 outbox 消息不存在。
	KindNotFound
	// KindConflict 幂等冲突：相同外部任务号提交了不同的负载。
	KindConflict
	// KindLeaseMismatch 租约问题：租约号/尝试号不匹配，或租约已过期（迟到操作）。
	KindLeaseMismatch
	// KindInvalidState 状态问题：任务已死信/已完成，不允许当前操作。
	KindInvalidState
	// KindDependency 依赖问题：前置任务不存在、自我依赖或循环依赖。
	KindDependency
	// KindInternal 持久化等内部错误。
	KindInternal
)

// Error 是队列返回的统一错误类型。
type Error struct {
	Kind ErrorKind
	Op   string
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("jobleases: %s: %s", e.Op, e.Msg)
}

// KindOf 返回错误的类别；err 不是 *Error 时 ok 为 false。
func KindOf(err error) (kind ErrorKind, ok bool) {
	if e, isErr := err.(*Error); isErr {
		return e.Kind, true
	}
	return 0, false
}

func invalidArg(op, msg string) *Error { return &Error{Kind: KindInvalidArgument, Op: op, Msg: msg} }
func notFound(op, msg string) *Error   { return &Error{Kind: KindNotFound, Op: op, Msg: msg} }
func conflict(op, msg string) *Error   { return &Error{Kind: KindConflict, Op: op, Msg: msg} }
func leaseErr(op, msg string) *Error   { return &Error{Kind: KindLeaseMismatch, Op: op, Msg: msg} }
func stateErr(op, msg string) *Error   { return &Error{Kind: KindInvalidState, Op: op, Msg: msg} }
func depErr(op, msg string) *Error     { return &Error{Kind: KindDependency, Op: op, Msg: msg} }
func internalErr(op string, err error) *Error {
	return &Error{Kind: KindInternal, Op: op, Msg: err.Error()}
}
