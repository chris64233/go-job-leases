package jobleases

import "errors"

// ErrorKind 区分错误的类别，调用方可以据此决定重试、报警还是修正请求。
type ErrorKind string

const (
	// KindInvalidArgument 参数问题：请求字段缺失或非法。
	KindInvalidArgument ErrorKind = "invalid_argument"
	// KindNotFound 指定的任务不存在。
	KindNotFound ErrorKind = "not_found"
	// KindConflict 幂等冲突：相同外部任务号提交了不同的负载。
	KindConflict ErrorKind = "conflict"
	// KindLease 租约问题：租约号/尝试号不匹配，或租约已过期。
	KindLease ErrorKind = "lease"
	// KindState 状态问题：任务当前状态不允许该操作（如已完成后再失败）。
	KindState ErrorKind = "state"
)

// Error 是队列返回的分类错误。
type Error struct {
	Kind ErrorKind
	Msg  string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Msg }

// IsKind 判断 err 是否属于指定类别。
func IsKind(err error, kind ErrorKind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	return false
}

func invalidArg(msg string) error { return &Error{Kind: KindInvalidArgument, Msg: msg} }
func notFound(msg string) error   { return &Error{Kind: KindNotFound, Msg: msg} }
func conflict(msg string) error   { return &Error{Kind: KindConflict, Msg: msg} }
func leaseErr(msg string) error   { return &Error{Kind: KindLease, Msg: msg} }
func stateErr(msg string) error   { return &Error{Kind: KindState, Msg: msg} }
