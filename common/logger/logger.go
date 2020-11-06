package logger

// Logger - Interface to pass into Proxy for it to log messages
type ILogger interface {
	Trace(format string, args ...interface{})
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	GetLevel() int
}

const LOG_LEVEL_ALL int = 0
const LOG_LEVEL_INFO int = 1
const LOG_LEVEL_WARN int = 2
const LOG_LEVEL_NONE int = 3

// Func Function wrapper that support lazy evaluation for the logger
type Func func() string

func (f Func) String() string {
	return f()
}

// NewFunc Create the function wrapper for func() string
func NewFunc(f Func) Func {
	return f
}

// NewFunc Create the function wrapper for func(arg interface{}) string
func NewFuncWithArg(f func(arg interface{}) string, arg interface{}) Func {
	return func() string {
		return f(arg)
	}
}

// NewFunc Create the function wrapper for func(arg ...interface{}) string
func NewFuncWithArgs(f func(arg ...interface{}) string, args ...interface{}) Func {
	return func() string {
		return f(args...)
	}
}
