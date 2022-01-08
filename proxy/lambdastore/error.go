package lambdastore

type LambdaErrorType int

const (
	LambdaErrorUncategoried LambdaErrorType = iota
	LambdaErrorTimeout
)

type LambdaError struct {
	error
	typ LambdaErrorType
}

func (e *LambdaError) IsTimeout() bool {
	return e.typ == LambdaErrorTimeout
}

func IsLambdaTimeout(err error) bool {
	if lambdaError, ok := err.(*LambdaError); ok {
		return lambdaError.IsTimeout()
	} else {
		return false
	}
}
