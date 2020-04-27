package util

func Ifelse(expr bool, then interface{}, or interface{}) interface{} {
	if expr {
		return then
	} else {
		return or
	}
}
