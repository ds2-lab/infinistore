package util

type Int int

func (i *Int) Int() int {
	return int(*i)
}

func (i *Int) Add1() int {
	*i++
	return int(*i)
}

func (i *Int) Add(j int) int {
	*i += Int(j)
	return int(*i)
}

func (i *Int) Sub1() int {
	*i--
	return int(*i)
}

func (i *Int) Sub(j int) int {
	*i -= Int(j)
	return int(*i)
}
