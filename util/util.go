package util

// Assert assert the condition to be true, otherwise panic
func Assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
