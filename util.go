package main

func lastIndexByteBefore(d []byte, delimiter byte, before int) int {
	for i := before; i >= 0; i-- {
		if d[i] == delimiter {
			return i
		}
	}
	return -1
}
func indexByteAfter(d []byte, delimiter byte, after int) int {
	for i := after; i < len(d); i++ {
		if d[i] == delimiter {
			return i
		}
	}
	return -1
}
