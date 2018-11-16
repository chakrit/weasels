package weasels

import "bytes"

type BytesSorter [][]byte

func (b BytesSorter) Len() int {
	return len(b)
}

func (b BytesSorter) Less(i, j int) bool {
	return bytes.Compare(b[i], b[j]) < 0
}

func (b BytesSorter) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
