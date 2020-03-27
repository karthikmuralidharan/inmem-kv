package kv

import "hash/fnv"

func NewHash32aShardLocator(size int) ShardLocator {
	return &Hash32aShardLocator{size: uint32(size)}
}

type Hash32aShardLocator struct {
	size uint32
}

func (l *Hash32aShardLocator) Locate(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	sum := h.Sum32()
	return int(sum % l.size)
}
