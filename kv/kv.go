package kv

type ShardLocator interface {
	Locate(string) int
}

type Value struct {
	Data    interface{}
	Version int
}

type ReaderWriter interface {
	Read(key string) (*Value, error)
	Write(key string, value *Value) error
}
