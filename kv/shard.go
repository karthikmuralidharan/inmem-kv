package kv

import (
	"errors"
	"fmt"
	"sync"
)

type Shard struct {
	prev       *Shard
	next       *Shard
	failed     bool
	store      map[string]*Value
	writeMutex sync.Mutex
}

func NewShard() *Shard {
	return &Shard{
		prev:   nil,
		next:   nil,
		failed: false,
		store:  make(map[string]*Value),
	}
}

func (shard *Shard) Read(key string) (*Value, error) {
	// TODO: attempt to load-balance reads between the current, prev, next shards
	// TODO: Account for errors and check if all the nearby shards are failing.
	if !shard.failed {
		return shard.store[key], nil
	}
	if !shard.prev.failed {
		return shard.prev.store[key], nil
	}
	if !shard.next.failed {
		return shard.next.store[key], nil
	}
	return nil, errors.New("all the shards with the key are in failed state")
}

func (shard *Shard) Write(key string, value *Value) error {
	// TODO: may need to check if I can remove this constraint
	if shard.failed {
		return errors.New("primary shard is down")
	}

	// store in primary shard
	// TODO: account for proper CAS concurrency
	shard.writeMutex.Lock()
	defer shard.writeMutex.Unlock()
	old, found := shard.store[key]
	if found {
		if old.Version >= value.Version {
			return errors.New("please update the version of the value you intend to write. It should be greater than the older version")
		}
	}
	shard.store[key] = value

	// store in nearby replicas
	shard.prev.store[key] = value
	shard.next.store[key] = value
	return nil
}

func (shard *Shard) Print() {
	for k, v := range shard.store {
		fmt.Printf("key: %s, value: %+v \n", k, v)
	}
}
