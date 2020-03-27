package kv

import (
	"errors"
	"fmt"
)

// TODO: add an LRU cache proxy for performance
type Cluster struct {
	shardLocator ShardLocator
	head         *Shard // Circular LinkedList for maintaining shards
	lookup       map[int]*Shard
	size         int
}

func NewCluster(size int, shardLocator ShardLocator) (*Cluster, error) {
	if size == 0 {
		return nil, errors.New("cluster size must be at-least 1")
	}
	c := &Cluster{
		shardLocator: shardLocator,
		head:         nil,
		size:         size,
		lookup:       make(map[int]*Shard),
	}
	c.provision()
	return c, nil
}

func (c *Cluster) provision() {
	if c.head != nil {
		// already provisioned
		return
	}

	var (
		provisioned int
		tail        *Shard
	)

	for provisioned < c.size {
		shard, found := c.lookup[provisioned]
		if !found {
			shard = NewShard()
			c.lookup[provisioned] = shard
		}
		provisioned++
		if c.head == nil {
			c.head = shard
			tail = shard
			continue
		}

		// set current shard's previous next
		tail.next = shard
		shard.prev = tail
		shard.next = nil

		// set tail to new shard
		tail = shard
	}

	// form a circular linked list towards the end to connect head and tail
	tail.next = c.head
	c.head.prev = tail
	fmt.Println("provisioned")

}

func (c *Cluster) Write(key string, value *Value) error {
	// identify which shard
	location := c.shardLocator.Locate(key)

	fmt.Printf("primary shard location for %s is %d \n", key, location)

	shard, _ := c.lookup[location]

	return shard.Write(key, value)
}

func (c *Cluster) Read(key string) (*Value, error) {
	// identify which shard
	location := c.shardLocator.Locate(key)

	fmt.Printf("primary shard location for %s is %d \n", key, location)

	shard, _ := c.lookup[location]
	return shard.Read(key)
}

func (c *Cluster) Dump() {
	for idx, shard := range c.lookup {
		fmt.Printf("\n\nKey-Values for shard %d\n\n", idx)
		shard.Print()
	}
}
