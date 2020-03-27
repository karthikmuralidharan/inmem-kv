package main

import (
	"fmt"
	"log"

	"github.com/karthikmuralidharan/inmem-kv/kv"
)

func main() {
	size := 10
	locator := kv.NewHash32aShardLocator(size)
	cluster, err := kv.NewCluster(size, locator)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(cluster.Write("test", &kv.Value{
		Data:    10,
		Version: 1,
	}))
	fmt.Println(cluster.Read("test"))
	fmt.Println(cluster.Write("test", &kv.Value{
		Data:    11,
		Version: 2,
	}))
	fmt.Println(cluster.Write("test", &kv.Value{
		Data:    10,
		Version: 2,
	}))
	fmt.Println(cluster.Read("test"))
	cluster.Dump()

}
