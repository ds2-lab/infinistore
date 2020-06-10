package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/mason-leap-lab/infinicache/client"
)

var (
	key      = flag.String("key", "foo", "key name")
	d        = flag.Int("d", 2, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	getonly  = flag.Bool("getonly", false, "try get only")
	addrList = "127.0.0.1:6378"
)

func main() {
	flag.Parse()
	// initial object with random value
	var val []byte
	val = []byte("Hello infinity!")

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(*d, *p, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)
	if !*getonly {
		cli.Set(*key, val)
	}

	start := time.Now()
	reader, ok := cli.Get(*key)
	dt := time.Since(start)
	if !ok {
		panic("Internal error!")
	}

	buf, _ := reader.ReadAll()
	fmt.Printf("GET %s:%s(%v)\n", *key, string(buf), dt)
}
