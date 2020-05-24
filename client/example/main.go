package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/mason-leap-lab/infinicache/client"
)

var (
	key      = flag.String("key", "foo", "key name")
	d        = flag.Int("d", 1, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	set      = flag.Bool("set", false, "perform set")
	get      = flag.Bool("get", false, "perform get")
	size     = flag.Uint("size", 1024, "object size")
	addrList = "127.0.0.1:6378"
)

func main() {
	flag.Parse()
	// initial object with random value
	var val []byte
	val = make([]byte, *size)
	rand.Read(val)

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(*d, *p, 32)

	// start dial and PUT/GET
	start := time.Now()
	cli.Dial(addrArr)
	if *set {
		cli.EcSet(*key, val)
	}
	if *get {
		res, _, ok := cli.EcGet(*key, int(*size))
		if !ok {
			fmt.Println("err is", ok)
		}
		fmt.Println("res", res)
	}
	end := time.Since(start)
	fmt.Printf("GET %s, size is %v, duration is %v \n", *key, *size, end)
}
