package main

import (
	"flag"
	"math/rand"
	"strings"

	"github.com/wangaoone/LambdaObjectstore/client"
)

var (
	key      = flag.String("key", "foo", "key name")
	d        = flag.Int("d", 1, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	addrList = "127.0.0.1:6378"
)

func main() {
	flag.Parse()
	// initial object with random value
	var val []byte
	val = make([]byte, 1024)
	rand.Read(val)

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(*d, *p, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)
	cli.EcSet(*key, val)
	//cli.EcGet("foo", 1024)
}
