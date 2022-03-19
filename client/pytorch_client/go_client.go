package main

import "C"
import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/mason-leap-lab/infinicache/client"
)

var (
	d        = flag.Int("d", 2, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	addrList = "3.239.126.0:6378"
)

//export getFromCache
func getFromCache(cacheKeyC *C.char) *C.char {
	cacheKeyGo := C.GoString(cacheKeyC)

	flag.Parse()

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(*d, *p, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)

	start := time.Now()
	reader, ok := cli.Get(cacheKeyGo)
	dt := time.Since(start)
	if !ok {
		return C.CString("NOT_IN")
	}

	buf, _ := reader.ReadAll()
	fmt.Printf("GET %s(%v)\n", cacheKeyGo, dt)
	return C.CString(string(buf))
}

//export setInCache
func setInCache(cacheKeyC *C.char, inputDataC *C.char) {
	cacheKeyGo := C.GoString(cacheKeyC)

	flag.Parse()
	inputGo := C.GoString(inputDataC)

	valBytes := []byte(inputGo)

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(*d, *p, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)
	ok := cli.Set(cacheKeyGo, valBytes)
	if !ok {
		return
	}
	fmt.Printf("SET %s\n", cacheKeyGo)

}

func main() {}
