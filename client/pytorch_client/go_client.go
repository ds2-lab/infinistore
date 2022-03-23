package main

/*
#include <stdlib.h>
*/
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
	addrList = "35.175.107.9:6378"
	cli      *client.Client
)

//export getFromCache
func getFromCache(cacheKeyC *C.char) *C.char {
	cacheKeyGo := C.GoString(cacheKeyC)

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

	inputGo := C.GoString(inputDataC)

	valBytes := []byte(inputGo)

	ok := cli.Set(cacheKeyGo, valBytes)
	if !ok {
		return
	}
	fmt.Printf("SET %s\n", cacheKeyGo)

}

//export initializeVars
func initializeVars() {
	flag.Parse()

	cli = client.NewClient(*d, *p, 32)
	addrArr := strings.Split(addrList, ",")
	cli.Dial(addrArr)
}

func main() {}
