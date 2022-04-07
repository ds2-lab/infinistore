package main

// Need to run: go build -o ecClient.so -buildmode=c-shared go_client.go

/*
#include <stdlib.h>
*/
import "C"
import (
	"flag"
	"strings"

	"github.com/mason-leap-lab/infinicache/client"
)

var (
	d        = flag.Int("d", 2, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	addrList = "127.0.0.1:6378"
	cli      *client.Client
)

//export getFromCache
func getFromCache(cacheKeyC *C.char) *C.char {
	cacheKeyGo := C.GoString(cacheKeyC)

	reader, ok := cli.Get(cacheKeyGo)
	if !ok {
		return C.CString("NOT_IN")
	}

	buf, _ := reader.ReadAll()
	return C.CString(string(buf))
}

//export setInCache
func setInCache(cacheKeyC *C.char, inputDataC *C.char) {
	cacheKeyGo := C.GoString(cacheKeyC)

	inputGo := C.GoString(inputDataC)

	valBytes := []byte(inputGo)

	cli.Set(cacheKeyGo, valBytes)

}

//export initializeVars
func initializeVars() {
	flag.Parse()

	cli = client.NewClient(*d, *p, 32)
	addrArr := strings.Split(addrList, ",")
	cli.Dial(addrArr)
}

func main() {}
