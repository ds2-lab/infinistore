# InfiniStore Client Library

Client Library for [InfiniStore](https://github.com/ds2-lab/infinistore)

## Examples
A simple client example with PUT/GET:
```go

package main

import (
	"github.com/ds2-lab/infinistore/client"
	"fmt"
	"math/rand"
	"strings"
)

var addrList = "127.0.0.1:6378"

func main() {
	// initial object with random value
	var val []byte
	val = make([]byte, 1024)
	rand.Read(val)

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	cli := client.NewClient(10, 2, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)
	cli.Set("foo", val)
	reader, ok := cli.Get("foo")
	if ok {
		fmt.Printf("GET foo:%s\n", string(reader.ReadAll()))
	}
}
```
