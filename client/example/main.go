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
	d        = flag.Int("d", 2, "data shard")
	p        = flag.Int("p", 1, "parity shard")
	addrList = "127.0.0.1:6378"
)

func main() {
	flag.Parse()

	cli := client.NewClient(*d, *p, 32)
	addrArr := strings.Split(addrList, ",")

	cli.Dial(addrArr)

	// Locks up after iterating for awhile
	for idx := 0; idx < 10_000; idx++ {
		fmt.Println("idx: ", idx)

		// Initial object with random value
		inputRandom := fmt.Sprintf("Hello test %d!", rand.Intn(1_000_000))
		val := []byte(inputRandom)

		// Initialize key with random value
		n := rand.Intn(1_000_000)
		cacheKeyGo := fmt.Sprintf("foo_%d", n)

		cli.Set(cacheKeyGo, val)

		start := time.Now()
		reader, ok := cli.Get(cacheKeyGo)

		dt := time.Since(start)
		if !ok {
			panic("Internal error!")
		}
		buf, _ := reader.ReadAll()

		fmt.Printf("GET %s:%s(%v)\n", cacheKeyGo, string(buf), dt)
	}
}
