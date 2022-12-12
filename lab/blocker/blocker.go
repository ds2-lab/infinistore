package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// This experiment simulates the situation that one routine is reading data and more routines are waiting for data.
func main() {
	var done sync.WaitGroup

	var mu sync.Mutex
	signal := sync.NewCond(&mu)

	routines := 10
	// notifiers := make([]func(), routines)

	start := time.Now()
	done.Add(1)
	target := 1000                   // total data to be transmitted, 1000 for transmissio 1M of data.
	interval := 6 * time.Microsecond // transmission time for 1K data.
	var passed int32
	var slepted time.Duration
	// Read routine.
	go func() {
		for i := 0; i < target; i++ {
			// wait.Add(1)
			start := time.Now()
			time.Sleep(interval)
			slepted += time.Since(start)
			// atomic.AddInt32(&passed, 1)
			passed += 1
			mu.Lock()
			signal.Broadcast()
			mu.Unlock()
			// for _, notifier := range notifiers {
			// 	if notifier != nil {
			// 		notifier()
			// 	}
			// }
		}
		done.Done()
	}()

	// Run routines that will block and wait for reading.
	for i := 0; i < routines; i++ {
		done.Add(1)
		go func(i int) {
			// var wait mysync.WaitGroup
			// notifiers[i] = func() {
			// 	wait.Done()
			// }
			if target > 1 {
				time.Sleep(interval * time.Duration(rand.Intn(target/2)))
			}
			read := atomic.LoadInt32(&passed)
			// fmt.Printf("Routine %d: start from %d\n", i, read)

			for read < int32(target) {
				// wait.Add(1)
				// wait.Wait()
				mu.Lock()
				// if atomic.LoadInt32(&passed) <= read {
				if passed <= read {
					signal.Wait()
				}
				mu.Unlock()
				// read = atomic.LoadInt32(&passed)
				read = passed
				// fmt.Printf("Routine %d: passed %d\n", i, read)
			}
			done.Done()
		}(i)
	}

	done.Wait()

	duration := time.Since(start)
	fmt.Printf("Duration: %v, overhead: %v\n", slepted, duration-slepted)
}
