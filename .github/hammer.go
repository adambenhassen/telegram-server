package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// dials addr repeatedly from many goroutines and reports how the dials ended.
func main() {
	var refused, reset, timeout, other, ok int64
	var wg sync.WaitGroup
	for w := 0; w < 60; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				c, err := net.DialTimeout("tcp", os.Args[1], 5*time.Second)
				switch {
				case err == nil:
					atomic.AddInt64(&ok, 1)
					c.Close()
				case strings.Contains(err.Error(), "connection refused"):
					atomic.AddInt64(&refused, 1)
				case strings.Contains(err.Error(), "connection reset"):
					atomic.AddInt64(&reset, 1)
				case strings.Contains(err.Error(), "i/o timeout"):
					atomic.AddInt64(&timeout, 1)
				default:
					atomic.AddInt64(&other, 1)
					fmt.Println("other:", err)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("%s ok=%d refused=%d reset=%d timeout=%d other=%d\n",
		os.Args[1], ok, refused, reset, timeout, other)
}
