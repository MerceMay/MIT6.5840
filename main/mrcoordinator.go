package main

import (
	"6.5840/mr"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, "Usage: mrcoordinator inputfile...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(1 * time.Second)
	}

	time.Sleep(time.Second)
}
