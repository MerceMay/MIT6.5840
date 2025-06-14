package main

import (
	"6.5840/mr"
	"fmt"
	"log"
	"os"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapFunction, reduceFunction := loadPlugin(os.Args[1])

	mr.Worker(mapFunction, reduceFunction)
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Println(err)
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmap, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapFunction := xmap.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reduceFunction := xreducef.(func(string, []string) string)
	return mapFunction, reduceFunction
}
