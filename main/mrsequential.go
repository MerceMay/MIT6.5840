package main

import (
	"fmt"
	"6.5840/mr"
	"log"
	"os"
	"plugin"
	"sort"
)

type ByKeyValue []mr.KeyValue

func (a ByKeyValue) Len() int {
	return len(a)
}
func (a ByKeyValue) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKeyValue) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprint(os.Stderr, "Usage: mrsequential xxx.so inputfile...\n")
		os.Exit(1)
	}

	mapFunction, reduceFunction := LoadMapReduce(os.Args[1])

	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Error opening file %s: %v", filename, err)
		}
		content, err := os.ReadFile(filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("Error reading file %s: %v", filename, err)
		}
		keyValueArr := mapFunction(filename, string(content))
		intermediate = append(intermediate, keyValueArr...)
	}

	sort.Sort(ByKeyValue(intermediate))

	outputFile, _ := os.Create("mr-out-0")
	defer outputFile.Close()

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunction(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

}

func LoadMapReduce(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("Error loading plugin: %v", err)
	}
	mapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("Error looking up Map function: %v", err)
	}
	mapFunction := mapf.(func(string, string) []mr.KeyValue)
	reducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("Error looking up Reduce function: %v", err)
	}
	reduceFunction := reducef.(func(string, []string) string)
	return mapFunction, reduceFunction
}
