package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"

	"../mr"
)

type KeyValueArray []mr.KeyValue

func (kva KeyValueArray) Len() int {
	return len(kva)
}

func (kva KeyValueArray) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func (kva KeyValueArray) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
}

func main() {
	// The number of arguments must be atleast 3
	if len(os.Args) < 3 {
		log.Fatalln("Incorrect usage, correct usage is: mrsequential.go plugin.so {inputfile}")
	}

	mapf, redf := loadPlugin(os.Args[1])

	intermediate := []mr.KeyValue{}

	// Read the contents of the file.
	for _, fname := range os.Args[2:] {
		file, err := os.Open(fname)
		if err != nil {
			fmt.Println("Unable to open file", fname, "Skipping..")
			continue
		}

		// Read into contents
		contents, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Println("Unable to read contents of", fname, "Skipping..")
			continue
		}
		file.Close()

		kva := mapf(fname, string(contents))
		intermediate = append(intermediate, kva...)
	}

	// Check intermediate output

	/*
		for _, kv := range intermediate {
			fmt.Println(kv.Key, kv.Value)
		}
	*/

	// Sort the Intermediate Output, to allow Reduce to group together easily.
	sort.Sort(KeyValueArray(intermediate))

	// Iterate over the Sorted intermediate output,
	// collecting the sub-slices and passing them to reduce.

	outFile, _ := os.Create("mr-out-0")

	for i, j := 0, 0; i < len(intermediate); {
		keySlice := []string{}

		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			keySlice = append(keySlice, intermediate[j].Key)
			j += 1x
		}

		count := redf(intermediate[i].Key, keySlice)

		fmt.Fprintf(outFile, "%s %s\n", intermediate[i].Key, count)

		i = j
	}
}

func loadPlugin(pluginName string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	plug, err := plugin.Open(pluginName)
	if err != nil {
		log.Fatalln("Unable to open plugin", pluginName)
	}

	vmapf, err := plug.Lookup("Map")
	if err != nil {
		log.Fatalln("Unable to find Map in", pluginName)
	}

	vredf, err := plug.Lookup("Reduce")
	if err != nil {
		log.Fatalln("Unable to find Map in", pluginName)
	}

	// Typecast
	mapf := vmapf.(func(string, string) []mr.KeyValue)
	redf := vredf.(func(string, []string) string)

	return mapf, redf
}
