package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"

	"../mr"
)

func main() {
	// The number of arguments must be atleast 3
	if len(os.Args) < 3 {
		log.Fatalln("Incorrect usage, correct usage is: mrsequential.go plugin.so {inputfile}")
	}

	mapf, redf := loadPlugin(os.Args[1])

	intermediate := []mr.KeyValue{}

	// Read the contents of the file.
	for _, fname := range os[2:] {
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
		intermediate = append(intermediate, kv)
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
