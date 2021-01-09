package main

import (
	"strconv"
	"strings"
	"unicode"

	"../mr"
)

// Map takes the contents of the given input file.
// It returns a slice of Key Value pairs of the form
// {"Word", "1"}
func Map(filename, contents string) []mr.KeyValue {
	// Need to split the contents into words.
	ff := func(r rune) bool {
		return !unicode.IsLetter(r)
	}

	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, word := range words {
		kva = append(kva, mr.KeyValue{Key: word, Value: "1"})
	}

	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
