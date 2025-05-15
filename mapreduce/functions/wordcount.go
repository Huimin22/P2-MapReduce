package main

import (
	// "encoding/json"
	"strconv"
	"strings"
	"unicode"
	// "fmt"
	// Import the shared types package
	"dfs/mapreduce/types" // <--- Adjust import path based on your module root
)

// KeyValue represents a key-value pair emitted by Map and processed by Reduce.
// IMPORTANT: For plugin loading to work correctly with type assertions,
// this type definition MUST exactly match the one used in the calling program (storage node),
// OR ideally, both should import this type from a shared package.

// WordCountMap implements the map function for word count.
// It must be EXPORTED (start with capital letter).
func WordCountMap(filename string, contents string) []types.KeyValue {
	// Function to split text into words, removing punctuation and converting to lower case.
	ff := func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) }

	// Split contents into words
	words := strings.FieldsFunc(contents, ff)

	kva := make([]types.KeyValue, 0, len(words))
	for _, w := range words {
		if w != "" { // Ensure not empty after split
			// Normalize: convert to lower case
			normalizedWord := strings.ToLower(w)
			kv := types.KeyValue{Key: normalizedWord, Value: "1"}
			kva = append(kva, kv)
		}
	}
	return kva
}

// WordCountReduce implements the reduce function for word count.
// It must be EXPORTED.
func WordCountReduce(key string, values []string) string {
	// All values should be "1"s from the map phase.
	count := len(values)
	return strconv.Itoa(count)
}

// NO init() function needed for plugin compilation.
// The functions WordCountMap and WordCountReduce will be looked up by name.

// main function is required for buildmode=plugin, but can be empty.
func main() {}

/*
// Helper functions (Encode/Decode) are likely handled by the main program now.
// If kept here, they also need to be Exported if the main program needs to look them up.

// Helper for encoding KV pairs (e.g., to JSON Lines for intermediate files)
func EncodeKeyValue(kv KeyValue) ([]byte, error) {
	return json.Marshal(kv)
}

// Helper for decoding KV pairs
func DecodeKeyValue(data []byte) (KeyValue, error) {
	var kv KeyValue
	err := json.Unmarshal(data, &kv)
	return kv, err
}
*/
