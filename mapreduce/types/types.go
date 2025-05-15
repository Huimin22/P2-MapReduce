package types

// KeyValue is a type used for intermediate Map output and Reduce input/output.
// It MUST be defined in a shared package imported by both the main program
// and the plugins to avoid type mismatches.
type KeyValue struct {
	Key   string
	Value string
}

// You might also define MapFunc and ReduceFunc types here for clarity,
// though the raw function signatures work too.
// type MapFunc func(filename string, contents string) []KeyValue
// type ReduceFunc func(key string, values []string) string
