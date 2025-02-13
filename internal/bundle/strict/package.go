package strict

import "slices"

var incompatibleProcessors []string
var incompatibleOutputs []string

func init() {
	incompatibleProcessors = []string{
		"try",
		"catch",
		"switch",
		"retry",
	}

	incompatibleOutputs = []string{
		"reject_errored",
		"switch",
	}
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)
}

func isOutputIncompatible(name string) bool {
	return slices.Contains(incompatibleOutputs, name)
}
