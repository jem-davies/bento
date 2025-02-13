package strict

import "slices"

var incompatibleProcessors []string
var incompatibleOutputs []string
var parentProcessors []string

func init() {
	incompatibleProcessors = []string{
		"try",
		"catch",
		"switch",
	}

	incompatibleOutputs = []string{
		"reject_errored",
		"switch",
	}

	parentProcessors = []string{
		"branch",
		"cached",
		"catch",
		"for_each",
		"group_by",
		"parallel",
		"retry",
		"switch",
		"try",
		"while",
		"workflow",
	}
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)
}

func isOutputIncompatible(name string) bool {
	return slices.Contains(incompatibleOutputs, name)
}

func isParentProcessor(name string) bool {
	return slices.Contains(parentProcessors, name)
}
