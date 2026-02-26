package pure

import (
	"context"
	"unicode"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	uepFormat          = "format"
	uepFieldInRange    = "in_range"
	uepFieldNotInRange = "not_in_range"
)

func unicodeEscapeProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Summary("Replaces Unicode codepoints with escaped reference.").
		Description(`
This processor is intended to be used in cases such as AWS SQS, where it will not 
accept specific unicode codepoints, and that it would be necessary to replace them.
`).Fields(
		service.NewStringEnumField(uepFormat, []string{"json", "xml"}...).
			Description("TODO"),
		service.NewAnyListField(uepFieldNotInRange).
			Description("TODO"),
	)
}

func init() {
	err := service.RegisterBatchProcessor(
		"unicode_escape", unicodeEscapeProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			format, err := conf.FieldString(uepFormat)
			if err != nil {
				return nil, err
			}

			notInRange, err := conf.FieldStringListOfLists(uepFieldNotInRange)
			if err != nil {
				return nil, err
			}

			p, err := newUnicodeEscapeProcessor(format, notInRange)
			if err != nil {
				return nil, err
			}

			return p, nil
		})
	if err != nil {
		panic(err)
	}
}

type ueProc struct {
	format  string
	inRange [][]string

	rangeTables []*unicode.RangeTable
}

func newUnicodeEscapeProcessor(format string, inRange [][]string) (*ueProc, error) {
	// here I want to parse the [][]string
	// into a slice of []*RangeTable where the inner most slice will have length 2
	// the first being the start of a range i.e. "U+0020" and the second being the
	// end: "U+D7FF"

	p := &ueProc{
		format:  format,
		inRange: inRange,

		// rangeTables
	}

	return p, nil
}

func (uep *ueProc) ProcessBatch(context.Context, service.MessageBatch) (msgBatch []service.MessageBatch, err error) {
	// here I want to iterate over the message Batch
	// and for each message, I want to iterate through the unicode codepoints (runes)
	// and for ones not in the uep.RangeTables I want to replace them with the json escaped
	// format i.e. : \uFFFE - and return that message batch back
	for _, msg := range msgBatch {

	}

	return nil, nil
}

func (uep *ueProc) Close(ctx context.Context) error {
	// don't think we will need to do anything here
	return nil
}
