package protobuf

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

// Too many calls to the buf schema registry.

// let's start 100 protobuf processors - each poll interval at 5 minutes by default.
// 100 will mean you will see 20 requests per minute - but if we had only 1 of the processors
// requesting every 5 mins you would see 0.2 requests per minute - that is the basis of the test
// first we must write a test that will fail ...

func TestXxx(t *testing.T) {
	mockBSRServerAddress, s := runMockBSRServer(t)
	message := "testing.Envelope"

	conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
bsr:
  - module: "testing"
    url: %s
`, message, "http://"+mockBSRServerAddress), nil)
	require.NoError(t, err)

	// create 100
	legionOfProcessors := map[int]*protobufProc{}
	for i := range 100 {
		p, err := newProtobuf(conf, service.MockResources())
		require.NoError(t, err)
		legionOfProcessors[i] = p
	}

	time.Sleep(time.Second * 20)

	assert.LessOrEqual(t, s.cc, 5)
}
