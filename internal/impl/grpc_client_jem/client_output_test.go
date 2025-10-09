package grpc_client_jem

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc_client_jem/grpc_test_server"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//------------------------------------------------------------------------------

type testServer struct {
	test_server.UnimplementedGreeterServer
	mu                  sync.Mutex
	sayHelloInvocations int
}

func (s *testServer) SayHello(_ context.Context, in *test_server.HelloRequest) (*test_server.HelloReply, error) {
	s.mu.Lock()
	s.sayHelloInvocations = s.sayHelloInvocations + 1
	s.mu.Unlock()
	return &test_server.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func startGRPCServer(t *testing.T) *testServer {
	t.Helper()
	lis, err := net.Listen("tcp", ":50051")
	assert.NoError(t, err)

	s := grpc.NewServer()

	reflection.Register(s)

	testServer := &testServer{}

	test_server.RegisterGreeterServer(s, testServer)
	go s.Serve(lis)
	return testServer
}

// ------------------------------------------------------------------------------
func TestNewGrpcClientWriterBasic(t *testing.T) {
	testServer := startGRPCServer(t)

	yamlConf := `
grpc_client_jem:
  address: localhost:50051
  service: helloworld.Greeter
  method: SayHello
`
	conf, err := testutil.OutputFromYAML(yamlConf)
	assert.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)
	assert.NoError(t, err)

	sendChan := make(chan message.Transaction)
	receiveChan := make(chan error)

	s.Consume(sendChan)
	t.Cleanup(s.TriggerCloseNow)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}
