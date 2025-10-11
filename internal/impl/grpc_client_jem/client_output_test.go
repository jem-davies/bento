package grpc_client_jem

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc_client_jem/grpc_test_server"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//------------------------------------------------------------------------------

type testServer struct {
	test_server.UnimplementedGreeterServer

	reflection bool

	mu                  sync.Mutex
	sayHelloInvocations int
	port                int
}

func (s *testServer) SayHello(_ context.Context, in *test_server.HelloRequest) (*test_server.HelloReply, error) {
	s.mu.Lock()
	s.sayHelloInvocations = s.sayHelloInvocations + 1
	s.mu.Unlock()
	return &test_server.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func startGRPCServer(t *testing.T, opts ...testServerOpt) *testServer {
	t.Helper()

	testServer := &testServer{}

	for _, o := range opts {
		o(testServer)
	}

	lis, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)

	testServer.port = lis.Addr().(*net.TCPAddr).Port

	s := grpc.NewServer()

	if testServer.reflection {
		reflection.Register(s)
	}

	test_server.RegisterGreeterServer(s, testServer)
	go s.Serve(lis)
	return testServer
}

//------------------------------------------------------------------------------

type testServerOpt func(*testServer)

func withReflection() testServerOpt {
	return func(ts *testServer) {
		ts.reflection = true
	}
}

//------------------------------------------------------------------------------

func TestGrpcClientWriterBasicReflection(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

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

func TestGrpcClientWriterSyncResponseReflection(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}
	names := []string{"Alice", "Bob", "Carol", "Dan"}

	for i, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		resultStore := transaction.NewResultStore()
		transaction.AddResultStore(testMsg, resultStore)

		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			resMsg := resMsgs[0]
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

//------------------------------------------------------------------------------

func startGrpcClientOutput(t *testing.T, yamlConf string) (
	sendChan chan message.Transaction,
	receiveChan chan error,
	err error,
) {
	t.Helper()

	conf, err := testutil.OutputFromYAML(yamlConf)
	if err != nil {
		return
	}

	s, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		return
	}

	sendChan = make(chan message.Transaction)
	receiveChan = make(chan error)

	s.Consume(sendChan)
	t.Cleanup(s.TriggerCloseNow)

	return sendChan, receiveChan, nil
}
