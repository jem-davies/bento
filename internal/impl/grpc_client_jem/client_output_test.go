package grpc_client_jem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc_client_jem/grpc_test_server"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

//------------------------------------------------------------------------------

type testServer struct {
	test_server.UnimplementedGreeterServer

	reflection  bool
	tls         bool
	healthCheck bool

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

	serverOpts := []grpc.ServerOption{}

	if testServer.tls {
		creds, err := credentials.NewServerTLSFromFile("./grpc_test_server/server_cert.pem", "./grpc_test_server/server_key.pem")
		if err != nil {
			t.Fatal(err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	s := grpc.NewServer(serverOpts...)

	if testServer.healthCheck {
		hc := health.NewServer()
		healthgrpc.RegisterHealthServer(s, hc)
		hc.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	}

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

func withTLS() testServerOpt {
	return func(ts *testServer) {
		ts.tls = true
	}
}

func withHealthCheck() testServerOpt {
	return func(ts *testServer) {
		ts.healthCheck = true
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
  reflection: true
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
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
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
  reflection: true
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
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			resMsg := resMsgs[0]
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterTLS(t *testing.T) {
	testServer := startGRPCServer(t, withReflection(), withTLS())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
  tls:
    enabled: true
    root_cas_file: ./grpc_test_server/client_ca_cert.pem
    client_certs:
      - cert_file: ./grpc_test_server/client_cert.pem
        key_file: ./grpc_test_server/client_key.pem
    skip_cert_verify: true
`, testServer.port) // TODO: have skip_cert_verify: false

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
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

func TestGrpcClientWriterBasicProtoFile(t *testing.T) {
	testServer := startGRPCServer(t)

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  proto_files: 
    - "./grpc_test_server/helloworld.proto"
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
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterHealthCheck(t *testing.T) {
	testServer := startGRPCServer(t, withReflection(), withHealthCheck())

	yamlConf := fmt.Sprintf(`
address: localhost:%v
service: helloworld.Greeter
method: SayHello
reflection: true
health_check:
  enabled: true
  service_name: ""
`, testServer.port)

	pConf, err := grcpClientOutputSpec().ParseYAML(yamlConf, nil)
	require.NoError(t, err)

	foo, err := newGrpcClientWriterFromParsed(pConf, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = foo.Connect(ctx)
	require.NoError(t, err)
}

func TestGrpcClientWriterUnableToFindMethodErr(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	sb := service.NewStreamBuilder()

	sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: root.name = "Alice"
    count: 1

output:
  grpc_client_jem:
    address: localhost:%v
    service: helloworld.Greeter
    method: DoesNotExist
    reflection: true
`, testServer.port))

	originalStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() {
		os.Stdout = originalStdout
	}()

	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = stream.Run(ctx)
	require.Equal(t, err, context.DeadlineExceeded)

	var buf bytes.Buffer
	w.Close()
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	r.Close()

	assert.Contains(t, buf.String(), "method: DoesNotExist not found")
}
