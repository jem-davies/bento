package grpcserver

import (
	"context"
	"io"
	"net"

	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	gsiAddress     = "address"
	gsiServiceName = "service_name"
	gsiStreamName  = "stream_name"
)

func grpcServerInputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Network").
		Summary("TODO").
		Description("TODO").
		Fields(
			service.NewStringField(gsiServiceName).
				Description("TODO").
				Default("bento.Bento"),
			service.NewStringField(gsiAddress).
				Description("TODO").
				Default(":50051"),
			service.NewStringField(gsiStreamName).
				Description("TODO").
				Default("Stream"),
		)
}

func init() {
	err := service.RegisterBatchInput(
		"grpc_server", grpcServerInputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.BatchInput, err error) {
			in, err = newGrpcServerInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return in, nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func create_handler(structpbChan chan (*structpb.Struct)) func(_ any, stream grpc.ServerStream) error {
	handler := func(_ any, stream grpc.ServerStream) error {
		in := new(structpb.Struct)
		if err := stream.RecvMsg(in); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		structpbChan <- in

		return nil
	}
	return handler
}

//------------------------------------------------------------------------------

type grpcServerInput struct {
	address      string
	serviceDesc  grpc.ServiceDesc
	structpbChan chan (*structpb.Struct)
	server       *grpc.Server
}

func newGrpcServerInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*grpcServerInput, error) {
	address, err := conf.FieldString(gsiAddress)
	if err != nil {
		return nil, err
	}

	serviceName, err := conf.FieldString(gsiServiceName)
	if err != nil {
		return nil, err
	}

	streamName, err := conf.FieldString(gsiStreamName)
	if err != nil {
		return nil, err
	}

	structpbChan := make(chan (*structpb.Struct))

	bentoService := grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    streamName,
			Handler:       create_handler(structpbChan),
			ServerStreams: true,
			ClientStreams: false,
		}},
	}

	gsi := grpcServerInput{
		address:      address,
		serviceDesc:  bentoService,
		structpbChan: structpbChan,
	}

	return &gsi, nil
}

func (gsi *grpcServerInput) Connect(ctx context.Context) error {
	if gsi.server != nil {
		return nil
	}

	lis, err := net.Listen("tcp", gsi.address)
	if err != nil {
		return err
	}
	gsi.server = grpc.NewServer()

	gsi.server.RegisterService(&gsi.serviceDesc, nil)

	go gsi.server.Serve(lis)

	return nil
}

func (gsi *grpcServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	select {
	case structpb, open := <-gsi.structpbChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}

		msgBytes, err := structpb.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}

		return []*service.Message{service.NewMessage(msgBytes)}, func(context.Context, error) error { return nil }, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (gsi *grpcServerInput) Close(ctx context.Context) error {
	gsi.server.GracefulStop()
	return nil
}
