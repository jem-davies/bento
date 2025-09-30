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
	gsiServiceName = "service_name"
)

func grpcServerInputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Network").
		Summary("TODO").
		Description("TODO").
		Fields(
			service.NewStringField(gsiServiceName).
				Description("TODO").
				Default("echo.Echo"),
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
	serviceDesc  grpc.ServiceDesc
	structpbChan chan (*structpb.Struct)
}

func newGrpcServerInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*grpcServerInput, error) {
	serviceName, err := conf.FieldString(gsiServiceName)
	if err != nil {
		return nil, err
	}

	structpbChan := make(chan (*structpb.Struct))

	bentoService := grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Stream",
			Handler:       create_handler(structpbChan),
			ServerStreams: true,
			ClientStreams: false,
		}},
	}

	gsi := grpcServerInput{
		serviceDesc:  bentoService,
		structpbChan: structpbChan,
	}

	return &gsi, nil
}

func (gsi *grpcServerInput) Connect(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return err
	}
	s := grpc.NewServer()

	s.RegisterService(&gsi.serviceDesc, nil)

	go s.Serve(lis)

	return nil
}

func (gsi *grpcServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	structpb := <-gsi.structpbChan

	msgBytes, err := structpb.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}

	return []*service.Message{service.NewMessage(msgBytes)}, func(context.Context, error) error { return nil }, nil
}

func (gsi *grpcServerInput) Close(ctx context.Context) error {
	return nil
}
