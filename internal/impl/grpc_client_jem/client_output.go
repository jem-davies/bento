package grpc_client_jem

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	grpcClientOutputAddress  = "address"
	grpcClientOutputService  = "service"
	grpcClientOutputMethod   = "method"
	grpcClientOutputBatching = "batching"
	grpcClientOutputPropRes  = "propagate_response"
	grpcClientOutputTls      = "tls"
)

func grcpClientOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sends messages to a GRPC server.").
		Description("TODO").
		Categories("network").
		Fields(
			service.NewStringField(grpcClientOutputAddress).
				Description("TODO").
				Example("localhost:50051"),
			service.NewStringField(grpcClientOutputService).
				Description("TODO").
				Example("helloworld.Greeter"),
			service.NewStringField(grpcClientOutputMethod).
				Description("TODO").
				Example("SayHello"),
			service.NewBoolField(grpcClientOutputPropRes).
				Description("TODO").
				Default(false).
				Advanced(),
			service.NewTLSToggledField(grpcClientOutputTls),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(grpcClientOutputBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("grpc_client_jem", grcpClientOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}

			out, err = newGrpcClientWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type grpcClientWriter struct {
	address      string
	serviceName  string
	methodName   string
	propResponse bool
	tls          *tls.Config

	conn *grpc.ClientConn

	stub   grpcdynamic.Stub
	method *desc.MethodDescriptor
}

func newGrpcClientWriterFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*grpcClientWriter, error) {
	address, err := conf.FieldString(grpcClientOutputAddress)
	if err != nil {
		return nil, err
	}
	serviceName, err := conf.FieldString(grpcClientOutputService)
	if err != nil {
		return nil, err
	}
	methodName, err := conf.FieldString(grpcClientOutputMethod)
	if err != nil {
		return nil, err
	}
	propResponse, err := conf.FieldBool(grpcClientOutputPropRes)
	if err != nil {
		return nil, err
	}
	tls, err := conf.FieldTLS(grpcClientOutputTls)
	if err != nil {
		return nil, err
	}

	writer := &grpcClientWriter{
		address:      address,
		serviceName:  serviceName,
		methodName:   methodName,
		propResponse: propResponse,
		tls:          tls,
	}

	return writer, nil
}

//------------------------------------------------------------------------------

func (gcw *grpcClientWriter) Connect(ctx context.Context) (err error) {
	if gcw.conn != nil {
		return nil
	}

	dialOpts := []grpc.DialOption{}

	if gcw.tls != nil {
		fmt.Println("gcw.tls not nil")
		creds := credentials.NewTLS(gcw.tls)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	gcw.conn, err = grpc.NewClient(gcw.address, dialOpts...)
	if err != nil {
		return err
	}

	reflectClient := grpcreflect.NewClientAuto(ctx, gcw.conn)
	defer reflectClient.Reset() // TODO -> move to the gcw.Close()

	serviceDescriptor, err := reflectClient.ResolveService(gcw.serviceName)
	if err != nil {
		return err
	}

	gcw.method = serviceDescriptor.FindMethodByName(gcw.methodName)
	if gcw.method == nil {
		return fmt.Errorf("method: %v not found", gcw.methodName)
	}

	gcw.stub = grpcdynamic.NewStub(gcw.conn)

	return nil
}

func (gcw *grpcClientWriter) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	if gcw.conn == nil {
		return service.ErrNotConnected
	}

	rc := grpcreflect.NewClientAuto(ctx, gcw.conn)
	defer rc.Reset()

	for _, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			return err
		}

		resProtoMessage, err := gcw.stub.InvokeRpc(ctx, gcw.method, request)
		if err != nil {
			return err
		}
		if gcw.propResponse {
			if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
				jsonBytes, err := dynMsg.MarshalJSON()
				if err != nil {
					return fmt.Errorf("failed to marshal proto response to JSON: %w", err)
				}

				responseMsg := msg.Copy()
				responseMsg.SetBytes(jsonBytes)

				responseBatch := service.MessageBatch{responseMsg}
				if err := responseBatch.AddSyncResponse(); err != nil {
					return err
				}
			}
		} // TODO: fallback for unexpected undynamic messages...
	}
	return nil
}

func (gcw *grpcClientWriter) Close(ctx context.Context) (err error) {
	return gcw.conn.Close()
}
