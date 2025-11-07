package grpc_client_jem

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	grpcClientOutputAddress                = "address"
	grpcClientOutputService                = "service"
	grpcClientOutputMethod                 = "method"
	grpcClientOutputRPCType                = "rpc_type"
	grpcClientOutputReflection             = "reflection"
	grpcClientOutputProtoFiles             = "proto_files"
	grpcClientOutputBatching               = "batching"
	grpcClientOutputPropRes                = "propagate_response"
	grpcClientOutputTls                    = "tls"
	grpcClientOutputHealthCheck            = "health_check"
	grpcClientOutputHealthCheckToggle      = "enabled"
	grpcClientOutputHealthCheckServiceName = "service_name"
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
			service.NewStringEnumField(
				grpcClientOutputRPCType,
				[]string{"unary", "client_stream", "server_stream", "bidi"}...,
			).
				Description("TODO").
				Default("unary"),
			service.NewBoolField(grpcClientOutputReflection).
				Description("TODO").
				Default(false),
			service.NewStringListField(grpcClientOutputProtoFiles).
				Description("TODO").
				Default([]string{}),
			service.NewBoolField(grpcClientOutputPropRes).
				Description("TODO").
				Default(false).
				Advanced(),
			service.NewObjectField(grpcClientOutputHealthCheck,
				service.NewBoolField(grpcClientOutputHealthCheckToggle).
					Description("TODO").
					Default(false).
					Advanced(),
				service.NewStringField(grpcClientOutputHealthCheckServiceName).
					Description("TODO").
					Default("").
					Advanced(),
			),
			service.NewTLSToggledField(grpcClientOutputTls),
			oAuth2FieldSpec(),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(grpcClientOutputBatching),
		)
}

// TODO - dedupe from ./internal/httpclient ?
const (
	aFieldOAuth2           = "oauth2"
	ao2FieldEnabled        = "enabled"
	ao2FieldClientKey      = "client_key"
	ao2FieldClientSecret   = "client_secret"
	ao2FieldTokenURL       = "token_url"
	ao2FieldScopes         = "scopes"
	ao2FieldEndpointParams = "endpoint_params"
)

func oAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField(aFieldOAuth2,
		service.NewBoolField(ao2FieldEnabled).
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),
		service.NewStringField(ao2FieldClientKey).
			Description("A value used to identify the client to the token provider.").
			Default(""),
		service.NewStringField(ao2FieldClientSecret).
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),
		service.NewURLField(ao2FieldTokenURL).
			Description("The URL of the token provider.").
			Default(""),
		service.NewStringListField(ao2FieldScopes).
			Description("A list of optional requested permissions.").
			Default([]any{}).
			Advanced(),
		service.NewAnyMapField(ao2FieldEndpointParams).
			Description("A list of optional endpoint parameters, values should be arrays of strings.").
			Advanced().
			Example(map[string]any{
				"foo": []string{"meow", "quack"},
				"bar": []string{"woof"},
			}).
			Default(map[string]any{}).
			Optional(),
	).
		Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
		Optional().Advanced()
}

type oauth2Config struct {
	enabled        bool
	clientKey      string
	clientSecret   string
	tokenURL       string
	scopes         []string
	endpointParams map[string][]string
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
	address                string
	serviceName            string
	methodName             string
	rpcType                string
	reflection             bool
	protoFiles             []string
	propResponse           bool
	tls                    *tls.Config
	oauth                  oauth2Config
	healthCheckEnabled     bool
	healthCheckServiceName string

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
	rpcType, err := conf.FieldString(grpcClientOutputRPCType)
	if err != nil {
		return nil, err
	}
	reflection, err := conf.FieldBool(grpcClientOutputReflection)
	if err != nil {
		return nil, err
	}
	protoFiles, err := conf.FieldStringList(grpcClientOutputProtoFiles)
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

	healthCheckConf := conf.Namespace(grpcClientOutputHealthCheck)
	healthCheckEnabled, err := healthCheckConf.FieldBool(grpcClientOutputHealthCheckToggle)
	if err != nil {
		return nil, err
	}
	healthCheckServiceName, err := healthCheckConf.FieldString(grpcClientOutputHealthCheckServiceName)
	if err != nil {
		return nil, err
	}

	oauthConf := conf.Namespace(aFieldOAuth2)

	enabled, err := oauthConf.FieldBool(ao2FieldEnabled)
	if err != nil {
		return nil, err
	}
	clientKey, err := oauthConf.FieldString(ao2FieldClientKey)
	if err != nil {
		return nil, err
	}
	clientSecret, err := oauthConf.FieldString(ao2FieldClientSecret)
	if err != nil {
		return nil, err
	}
	tokenURL, err := oauthConf.FieldString(ao2FieldTokenURL)
	if err != nil {
		return nil, err
	}
	scopes, err := oauthConf.FieldStringList(ao2FieldScopes)
	if err != nil {
		return nil, err
	}
	ep, err := oauthConf.FieldAnyMap(ao2FieldEndpointParams)
	if err != nil {
		return nil, err
	}

	endpointParams := map[string][]string{}
	for k, v := range ep {
		if endpointParams[k], err = v.FieldStringList(); err != nil {
			return nil, err
		}
	}

	oauth := oauth2Config{
		enabled:        enabled,
		clientKey:      clientKey,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		scopes:         scopes,
		endpointParams: endpointParams,
	}

	writer := &grpcClientWriter{
		address:      address,
		serviceName:  serviceName,
		methodName:   methodName,
		rpcType:      rpcType,
		reflection:   reflection,
		protoFiles:   protoFiles,
		propResponse: propResponse,
		tls:          tls,
		oauth:        oauth,

		healthCheckEnabled:     healthCheckEnabled,
		healthCheckServiceName: healthCheckServiceName,
	}

	return writer, nil
}

//------------------------------------------------------------------------------

func (gcw *grpcClientWriter) Connect(ctx context.Context) (err error) {
	if gcw.conn != nil && gcw.method != nil {
		return nil
	}

	dialOpts := []grpc.DialOption{}

	if gcw.tls != nil {
		creds := credentials.NewTLS(gcw.tls)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if gcw.oauth.enabled {
		oauth2Conf := &clientcredentials.Config{
			ClientID:       gcw.oauth.clientKey,
			ClientSecret:   gcw.oauth.clientSecret,
			TokenURL:       gcw.oauth.tokenURL,
			Scopes:         gcw.oauth.scopes,
			EndpointParams: gcw.oauth.endpointParams,
		}

		tokenSource := oauth2Conf.TokenSource(ctx)

		_, err := tokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to fetch OAuth2 token: %w", err)
		}

		perRPC := oauth.TokenSource{TokenSource: tokenSource}
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(perRPC))
	}

	if gcw.healthCheckEnabled {
		serviceConf := fmt.Sprintf(`{"healthCheckConfig": {"serviceName": "%v"}}`, gcw.healthCheckServiceName)
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(serviceConf))
	}

	gcw.conn, err = grpc.NewClient(gcw.address, dialOpts...)
	if err != nil {
		return err
	}

	if gcw.healthCheckEnabled {
		healthClient := grpc_health_v1.NewHealthClient(gcw.conn)
		resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{
			Service: gcw.healthCheckServiceName,
		})
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("service %q not healthy: %v", gcw.healthCheckServiceName, resp.GetStatus())
		}
		fmt.Println("Health check OK for service:", gcw.healthCheckServiceName) // TODO replace with a TRCE / DBUG LOG
	}

	if gcw.reflection {
		reflectClient := grpcreflect.NewClientAuto(ctx, gcw.conn)
		// defer reflectClient.Reset() // TODO -> move to the gcw.Close()

		serviceDescriptor, err := reflectClient.ResolveService(gcw.serviceName)
		if err != nil {
			return err
		}

		if method := serviceDescriptor.FindMethodByName(gcw.methodName); method != nil {
			gcw.method = method
		} else {
			return fmt.Errorf("method: %v not found", gcw.methodName)
		}
	}

	if len(gcw.protoFiles) != 0 {
		var parser protoparse.Parser

		fileDescriptors, err := parser.ParseFiles(gcw.protoFiles...)
		if err != nil {
			return err
		}

		// TODO - check this
	Found:
		for _, fileDescriptor := range fileDescriptors {
			for _, service := range fileDescriptor.GetServices() {
				if service.GetFullyQualifiedName() == gcw.serviceName || service.GetName() == gcw.serviceName {
					if method := service.FindMethodByName(gcw.methodName); method != nil {
						gcw.method = method
						break Found
					}
				}
			}
		}
	}

	if gcw.method == nil {
		return fmt.Errorf("unable to find method: %s in provided proto files", gcw.methodName)
	}

	gcw.stub = grpcdynamic.NewStub(gcw.conn)

	return nil
}

func (gcw *grpcClientWriter) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	if gcw.conn == nil || gcw.method == nil {
		return service.ErrNotConnected
	}

	if gcw.rpcType == "client_stream" {
		err := gcw.clientStreamHandler(ctx, msgBatch)
		if err != nil {
			return err
		}
		return nil
	}

	if gcw.rpcType == "server_stream" {
		err := gcw.serverStreamHandler(ctx, msgBatch)
		if err != nil {
			return err
		}
		return nil
	}

	err := gcw.unaryHandler(ctx, msgBatch)
	if err != nil {
		return err
	}
	return nil
}

func (gcw *grpcClientWriter) Close(ctx context.Context) (err error) {
	return gcw.conn.Close()
}

//------------------------------------------------------------------------------

func (gcw *grpcClientWriter) unaryHandler(ctx context.Context, msgBatch service.MessageBatch) error {

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

func (gcw *grpcClientWriter) clientStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {

	clientStream, err := gcw.stub.InvokeRpcClientStream(ctx, gcw.method)
	if err != nil {
		return err
	}

	for _, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			return err
		}

		err = clientStream.SendMsg(request)
		if err != nil {
			return err
		}
	}
	if gcw.propResponse {
		resProtoMessage, err := clientStream.CloseAndReceive()
		if err != nil {
			return err
		}

		if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
			jsonBytes, err := dynMsg.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal proto response to JSON: %w", err)
			}

			responseBatch := msgBatch.Copy()
			responseBatch[0].SetBytes(jsonBytes)

			if err := responseBatch.AddSyncResponse(); err != nil {
				return err
			}
		}
	} // TODO: fallback for unexpected undynamic messages...
	return nil
}

func (gcw *grpcClientWriter) serverStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {

	for _, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			return err
		}

		serverStream, err := gcw.stub.InvokeRpcServerStream(ctx, gcw.method, request)
		if err != nil {
			return err
		}

		i := 0
		responseBatch := msgBatch.Copy()

		for {
			resProtoMessage, err := serverStream.RecvMsg()
			if err == io.EOF {
				if gcw.propResponse {
					if err := responseBatch.AddSyncResponse(); err != nil {
						return err
					}
				}
				break
			}

			if gcw.propResponse {
				if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
					jsonBytes, err := dynMsg.MarshalJSON()
					if err != nil {
						return fmt.Errorf("failed to marshal proto response to JSON: %w", err)
					}

					if len(responseBatch)-1 < i { // TODO: propResponse logic / msg copying
						responseBatch = append(responseBatch, service.NewMessage(jsonBytes))
					} else {
						responseBatch[i].SetBytes(jsonBytes)
					}

				}
			}
			i++
		}
	}
	return nil
}
