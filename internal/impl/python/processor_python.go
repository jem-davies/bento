package python

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func injectScript(filename, script string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read %s: %w", filename, err)
	}
	text := string(data)

	startMarker := "### <script>"
	endMarker := "### </script>"

	start := strings.Index(text, startMarker)
	end := strings.Index(text, endMarker)
	if start == -1 || end == -1 || end <= start {
		return fmt.Errorf("could not find script markers in %s", filename)
	}

	lastNL := strings.LastIndex(text[:start], "\n")
	lineStart := 0
	if lastNL != -1 {
		lineStart = lastNL + 1
	}
	indent := text[lineStart:start]

	codeIndent := indent + "    "

	lines := strings.Split(script, "\n")
	var b strings.Builder
	for i, ln := range lines {
		b.WriteString(codeIndent)
		b.WriteString(ln)
		if i < len(lines)-1 {
			b.WriteString("\n")
		}
	}

	newText := text[:start+len(startMarker)] + "\n" + b.String() + "\n" + text[end:]
	if err := os.WriteFile(filename, []byte(newText), 0644); err != nil {
		return fmt.Errorf("write %s: %w", filename, err)
	}
	return nil
}
func pythonProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().Fields(service.NewStringListField("dependencies"), service.NewStringField("script"))
}

func init() {
	err := service.RegisterProcessor(
		"python", pythonProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newPythonProcessorFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

type pythonProcessor struct{}

func newPythonProcessorFromConfig(conf *service.ParsedConfig) (*pythonProcessor, error) {
	// create the python project and start the gRPC server
	fmt.Println("CREATING PYTHON PROJECT")
	if err := os.Mkdir("./tmp/mat_multi", 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	name := "uv"
	args := []string{"init", "--directory", "./tmp/mat_multi", "--python", "/opt/homebrew/bin/python3", "--name", "mat_multi", "--vcs", "none", "--no-description", "--author-from", "none", "--no-readme"}
	cmd := exec.Command(name, args...)
	cmd.Run()

	// add numpy from bento yaml config:
	deps, err := conf.FieldStringList("dependencies")
	if err != nil {
		return nil, err
	}

	for _, dep := range deps {
		cmd := exec.Command("uv", []string{"--directory", "./tmp/mat_multi", "add", dep}...)
		cmd.Run()
	}

	// add all the other deps :
	grpcDeps := []string{"grpcio-tools", "grpcio-reflection"}

	for _, dep := range grpcDeps {
		cmd := exec.Command("uv", []string{"--directory", "./tmp/mat_multi", "add", dep}...)
		cmd.Run()
	}

	// inject user config script field into main.py
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	err = injectScript("main.py", script)
	if err != nil {
		return nil, err
	}

	// overwrite main.py
	cmd = exec.Command("mv", "-f", "main.py", "./tmp/mat_multi/")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command("mv", "-f", "bento_python_pb2_grpc.py", "./tmp/mat_multi/")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command("mv", "-f", "bento_python_pb2.py", "./tmp/mat_multi/")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command("mv", "-f", "bento_python_pb2.pyi", "./tmp/mat_multi/")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	// start the server:
	fmt.Println("STARTING SERVER")

	name = "uv"
	args = []string{"run", "--directory", "./tmp/mat_multi", "main.py"}

	cmd = exec.Command(name, args...)

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	fmt.Println("PYTHON gRPC SERVER STARTED ON 50051")

	return &pythonProcessor{}, nil
}

func (pp *pythonProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	encodedMsg := base64.StdEncoding.EncodeToString(msgBytes)

	messageToSend := `{"data": "` + encodedMsg + `"}`

	dialopts := []grpc.DialOption{}
	dialopts = append(dialopts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient("0.0.0.0:50051", dialopts...)
	if err != nil {
		return nil, err
	}

	reflectClient := grpcreflect.NewClientAuto(ctx, conn)

	serviceDescriptor, err := reflectClient.ResolveService("bento.grpc.BentoPython")
	if err != nil {
		return nil, err
	}

	var foomethod *desc.MethodDescriptor
	if method := serviceDescriptor.FindMethodByName("Process"); method != nil {
		foomethod = method
	} else {
		return nil, fmt.Errorf("method: %v not found", "Process")
	}

	request := dynamic.NewMessage(foomethod.GetInputType())
	if err := request.UnmarshalJSON([]byte(messageToSend)); err != nil {
		return nil, err
	}

	stub := grpcdynamic.NewStub(conn)
	resProtoMessage, err := stub.InvokeRpc(ctx, foomethod, request)
	if err != nil {
		return nil, err
	}
	var retMsg *service.Message
	if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
		jsonBytes, err := dynMsg.MarshalJSON()
		if err != nil {
			return nil, err
		}

		var jsonMap map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &jsonMap); err != nil {
			return nil, err
		}

		if dataStr, ok := jsonMap["data"].(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(dataStr)
			if err == nil {
				var nested interface{}
				if json.Unmarshal(decoded, &nested) == nil {
					jsonMap["data"] = nested
				} else {
					jsonMap["data"] = string(decoded)
				}
			}
		}

		decodedJSON, err := json.Marshal(jsonMap)
		if err != nil {
			return nil, err
		}

		retMsg = service.NewMessage(decodedJSON)
	}

	return service.MessageBatch{
		retMsg,
	}, nil
}

func (pp *pythonProcessor) Close(ctx context.Context) error {
	return nil
}
