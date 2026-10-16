package httpclient

import (
	"context"
	"errors"
	"io"

	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	config.SetRemoteConfigFetcher(fetchRemoteConfig)
}

func remoteConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(ConfigField("GET", false))
}

// fetchRemoteConfig parses specBytes as an HTTP client config, performs a
// single request and returns the response body.
func fetchRemoteConfig(ctx context.Context, specBytes []byte) ([]byte, error) {
	pConf, err := remoteConfigSpec().ParseYAML(string(specBytes), nil)
	if err != nil {
		return nil, err
	}

	oldConf, err := ConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	if oldConf.RateLimit != "" {
		return nil, errors.New("rate_limit is not supported when fetching a remote config")
	}

	client, err := NewClientFromOldConfig(oldConf, service.MockResources())
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	res, err := client.SendToResponse(ctx, service.MessageBatch{service.NewMessage(nil)})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}
