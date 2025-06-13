package aws

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/warpstreamlabs/bento/internal/impl/kafka"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/twmb/franz-go/pkg/sasl"
	kaws "github.com/twmb/franz-go/pkg/sasl/aws"

	sess "github.com/warpstreamlabs/bento/internal/impl/aws"
)

func init() {
	kafka.AWSSASLFromConfigFn = func(c *service.ParsedConfig) (sasl.Mechanism, error) {
		awsConf, err := sess.GetSession(context.TODO(), c.Namespace("aws"))
		if err != nil {
			return nil, err
		}

		creds := awsConf.Credentials
		return kaws.ManagedStreamingIAM(func(ctx context.Context) (kaws.Auth, error) {
			val, err := creds.Retrieve(ctx)
			if err != nil {
				return kaws.Auth{}, err
			}
			return kaws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
			}, nil
		}), nil
	}

	kafka.AWSSASLFromConfigFnSarama = func(awsConf *service.ParsedConfig) (sarama.AccessTokenProvider, sarama.SASLMechanism, error) {

		session, err := sess.GetSession(context.Background(), awsConf)
		if err != nil {
			return nil, "", err
		}

		tp := &kafka.MskAccessTokenProvider{
			Region:      session.Region,
			Credentials: session.Credentials,
			Signer:      kafka.AwsMskIamSaslSigner,
		}
		m := sarama.SASLMechanism(sarama.SASLTypeOAuth)
		return tp, m, err
	}
}
