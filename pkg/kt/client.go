package kt

import (
	"github.com/Shopify/sarama"
)

type Client struct {
	SaramaClient sarama.Client
}

func (c *Client) NewOffsetManager(group string) (sarama.OffsetManager, error) {
	if group == "" {
		return nil, nil
	}

	return sarama.NewOffsetManagerFromClient(group, c.SaramaClient)
}

func (c ConsumerConfig) SetupClient() (*Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = c.Version
	cfg.ClientID = "kt-consume-" + CurrentUserName()

	if err := c.Auth.SetupAuth(cfg); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(c.Brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &Client{SaramaClient: client}, err
}
