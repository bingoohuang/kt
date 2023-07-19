package kt

import (
	"github.com/IBM/sarama"
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
	sc := sarama.NewConfig()
	sc.Version = c.Version
	sc.ClientID = "kt-consume-" + CurrentUserName()

	if err := c.Auth.SetupAuth(sc); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(c.Brokers, sc)
	if err != nil {
		return nil, err
	}

	return &Client{SaramaClient: client}, err
}
