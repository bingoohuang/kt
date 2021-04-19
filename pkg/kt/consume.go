package kt

import (
	"encoding/json"
	"fmt"
	"github.com/bingoohuang/gg/pkg/gz"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"golang.org/x/crypto/ssh/terminal"
)

type Consumer struct {
	sync.Mutex

	Client *Client

	SaramaConsumer sarama.Consumer
	OffsetManager  sarama.OffsetManager
	Poms           map[int32]sarama.PartitionOffsetManager
	Offsets        map[int32]OffsetInterval

	out chan ConsumedContext

	MessageConsumer MessageConsumer
	Topic           string
	Group           string
	Timeout         time.Duration
}

type ConsumerConfig struct {
	Brokers []string
	Version sarama.KafkaVersion
	Auth    AuthConfig
	Group   string
	Topic   string
	Offsets string
	Timeout time.Duration

	MessageConsumer MessageConsumer
}

func StartConsume(conf ConsumerConfig) (c *Consumer, err error) {
	c = &Consumer{
		MessageConsumer: conf.MessageConsumer,
		Topic:           conf.Topic,
		Group:           conf.Group,
		Timeout:         conf.Timeout,
	}
	c.Client, err = conf.SetupClient()
	if err != nil {
		return nil, err
	}

	if err = c.setupOffsetManager(); err != nil {
		return nil, err
	}

	if c.SaramaConsumer, err = sarama.NewConsumerFromClient(c.Client.SaramaClient); err != nil {
		return nil, err
	}

	defer LogClose("consumer", c.SaramaConsumer)

	if c.Offsets, err = ParseOffsets(conf.Offsets); err != nil {
		return nil, err
	}

	var partitions []int32
	partitions, err = c.findPartitions()
	if err != nil {
		return nil, err
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions to consume")
	}
	defer c.Close()

	c.consume(partitions)
	return c, nil
}

func (c *Consumer) setupOffsetManager() (err error) {
	if c.Group == "" {
		return nil
	}

	c.OffsetManager, err = c.Client.NewOffsetManager(c.Group)
	return err
}

func (c *Consumer) consume(partitions []int32) {
	c.out = make(chan ConsumedContext)

	go c.consumeMsg()
	c.consumePartitions(partitions)
}

func (c *Consumer) consumeMsg() {
	for {
		ctx := <-c.out
		if c.MessageConsumer != nil {
			c.MessageConsumer.Consume(ctx.Message)
		}
		close(ctx.Done)
	}
}

func (c *Consumer) consumePartitions(partitions []int32) {
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(p int32) {
			defer wg.Done()
			if err := c.consumePartition(p); err != nil {
				log.Printf("E! consume partition %d error %v", p, err)
			}
		}(p)
	}
	wg.Wait()
}

func (c *Consumer) consumePartition(partition int32) error {
	offsets, ok := c.Offsets[partition]
	if !ok {
		offsets = c.Offsets[-1]
	}

	var err error
	var start, end int64
	if start, err = c.resolveOffset(offsets.Start, partition); err != nil {
		log.Printf("Failed to read Start Offset for partition %v err=%v\n", partition, err)
		return nil
	}

	if end, err = c.resolveOffset(offsets.End, partition); err != nil {
		log.Printf("Failed to read End Offset for partition %v err=%v\n", partition, err)
		return nil
	}

	var pc sarama.PartitionConsumer
	if pc, err = c.SaramaConsumer.ConsumePartition(c.Topic, partition, start); err != nil {
		log.Printf("Failed to consume partition %v err=%v\n", partition, err)
		return nil
	}

	return c.partitionLoop(pc, partition, end)
}

func (c *Consumer) resolveOffset(o Offset, partition int32) (int64, error) {
	if !o.Relative {
		return o.Start, nil
	}

	var res int64
	var err error

	if o.Start == sarama.OffsetNewest || o.Start == sarama.OffsetOldest {
		if res, err = c.Client.SaramaClient.GetOffset(c.Topic, partition, o.Start); err != nil {
			return 0, err
		}

		if o.Start == sarama.OffsetNewest {
			res = res - 1
		}

		return res + o.Diff, nil
	} else if o.Start == OffsetResume {
		if c.Group == "" {
			return 0, fmt.Errorf("cannot resume without -group argument")
		}
		pom, _ := c.getPOM(partition)
		next, _ := pom.NextOffset()
		return next, nil
	}

	return o.Start + o.Diff, nil
}

func (c *Consumer) Close() {
	c.Lock()
	defer c.Unlock()

	for p, pom := range c.Poms {
		LogClose(fmt.Sprintf("partition Offset manager for partition %v", p), pom)
	}
}

func (c *Consumer) getPOM(p int32) (sarama.PartitionOffsetManager, error) {
	c.Lock()
	defer c.Unlock()

	if c.Poms == nil {
		c.Poms = map[int32]sarama.PartitionOffsetManager{}
	}

	pom, ok := c.Poms[p]
	if ok {
		return pom, nil
	}

	pom, err := c.OffsetManager.ManagePartition(c.Topic, p)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition Offset manager err:%q", err)
	}
	c.Poms[p] = pom

	return pom, nil
}

func (c *Consumer) partitionLoop(pc sarama.PartitionConsumer, p int32, end int64) (err error) {
	defer LogClose(fmt.Sprintf("partition consumer %v", p), pc)

	var timer *time.Timer
	timeout := make(<-chan time.Time)

	var pom sarama.PartitionOffsetManager
	if c.Group != "" {
		if pom, err = c.getPOM(p); err != nil {
			return err
		}
	}

	for {
		if c.Timeout > 0 {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(c.Timeout)
			timeout = timer.C
		}

		select {
		case <-timeout:
			log.Printf("consuming from partition %v timed out after %s\n", p, c.Timeout)
			return
		case err = <-pc.Errors():
			log.Printf("partition %v consumer encountered err %s", p, err)
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				log.Printf("unexpected closed messages chan")
				return
			}

			ctx := ConsumedContext{Message: msg, Done: make(chan struct{})}
			c.out <- ctx
			<-ctx.Done

			if pom != nil {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return
			}
		}
	}
}

func (c *Consumer) findPartitions() ([]int32, error) {
	all, err := c.SaramaConsumer.Partitions(c.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions for topic %v err %q", c.Topic, err)
	}

	if _, hasDefault := c.Offsets[-1]; hasDefault {
		return all, nil
	}

	var res []int32
	for _, p := range all {
		if _, ok := c.Offsets[p]; ok {
			res = append(res, p)
		}
	}

	return res, nil
}

type MessageConsumer interface {
	Consume(*sarama.ConsumerMessage)
}

type ConsumedContext struct {
	Message *sarama.ConsumerMessage
	Done    chan struct{}
}

type ConsumedMessage struct {
	Partition int32      `json:"partition"`
	Offset    int64      `json:"Offset"`
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

type PrintMessageConsumer struct {
	Marshal                func(v interface{}) ([]byte, error)
	ValEncoder, KeyEncoder BytesEncoder
}

func NewPrintMessageConsumer(pretty bool, keyEncoder, valEncoder BytesEncoder) *PrintMessageConsumer {
	marshal := json.Marshal

	if pretty && terminal.IsTerminal(syscall.Stdout) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}

	return &PrintMessageConsumer{
		Marshal:    marshal,
		KeyEncoder: keyEncoder,
		ValEncoder: valEncoder,
	}
}

func (p PrintMessageConsumer) Consume(m *sarama.ConsumerMessage) {
	msg := newConsumedMessage(m, p.KeyEncoder, p.ValEncoder)
	buf, err := p.Marshal(msg)
	if err != nil {
		log.Printf("failed to marshal Output %#v, err=%v", msg, err)
	}

	fmt.Printf("topic:%s offset:%d partition:%d key:%s timestamp:%s msg:%s\n",
		m.Topic, m.Offset, m.Partition, m.Key,
		m.Timestamp.Format("2006-01-02 15:04:05.000"),
		string(buf),
	)
}

type consumedMessage struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Value     json.RawMessage   `json:"value,omitempty"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func newConsumedMessage(m *sarama.ConsumerMessage, keyEnc, valEnc BytesEncoder) consumedMessage {
	result := consumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       encodeBytes(m.Key, keyEnc),
		Value:     []byte(encodeBytes(m.Value, valEnc)),
	}

	if !m.Timestamp.IsZero() {
		result.Timestamp = &m.Timestamp
	}

	result.Headers = make(map[string]string)
	for _, h := range m.Headers {
		result.Headers[string(h.Key)] = string(h.Value)
	}
	const Gzipped = "gzipped"
	if result.Headers[Gzipped] == "true" {
		if v, err := gz.Ungzip(m.Value); err == nil {
			result.Value = []byte(encodeBytes(v, valEnc))
		}
	}

	return result
}

func encodeBytes(data []byte, encoder BytesEncoder) string {
	if data == nil {
		return ""
	}

	return encoder.Encode(data)
}
