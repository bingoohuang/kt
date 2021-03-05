package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	. "github.com/bingoohuang/kt/pkg/kt"

	"github.com/Shopify/sarama"
)

type produceArgs struct {
	topic       string
	partition   int
	brokers     string
	auth        string
	batch       int
	timeout     time.Duration
	verbose     bool
	pretty      bool
	version     string
	compress    string
	literal     bool
	decKey      string
	decVal      string
	partitioner string
	bufSize     int
}

type message struct {
	Key       *string `json:"key,omitempty"`
	Value     *string `json:"value,omitempty"`
	K         *string `json:"k,omitempty"`
	V         *string `json:"v,omitempty"`
	Partition *int32  `json:"partition"`
	P         *int32  `json:"p"`
}

func (c *produceCmd) read(as []string) produceArgs {
	var a produceArgs
	f := flag.NewFlagSet("produce", flag.ContinueOnError)
	f.StringVar(&a.topic, "topic", "", "Topic to produce to (required).")
	f.IntVar(&a.partition, "partition", 0, "Partition to produce to (defaults to 0).")
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 if omitted (defaults to localhost:9092).")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", EnvAuth))
	f.IntVar(&a.batch, "batch", 1, "Max size of a batch before sending it off")
	f.DurationVar(&a.timeout, "timeout", 50*time.Millisecond, "Duration to wait for batch to be filled before sending it off")
	f.BoolVar(&a.verbose, "verbose", false, "Verbose Output")
	f.BoolVar(&a.pretty, "pretty", false, "Control Output pretty printing.")
	f.BoolVar(&a.literal, "literal", false, "Interpret stdin line literally and pass it as value, key as null.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.StringVar(&a.compress, "compress", "", "Kafka message compress codec [gzip|snappy|lz4], defaults to none")
	f.StringVar(&a.partitioner, "partitioner", "hash", "Optional partitioner. hash/rand")
	f.StringVar(&a.decKey, "dec.key", "string", "Decode message value as (string|hex|base64), defaults to string.")
	f.StringVar(&a.decVal, "dec.val", "string", "Decode message value as (string|hex|base64), defaults to string.")
	f.IntVar(&a.bufSize, "buf.size", 16777216, "Buffer size for scanning stdin, defaults to 16777216=16*1024*1024.")

	f.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage of produce:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, produceDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return a
}

func (c *produceCmd) parseArgs(as []string) {
	a := c.read(as)
	c.topic = getKtTopic(a.topic)

	if err := c.auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}

	c.brokers = ParseBrokers(a.brokers)

	var err error
	if c.valDecoder, err = ParseStringDecoder(a.decVal); err != nil {
		failStartup(err.Error())
	}
	if c.keyDecoder, err = ParseStringDecoder(a.decKey); err != nil {
		failStartup(err.Error())
	}

	c.batch = a.batch
	c.timeout = a.timeout
	c.verbose = a.verbose
	c.pretty = a.pretty
	c.literal = a.literal
	c.partition = int32(a.partition)
	c.partitioner = a.partitioner
	c.version = kafkaVersion(a.version)
	c.compress = kafkaCompression(a.compress)
	c.bufSize = a.bufSize
}

func kafkaCompression(codecName string) sarama.CompressionCodec {
	switch codecName {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "":
		return sarama.CompressionNone
	}

	failf("unsupported compress codec %#v - supported: gzip, snappy, lz4", codecName)
	panic("unreachable")
}

func (c *produceCmd) findLeaders() {
	var (
		err error
		res *sarama.MetadataResponse
	)

	req := sarama.MetadataRequest{Topics: []string{c.topic}}
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Version = c.version
	cfg.ClientID = "kt-produce-" + CurrentUserName()
	if c.verbose {
		log.Printf("sarama client configuration %#v\n", cfg)
	}

	if err = c.auth.SetupAuth(cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

loop:
	for _, addr := range c.brokers {
		broker := sarama.NewBroker(addr)
		if err = broker.Open(cfg); err != nil {
			log.Printf("Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}
		if connected, err := broker.Connected(); !connected || err != nil {
			log.Printf("Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}

		if res, err = broker.GetMetadata(&req); err != nil {
			log.Printf("Failed to get metadata from %#v. err=%v\n", addr, err)
			continue loop
		}

		brokers := map[int32]*sarama.Broker{}
		for _, b := range res.Brokers {
			brokers[b.ID()] = b
		}

		for _, tm := range res.Topics {
			if tm.Name == c.topic {
				if tm.Err != sarama.ErrNoError {
					log.Printf("Failed to get metadata from %#v. err=%v\n", addr, tm.Err)
					continue loop
				}

				c.leaders = map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						failf("failed to find leader in broker response, giving up")
					}

					if err = b.Open(cfg); err != nil && err != sarama.ErrAlreadyConnected {
						failf("failed to open broker connection err=%s", err)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						failf("failed to wait for broker connection to open err=%s", err)
					}

					c.leaders[pm.ID] = b
				}
				return
			}
		}
	}

	failf("failed to find leader for given topic")
}

type produceCmd struct {
	topic                  string
	brokers                []string
	auth                   AuthConfig
	batch                  int
	timeout                time.Duration
	verbose                bool
	pretty                 bool
	literal                bool
	partition              int32
	version                sarama.KafkaVersion
	compress               sarama.CompressionCodec
	partitioner            string
	keyDecoder, valDecoder StringDecoder
	bufSize                int

	leaders map[int32]*sarama.Broker
	out     chan PrintContext
}

func (c *produceCmd) run(as []string) {
	c.parseArgs(as)
	if c.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	defer c.close()
	c.findLeaders()
	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan message)
	batchedMessages := make(chan []message)
	c.out = make(chan PrintContext)
	q := make(chan struct{})

	go readStdinLines(c.bufSize, stdin)
	go PrintOut(c.out, c.pretty)

	go listenForInterrupt(q)
	go c.readInput(q, stdin, lines)
	go c.deserializeLines(lines, messages, int32(len(c.leaders)))
	go c.batchRecords(messages, batchedMessages)
	c.produce(batchedMessages)
}

func (c *produceCmd) close() {
	for _, b := range c.leaders {
		var (
			connected bool
			err       error
		)

		if connected, err = b.Connected(); err != nil {
			log.Printf("Failed to check if broker is connected. err=%s\n", err)
			continue
		}

		if !connected {
			continue
		}

		if err = b.Close(); err != nil {
			log.Printf("Failed to close broker %v connection. err=%s\n", b, err)
		}
	}
}

func (c *produceCmd) deserializeLines(in chan string, out chan message, partitionCount int32) {
	defer func() { close(out) }()
	for l := range in {
		var msg message

		switch {
		case c.literal:
			msg.Value = &l
			msg.Partition = &c.partition
		default:
			if err := json.Unmarshal([]byte(l), &msg); err != nil {
				if c.verbose {
					log.Printf("Failed to unmarshal input [%v], falling back to defaults. err=%v\n", l, err)
				}
				v := &l
				if len(l) == 0 {
					v = nil
				}
				msg = message{Value: v}
			}
		}

		c.setPartition(&msg, partitionCount)

		out <- msg
	}
}

func (c *produceCmd) setPartition(msg *message, partitionCount int32) {
	if msg.Partition == nil && msg.P != nil {
		msg.Partition = msg.P
	}

	if msg.Partition != nil {
		return
	}

	part := int32(0)
	switch {
	case c.partitioner == "rand":
		part = randPartition(partitionCount)
	case msg.Key != nil && c.partitioner == "hash":
		part = hashCodePartition(*msg.Key, partitionCount)
	}

	msg.Partition = &part
}

func (c *produceCmd) batchRecords(in chan message, out chan []message) {
	defer func() { close(out) }()

	var messages []message
	send := func() {
		out <- messages
		messages = messages[:0]
	}

	for {
		select {
		case m, ok := <-in:
			if !ok {
				send()
				return
			}

			messages = append(messages, m)
			if len(messages) > 0 && len(messages) >= c.batch {
				send()
			}
		case <-time.After(c.timeout):
			if len(messages) > 0 {
				send()
			}
		}
	}
}

type partitionProduceResult struct {
	start int64
	count int64
}

func (c *produceCmd) makeSaramaMessage(msg message) (*sarama.Message, error) {
	var (
		err error
		sm  = &sarama.Message{Codec: c.compress}
	)

	if v := FirstNotNil(msg.Key, msg.K); v != "" {
		if sm.Key, err = c.keyDecoder.Decode(v); err != nil {
			return sm, fmt.Errorf("failed to decode key as string, err=%v", err)
		}
	}

	if v := FirstNotNil(msg.Value, msg.V); v != "" {
		if sm.Value, err = c.valDecoder.Decode(v); err != nil {
			return sm, fmt.Errorf("failed to decode value as string, err=%v", err)
		}
	}

	if c.version.IsAtLeast(sarama.V0_10_0_0) {
		sm.Version = 1
		sm.Timestamp = time.Now()
	}

	return sm, nil
}

func (c *produceCmd) produceBatch(leaders map[int32]*sarama.Broker, batch []message) error {
	requests := map[*sarama.Broker]*sarama.ProduceRequest{}
	for _, msg := range batch {
		broker, ok := leaders[*msg.Partition]
		if !ok {
			return fmt.Errorf("non-configured partition %v", *msg.Partition)
		}
		req, ok := requests[broker]
		if !ok {
			req = &sarama.ProduceRequest{RequiredAcks: sarama.WaitForAll, Timeout: 10000}
			requests[broker] = req
		}

		sm, err := c.makeSaramaMessage(msg)
		if err != nil {
			return err
		}
		req.AddMessage(c.topic, *msg.Partition, sm)
	}

	for broker, req := range requests {
		resp, err := broker.Produce(req)
		if err != nil {
			return fmt.Errorf("failed to send request to broker %#v. err=%s", broker, err)
		}

		offsets, err := readPartitionOffsetResults(resp)
		if err != nil {
			return fmt.Errorf("failed to read producer response err=%s", err)
		}

		for p, o := range offsets {
			result := map[string]interface{}{"partition": p, "startOffset": o.start, "count": o.count}
			ctx := PrintContext{Output: result, Done: make(chan struct{})}
			c.out <- ctx
			<-ctx.Done
		}
	}

	return nil
}

func readPartitionOffsetResults(resp *sarama.ProduceResponse) (map[int32]partitionProduceResult, error) {
	offsets := map[int32]partitionProduceResult{}
	for _, blocks := range resp.Blocks {
		for partition, block := range blocks {
			if block.Err != sarama.ErrNoError {
				log.Printf("Failed to send message. err=%v\n", block.Err)
				return offsets, block.Err
			}

			if r, ok := offsets[partition]; ok {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: r.count + 1}
			} else {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: 1}
			}
		}
	}
	return offsets, nil
}

func (c *produceCmd) produce(in chan []message) {
	for b := range in {
		if err := c.produceBatch(c.leaders, b); err != nil {
			log.Printf("produce batch error %v", err.Error())
			return
		}
	}
}

func (c *produceCmd) readInput(q chan struct{}, stdin chan string, out chan string) {
	defer func() { close(out) }()
	for {
		select {
		case l, ok := <-stdin:
			if !ok {
				return
			}
			out <- l
		case <-q:
			return
		}
	}
}

var produceDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

Input is read from stdin and separated by newlines.

To specify the key, value and partition individually pass it as a JSON object
like the following:
    {"key": "id-23", "value": "message content", "partition": 0}
    {"k": "id-23", "v": "message content", "p": 0}

In case the input line cannot be interpeted as a JSON object the key and value
both default to the input line and partition to 0.

Examples:
Send a single message with a specific key:
  $ echo '{"key": "id-23", "value": "ola", "partition": 0}' | kt produce -topic greetings
  Sent message to partition 0 at Offset 3.
  $ echo '{"k": "id-23", "v": "ola", "p": 0}' | kt produce -topic greetings
  Sent message to partition 0 at Offset 3.
  $ kt consume -topic greetings -timeout 1s -offsets 0:3-
  {"partition":0,"Offset":3,"key":"id-23","message":"ola"}

Keep reading input from stdin until interrupted (via ^C).
  $ kt produce -topic greetings
  hello.
  Sent message to partition 0 at Offset 4.
  bonjour.
  Sent message to partition 0 at Offset 5.
  $ kt consume -topic greetings -timeout 1s -offsets 0:4-
  {"partition":0,"Offset":4,"key":"hello.","message":"hello."}
  {"partition":0,"Offset":5,"key":"bonjour.","message":"bonjour."}
`, EnvTopic, EnvBrokers)
