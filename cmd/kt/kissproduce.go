package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bingoohuang/gg/pkg/fla9"
	"github.com/bingoohuang/gg/pkg/randx"
	. "github.com/bingoohuang/kt/pkg/kt"
)

type kissProducer struct {
	brokers          string
	topic            string
	version          string
	timeout          time.Duration
	msgTotal         int
	msgSize          int
	numThreads       int
	requiredAcks     string
	flushFrequency   time.Duration
	flushMessages    int
	flushMaxMessages int
	sync             bool
}

// help links:
// https://github.com/Shopify/sarama/blob/main/tools/kafka-producer-performance/main.go

func (p *kissProducer) run(args []string) {
	p.parseArgs(args)

	ctx, cancel := CreateCancelContext()

	c := sarama.NewConfig()
	c.Producer.RequiredAcks = ParseRequiredAcks(p.requiredAcks)
	c.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	c.Producer.Flush.Messages = p.flushMessages
	c.Producer.Flush.MaxMessages = p.flushMaxMessages
	c.Producer.Flush.Frequency = p.flushFrequency
	c.Producer.Return.Successes = true
	c.Version = parseVersion(p.version)

	if err := c.Validate(); err != nil {
		printErrorAndExit(69, "Invalid configuration: %s", err)
	}

	var kp kafkaProducer
	brokers := strings.Split(p.brokers, ",")
	if p.sync {
		producer, err := sarama.NewSyncProducer(brokers, c)
		if err != nil {
			log.Fatalln(err)
		}
		kp = &syncProducer{Context: ctx, SyncProducer: producer}
	} else {
		producer, err := sarama.NewAsyncProducer(brokers, c)
		if err != nil {
			log.Fatalln(err)
		}

		kp = &asyncProducer{Context: ctx, AsyncProducer: producer}
	}
	go kp.Start()
	defer func() {
		cancel()
		if err := kp.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	numCh := make(chan int)
	log.Println("Start")
	start := time.Now()

	for i := 0; i < p.numThreads; i++ {
		var chunk int
		if i == p.numThreads-1 {
			chunk = p.msgTotal/p.numThreads + (p.msgTotal % p.numThreads)
		} else {
			chunk = p.msgTotal / p.numThreads
		}
		go produce(ctx, kp, numCh, chunk, p.msgSize, p.topic)
	}

	nn := 0
	for i := 0; i < p.numThreads; i++ {
		n := <-numCh
		nn += n
		log.Printf("Thread %d has sent %d messages\n", i+1, n)
	}
	msg := &sarama.ProducerMessage{Topic: p.topic, Partition: -1, Value: sarama.StringEncoder("THE END")}
	if _, err := kp.SendMessage(msg); err != nil {
		log.Printf("FAILED to send END message: %s\n", err)
	}
	nn++
	duration := time.Since(start)
	log.Printf("Finish, TPS %f/s, total %d messages\n", float64(nn)/duration.Seconds(), nn)
}

func (p *kissProducer) parseArgs(args []string) {
	f := fla9.NewFlagSet("kiss-produce", fla9.ContinueOnError)
	f.StringVar(&p.brokers, "brokers,b", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.StringVar(&p.topic, "topic", "", "Kafka topic to send messages to")
	f.StringVar(&p.version, "version,v", "0.8.2.0", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.DurationVar(&p.timeout, "timeout", 3*time.Second, "Timeout for request to Kafka (default: 3s)")
	f.IntVar(&p.msgTotal, "n", 50000, `total number of messages`)
	f.IntVar(&p.msgSize, "size", 100, `message size`)
	f.IntVar(&p.numThreads, "routines,t", 1, `goroutine number`)
	f.StringVar(&p.requiredAcks, "acks", "local", `how many replica acknowledgements it must see before responding, e.g.;local/all/none`)
	f.DurationVar(&p.flushFrequency, "flush.frequency", 0, `The best-effort frequency of flushes`)
	f.IntVar(&p.flushMessages, "flush.messages", 0, `The best-effort number of messages needed to trigger a flush.`)
	f.IntVar(&p.flushMaxMessages, "flush.max", 0, `The maximum number of messages the producer will send in a single broker request..`)
	f.BoolVar(&p.sync, "sync", false, `Use sync producer`)

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of kiss-produce:")
		f.PrintDefaults()
		fmt.Fprintln(os.Stderr, fmt.Sprintf(`
The value for -brokers can also be set via environment variables %s.
The value supplied on the command line wins over the environment variable value.

kt kiss-produce -brokers 192.168.126.200:9092,192.168.126.200:9192,192.168.126.200:9292 -topic=elastic.backup -size 10 )`, EnvBrokers))
	}

	err := f.Parse(args)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}
}

func CreateCancelContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Println(<-sigs)
		cancel()
	}()
	return ctx, cancel
}

func produce(ctx context.Context, p kafkaProducer, numCh chan int, n int, s int, topic string) {
	i := 0
	for ; i < n; i++ {
		if ctx.Err() != nil {
			break
		}
		msg := &sarama.ProducerMessage{Topic: topic, Partition: -1, Value: sarama.StringEncoder(randx.String(s))}
		if _, err := p.SendMessage(msg); err != nil {
			log.Printf("FAILED to send message: %s\n", err)
		}
	}
	numCh <- i
}

func ParseRequiredAcks(acks string) sarama.RequiredAcks {
	acks = strings.ToLower(acks)
	switch acks {
	case "waitforlocal", "local":
		return sarama.WaitForLocal
	case "noresponse", "none":
		return sarama.NoResponse
	case "waitforall", "all":
		return sarama.WaitForAll
	default:
		return sarama.WaitForLocal
	}
}

type kafkaProducer interface {
	SendMessage(msg *sarama.ProducerMessage) (interface{}, error)
	Start()
	Close() error
}

type asyncProducer struct {
	sarama.AsyncProducer
	context.Context
}

func (p *asyncProducer) Start() {
	for p.Err() == nil {
		select {
		case <-p.Successes():
		case err := <-p.Errors():
			log.Printf("kafka producer send error %+v for message %+v", err.Err, err.Msg)
		}
	}
}

func (p *asyncProducer) SendMessage(msg *sarama.ProducerMessage) (interface{}, error) {
	select {
	case <-p.Done():
		return AsyncProducerResult{ContextDone: true}, nil
	case p.Input() <- msg:
		return AsyncProducerResult{Enqueued: true}, nil
	}
}

type syncProducer struct {
	sarama.SyncProducer
	context.Context
}

func (p *syncProducer) Start() {}

type AsyncProducerResult struct {
	Enqueued    bool
	ContextDone bool
}
type SyncProducerResult struct {
	Partition int32
	Offset    int64
}

func (p *syncProducer) SendMessage(msg *sarama.ProducerMessage) (interface{}, error) {
	partition, offset, err := p.SyncProducer.SendMessage(msg)
	return SyncProducerResult{Partition: partition, Offset: offset}, err
}
