package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
	"github.com/bingoohuang/gg/pkg/fla9"
	"github.com/bingoohuang/kt/pkg/kt"
)

type consoleConsumerCmd struct {
	logger        *log.Logger
	tlsClientCert string
	topic         string
	version       string
	partitions    string
	offset        string
	flagBrokers   string

	tlsClientKey string

	brokers       []string
	bufferSize    int
	verbose       bool
	tlsEnabled    bool
	tlsSkipVerify bool
}

func (p *consoleConsumerCmd) run(args []string) {
	p.parseArgs(args)

	var initialOffset int64
	switch p.offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	sc := sarama.NewConfig()
	if p.tlsEnabled {
		tlsConfig, err := tls.NewConfig(p.tlsClientCert, p.tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tlsConfig
		sc.Net.TLS.Config.InsecureSkipVerify = p.tlsSkipVerify
	}

	var err error
	sc.Version, err = kt.ParseKafkaVersion(p.version)
	if err != nil {
		failStartup(err.Error())
	}
	c, err := sarama.NewConsumer(p.brokers, sc)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := p.getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, p.bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals
		p.logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(p.topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", string(msg.Value))
			fmt.Println()
		}
	}()

	wg.Wait()
	p.logger.Println("Done consuming topic", p.topic)
	close(messages)

	if err := c.Close(); err != nil {
		p.logger.Println("Failed to close consumer: ", err)
	}
}

func (p *consoleConsumerCmd) getPartitions(c sarama.Consumer) ([]int32, error) {
	if p.partitions == "all" {
		return c.Partitions(p.topic)
	}

	tmp := strings.Split(p.partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func (p *consoleConsumerCmd) parseArgs(args []string) {
	f := fla9.NewFlagSet("console-consume", fla9.ContinueOnError)

	f.StringVar(&p.flagBrokers, "brokers", "", "The comma separated list of brokers in the Kafka cluster")
	f.StringVar(&p.topic, "topic", "", "The topic to consume")
	f.StringVar(&p.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or by env %s", kt.EnvVersion))

	f.StringVar(&p.partitions, "partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	f.StringVar(&p.offset, "offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	f.BoolVar(&p.verbose, "verbose", false, "Whether to turn on sarama logging")
	f.BoolVar(&p.tlsEnabled, "tls-enabled", false, "Whether to enable TLS")
	f.BoolVar(&p.tlsSkipVerify, "tls-skip-verify", false, "Whether skip TLS server cert verification")
	f.StringVar(&p.tlsClientCert, "tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	f.StringVar(&p.tlsClientKey, "tls-client-key", "", "Client key for client authentication (use with tls-enabled and -tls-client-cert)")
	f.IntVar(&p.bufferSize, "buffer-size", 256, "The buffer size of the message channel.")

	p.logger = log.New(os.Stderr, "", log.LstdFlags)

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of console-consume:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, `
A simple command line tool to consume partitions of a topic and print the messages on the standard output.

kt console-consume -brokers=kafka:9092 -topic test-topic
# Minimum invocation
kt console-consume -topic=test -brokers=kafka1:9092

# It will pick up a KAFKA_PEERS environment variable
export KAFKA_PEERS=kafka1:9092,kafka2:9092,kafka3:9092
kt console-consume -topic=test

# You can specify the offset you want to start at. It can be either oldest or newest. The default is newest.
kt console-consume -topic=test -offset=oldest
kt console-consume -topic=test -offset=newest

# You can specify the partition(s) you want to consume as a comma-separated
# list. The default is all.
kt console-consume -topic=test -partitions=1,2,3

# Display all command line options
kt console-consume -help
`)
	}

	err := f.Parse(args)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	p.brokers = kt.ParseBrokers(p.flagBrokers)
	if p.topic, err = kt.ParseTopic(p.topic); err != nil {
		failStartup(err.Error())
	}

	if p.verbose {
		sarama.Logger = p.logger
	}
}
