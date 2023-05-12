package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
	"github.com/bingoohuang/gg/pkg/fla9"
	"github.com/rcrowley/go-metrics"
)

type consoleProducerCmd struct {
	logger        *log.Logger
	tlsClientCert string
	headers       string
	topic         string
	key           string
	value         string
	partitioner   string
	brokers       string
	tlsClientKey  string
	partition     int
	tlsEnabled    bool
	tlsSkipVerify bool
	silent        bool
	showMetrics   bool
	verbose       bool
}

func (p *consoleProducerCmd) run(args []string) {
	p.parseArgs(args)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	if p.tlsEnabled {
		tlsConfig, err := tls.NewConfig(p.tlsClientCert, p.tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = p.tlsSkipVerify
	}

	switch p.partitioner {
	case "":
		if p.partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if p.partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", p.partitioner))
	}

	message := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(p.partition)}

	if p.key != "" {
		message.Key = sarama.StringEncoder(p.key)
	}

	switch {
	case p.value != "":
		message.Value = sarama.StringEncoder(p.value)
	case stdinAvailable():
		bytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			printErrorAndExit(66, "Failed to read data from the standard input: %s", err)
		}
		message.Value = sarama.ByteEncoder(bytes)
	default:
		printUsageErrorAndExit("-value is required, or you have to provide the value on stdin")
	}

	if p.headers != "" {
		var hdrs []sarama.RecordHeader
		arrHdrs := strings.Split(p.headers, ",")
		for _, h := range arrHdrs {
			if header := strings.Split(h, ":"); len(header) != 2 {
				printUsageErrorAndExit("-header should be key:value. Example: -headers=foo:bar,bar:foo")
			} else {
				hdrs = append(hdrs, sarama.RecordHeader{
					Key:   []byte(header[0]),
					Value: []byte(header[1]),
				})
			}
		}

		if len(hdrs) != 0 {
			message.Headers = hdrs
		}
	}

	producer, err := sarama.NewSyncProducer(strings.Split(p.brokers, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			p.logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		printErrorAndExit(69, "Failed to produce message: %s", err)
	} else if !p.silent {
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", p.topic, partition, offset)
	}
	if p.showMetrics {
		metrics.WriteOnce(config.MetricRegistry, os.Stderr)
	}
}

func (p *consoleProducerCmd) parseArgs(args []string) {
	f := fla9.NewFlagSet("console-producer", fla9.ContinueOnError)
	f.StringVar(&p.brokers, "brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	f.StringVar(&p.headers, "headers", "", "The headers of the message to produce. Example: -headers=foo:bar,bar:foo")
	f.StringVar(&p.topic, "topic", "", "the topic to produce to")
	f.StringVar(&p.key, "key", "", "The key of the message to produce. Can be empty.")
	f.StringVar(&p.value, "value", "", "the value of the message to produce. You can also provide the value on stdin.")
	f.StringVar(&p.partitioner, "partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	f.IntVar(&p.partition, "partition", -1, "The partition to produce to.")
	f.BoolVar(&p.verbose, "verbose", false, "Turn on sarama logging to stderr")
	f.BoolVar(&p.showMetrics, "metrics", false, "Output metrics on successful publish to stderr")
	f.BoolVar(&p.silent, "silent", false, "Turn off printing the message's topic, partition, and offset to stdout")
	f.BoolVar(&p.tlsEnabled, "tls-enabled", false, "Whether to enable TLS")
	f.BoolVar(&p.tlsSkipVerify, "tls-skip-verify", false, "Whether skip TLS server cert verification")
	f.StringVar(&p.tlsClientCert, "tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	f.StringVar(&p.tlsClientKey, "tls-client-key", "", "Client key for client authentication (use with tls-enabled and -tls-client-cert)")

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of console-producer:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, `
A simple command line tool to produce a single message to Kafka.

# Minimum invocation
kt console-produce -topic=test -value=value -brokers=kafka1:9092

# It will pick up a KAFKA_PEERS environment variable
export KAFKA_PEERS=kafka1:9092,kafka2:9092,kafka3:9092
kt console-produce -topic=test -value=value

# It will read the value from stdin by using pipes
echo "hello world" | kt console-produce -topic=test

# Specify a key:
echo "hello world" | kt console-produce -topic=test -key=key

# Partitioning: by default, kt console-produce will partition as follows:
# - manual partitioning if a -partition is provided
# - hash partitioning by key if a -key is provided
# - random partitioning otherwise.
#
# You can override this using the -partitioner argument:
echo "hello world" | kt console-produce -topic=test -key=key -partitioner=random

# Display all command line options
kt console-produce -help
`)
	}

	err := f.Parse(args)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	p.logger = log.New(os.Stderr, "", log.LstdFlags)

	if p.brokers == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if p.topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	if p.verbose {
		sarama.Logger = p.logger
	}
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}
