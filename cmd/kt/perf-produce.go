package main

import (
	"context"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bingoohuang/golog/pkg/randx"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
	"github.com/bingoohuang/gg/pkg/fla9"
	"github.com/bingoohuang/kt/pkg/kt"
	"github.com/rcrowley/go-metrics"
)

// from https://github.com/Shopify/sarama/blob/main/tools/kafka-producer-performance/main.go

type perfProduceCmd struct {
	sync             bool
	messageBinary    bool
	messageLoad      int
	messageSize      int
	brokers          string
	securityProtocol string

	tlsRootCACerts string
	tlsClientCert  string
	tlsClientKey   string
	topic          string

	partition         int
	throughput        int
	maxOpenRequests   int
	maxMessageBytes   int
	requiredAcks      string
	timeout           time.Duration
	partitioner       string
	compression       string
	flushFrequency    time.Duration
	flushBytes        int
	flushMessages     int
	flushMaxMessages  int
	clientID          string
	channelBufferSize int
	routines          int
	version           string
	verbose           bool

	seq       int32
	seqHeader bool
}

func (p *perfProduceCmd) run(args []string) {
	p.parseArgs(args)

	c := sarama.NewConfig()
	c.Net.MaxOpenRequests = p.maxOpenRequests
	c.Producer.MaxMessageBytes = p.maxMessageBytes
	c.Producer.RequiredAcks = ParseRequiredAcks(p.requiredAcks)
	c.Producer.Timeout = p.timeout
	c.Producer.Partitioner = parsePartitioner(p.partitioner, p.partition)
	c.Producer.Compression = parseCompression(p.compression)
	c.Producer.Flush.Frequency = p.flushFrequency
	c.Producer.Flush.Bytes = p.flushBytes
	c.Producer.Flush.Messages = p.flushMessages
	c.Producer.Flush.MaxMessages = p.flushMaxMessages
	c.Producer.Return.Successes = true
	c.ClientID = p.clientID
	c.ChannelBufferSize = p.channelBufferSize
	c.Version = parseVersion(p.version)

	p.setupSSL(c)

	if err := c.Validate(); err != nil {
		printErrorAndExit(69, "Invalid configuration: %s", err)
	}

	// Print out metrics periodically.
	done := make(chan struct{})
	ctx, cancel := CreateCancelContext()
	go func(ctx context.Context) {
		defer close(done)
		t := time.Tick(5 * time.Second)
		for {
			select {
			case <-t:
				p.printMetrics(os.Stdout, c.MetricRegistry)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	brokers := strings.Split(p.brokers, ",")
	if p.sync {
		p.runSyncProducer(c, brokers)
	} else {
		p.runAsyncProducer(c, brokers)
	}

	cancel()
	<-done

	// Print final metrics.
	p.printMetrics(os.Stdout, c.MetricRegistry)
}

func (p *perfProduceCmd) setupSSL(c *sarama.Config) {
	if p.securityProtocol != "SSL" {
		return
	}

	tlsConfig, err := tls.NewConfig(p.tlsClientCert, p.tlsClientKey)
	if err != nil {
		printErrorAndExit(69, "failed to load client certificate from: %s and private key from: %s: %v",
			p.tlsClientCert, p.tlsClientKey, err)
	}

	if p.tlsRootCACerts != "" {
		rootCAsBytes, err := os.ReadFile(p.tlsRootCACerts)
		if err != nil {
			printErrorAndExit(69, "failed to read root CA certificates: %v", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(rootCAsBytes) {
			printErrorAndExit(69, "failed to load root CA certificates from file: %s", p.tlsRootCACerts)
		}
		// Use specific root CA set vs the host's set
		tlsConfig.RootCAs = certPool
	}

	c.Net.TLS.Enable = true
	c.Net.TLS.Config = tlsConfig
}

func (p *perfProduceCmd) runAsyncProducer(c *sarama.Config, brokers []string) {
	producer, err := sarama.NewAsyncProducer(brokers, c)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrorAndExit(69, "Failed to close producer: %s", err)
		}
	}()

	messages := p.generateMessages(p.messageLoad)
	messagesDone := make(chan struct{})
	go func() {
		for i := 0; i < p.messageLoad; i++ {
			select {
			case <-producer.Successes():
			case err = <-producer.Errors():
				printErrorAndExit(69, "%s", err)
			}
		}
		messagesDone <- struct{}{}
	}()

	if p.throughput > 0 {
		ticker := time.NewTicker(time.Second)
		for idx, message := range messages {
			producer.Input() <- message
			if (idx+1)%p.throughput == 0 {
				<-ticker.C
			}
		}
		ticker.Stop()
	} else {
		for _, message := range messages {
			producer.Input() <- message
		}
	}

	<-messagesDone
	close(messagesDone)
}

func (p *perfProduceCmd) runSyncProducer(config *sarama.Config, brokers []string) {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrorAndExit(69, "Failed to close producer: %s", err)
		}
	}()

	messages := make([][]*sarama.ProducerMessage, p.routines)
	for i := 0; i < p.routines; i++ {
		if i == p.routines-1 {
			messages[i] = p.generateMessages(p.messageLoad/p.routines + p.messageLoad%p.routines)
		} else {
			messages[i] = p.generateMessages(p.messageLoad / p.routines)
		}
	}

	var wg sync.WaitGroup
	if p.throughput > 0 {
		for _, m := range messages {
			wg.Add(1)
			go func(messages []*sarama.ProducerMessage) {
				defer wg.Done()

				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()

				for _, message := range messages {
					for i := 0; i < p.throughput; i++ {
						if _, _, err = producer.SendMessage(message); err != nil {
							printErrorAndExit(69, "Failed to send message: %s", err)
						}
					}
					<-ticker.C
				}
			}(m)
		}
	} else {
		for _, m := range messages {
			wg.Add(1)
			go func(messages []*sarama.ProducerMessage) {
				defer wg.Done()

				for _, message := range messages {
					if _, _, err = producer.SendMessage(message); err != nil {
						printErrorAndExit(69, "Failed to send message: %s", err)
					}
				}
			}(m)
		}
	}
	wg.Wait()
}

func (p *perfProduceCmd) printMetrics(w io.Writer, r metrics.Registry) {
	recordSendRateMetric := r.Get("record-send-rate")
	requestLatencyMetric := r.Get("request-latency-in-ms")
	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
	requestsInFlightMetric := r.Get("requests-in-flight")

	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
		requestsInFlightMetric == nil {
		return
	}
	recordSendRate := recordSendRateMetric.(metrics.Meter).Snapshot()
	requestLatency := requestLatencyMetric.(metrics.Histogram).Snapshot()
	requestLatencyPercentiles := requestLatency.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
	outgoingByteRate := outgoingByteRateMetric.(metrics.Meter).Snapshot()
	requestsInFlight := requestsInFlightMetric.(metrics.Counter).Count()
	fmt.Fprintf(w, "%d records sent, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), "+
		"%.1f ms avg latency, %.1f ms stddev, %.1f ms 50th, %.1f ms 75th, "+
		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th, %d total req. in flight\n",
		recordSendRate.Count(),
		recordSendRate.RateMean(),
		recordSendRate.RateMean()*float64(p.messageSize)/1024/1024,
		outgoingByteRate.RateMean()/1024/1024,
		requestLatency.Mean(),
		requestLatency.StdDev(),
		requestLatencyPercentiles[0],
		requestLatencyPercentiles[1],
		requestLatencyPercentiles[2],
		requestLatencyPercentiles[3],
		requestLatencyPercentiles[4],
		requestsInFlight,
	)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func (p *perfProduceCmd) parseArgs(args []string) {
	f := fla9.NewFlagSet("perf-produce", fla9.ContinueOnError)
	f.BoolVar(&p.sync, "sync", false, "Use a synchronous producer.")
	f.BoolVar(&p.messageBinary, "msg-binary", false, "Use a random binary message content or ascii message.")
	f.IntVar(&p.messageLoad, "msg-load,n", 50000, "REQUIRED: The number of messages to produce to -topic.")
	f.IntVar(&p.messageSize, "msg-size", 100, "REQUIRED: The approximate size (in bytes) of each message to produce to -topic.")
	f.StringVar(&p.brokers, "brokers", "127.0.0.1:9092", "REQUIRED: A comma separated list of broker addresses.")
	f.StringVar(&p.securityProtocol, "security-protocol", "PLAINTEXT", "The name of the security protocol to talk to Kafka (PLAINTEXT, SSL) (default: PLAINTEXT).")
	f.StringVar(&p.tlsRootCACerts, "tls-ca-certs", "", "The path to a file that contains a set of root certificate authorities in PEM format to trust when verifying broker certificates when -security-protocol=SSL (leave empty to use the host's root CA set).")
	f.StringVar(&p.tlsClientCert, "tls-client-cert", "", "The path to a file that contains the client certificate to send to the broker in PEM format if client authentication is required when -security-protocol=SSL (leave empty to disable client authentication).")
	f.StringVar(&p.tlsClientKey, "tls-client-key", "", "The path to a file that contains the client private key linked to the client certificate in PEM format when -security-protocol=SSL (REQUIRED if tls-client-cert is provided).")
	f.StringVar(&p.topic, "topic", "producer_perf_test", "REQUIRED: The topic to run the performance test on.")
	f.IntVar(&p.partition, "partition", -1, "The partition of -topic to run the performance test on.")
	f.IntVar(&p.throughput, "throughput", 0, "The maximum number of messages to send per second (0 for no limit).")
	f.IntVar(&p.maxOpenRequests, "max-open-requests", 5, "The maximum number of unacknowledged requests the client will send on a single connection before blocking (default: 5).")
	f.IntVar(&p.maxMessageBytes, "max-msg-bytes", 1000000, "The max permitted size of a message.")
	f.StringVar(&p.requiredAcks, "required-acks", "local", "The required number of acks needed from the broker (all,none, local).")
	f.DurationVar(&p.timeout, "timeout", 10*time.Second, "The duration the producer will wait to receive -required-acks.")
	f.StringVar(&p.partitioner, "partitioner", "roundrobin", "The partitioning scheme to use (hash, manual, random, roundrobin).")
	f.StringVar(&p.compression, "compression", "none", "The compression method to use (none, gzip, snappy, lz4).")
	f.DurationVar(&p.flushFrequency, "flush-frequency", 0, "The best-effort frequency of flushes.")
	f.IntVar(&p.flushBytes, "flush-bytes", 0, "The best-effort number of bytes needed to trigger a flush.")
	f.IntVar(&p.flushMessages, "flush-msgs", 0, "The best-effort number of messages needed to trigger a flush.")
	f.IntVar(&p.flushMaxMessages, "flush-max-msgs", 0, "The maximum number of messages the producer will send in a single request.")
	f.StringVar(&p.clientID, "client-id", "sarama", "The client ID sent with every request to the brokers.")
	f.IntVar(&p.channelBufferSize, "channel-buf-size", 256, "The number of events to buffer in internal and external channels.")
	f.IntVar(&p.routines, "routines", 1, "The number of routines to send the messages from (-sync only).")
	f.StringVar(&p.version, "version", "0.8.2.0", "The assumed version of Kafka.")
	f.BoolVar(&p.verbose, "verbose", false, "Turn on sarama logging to stderr")
	f.BoolVar(&p.seqHeader, "seq", false, "Add seq header")

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of perf-produce:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, fmt.Sprintf(`
The values for -brokers can also be set via the environment variable %s respectively.
The values supplied on the command line win over environment variable values.

kt perf-produce -brokers=kafka:9092
`, kt.EnvBrokers))
	}

	err := f.Parse(args)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}
	if p.brokers == "" {
		printUsageErrorAndExit("-brokers is required")
	}
	if p.topic == "" {
		printUsageErrorAndExit("-topic is required")
	}
	if p.messageLoad <= 0 {
		printUsageErrorAndExit("-message-load must be greater than 0")
	}
	if p.messageSize <= 0 {
		printUsageErrorAndExit("-message-size must be greater than 0")
	}
	if p.routines < 1 || p.routines > p.messageLoad {
		printUsageErrorAndExit("-routines must be greater than 0 and less than or equal to -message-load")
	}
	if p.securityProtocol != "PLAINTEXT" && p.securityProtocol != "SSL" {
		printUsageErrorAndExit(fmt.Sprintf("-security-protocol %q is not supported", p.securityProtocol))
	}
	if p.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
}

func parseCompression(scheme string) sarama.CompressionCodec {
	switch scheme {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -compression: %s", scheme))
	}
	panic("should not happen")
}

func parsePartitioner(partitioner string, partition int) sarama.PartitionerConstructor {
	if partition < 0 && partitioner == "manual" {
		printUsageErrorAndExit("-partition must not be -1 for -partitioning=manual")
	}
	switch partitioner {
	case "manual":
		return sarama.NewManualPartitioner
	case "hash":
		return sarama.NewHashPartitioner
	case "random":
		return sarama.NewRandomPartitioner
	case "roundrobin":
		return sarama.NewRoundRobinPartitioner
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -partitioner: %s", partitioner))
	}
	panic("should not happen")
}

func parseVersion(version string) sarama.KafkaVersion {
	result, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		printUsageErrorAndExit(fmt.Sprintf("unknown -version: %s", version))
	}
	return result
}

func (p *perfProduceCmd) generateMessages(messageLoad int) []*sarama.ProducerMessage {
	messages := make([]*sarama.ProducerMessage, messageLoad)
	for i := 0; i < messageLoad; i++ {
		pm := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(p.partition)}
		if p.seqHeader {
			pm.Headers = []sarama.RecordHeader{{
				Key:   []byte("seq"),
				Value: []byte(fmt.Sprintf("%d", atomic.AddInt32(&p.seq, 1))),
			}}
		}
		if p.messageBinary {
			payload := make([]byte, p.messageSize)
			if _, err := rand.Read(payload); err != nil {
				printErrorAndExit(69, "Failed to generate message payload: %s", err)
			}
			pm.Value = sarama.ByteEncoder(payload)
		} else {
			pm.Value = sarama.StringEncoder(randx.String(p.messageSize))
		}

		messages[i] = pm
	}
	return messages
}