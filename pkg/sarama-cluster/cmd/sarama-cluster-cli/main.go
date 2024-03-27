package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	cluster "github.com/bingoohuang/kt/pkg/sarama-cluster"
	"github.com/spf13/pflag"
)

var (
	groupID = pflag.StringP("group", "g", "myGroup",
		"REQUIRED: The shared consumer group name")
	brokerList = pflag.StringArrayP("brokers", "b", []string{"127.0.0.1:9092"},
		"The comma separated list of brokers in the Kafka cluster")
	topicList = pflag.StringArrayP("topics", "t", nil,
		"REQUIRED: The comma separated list of topics to consume")
	offset = pflag.StringP("offset", "o", "oldest",
		"The offset to start with. Can be `oldest`, `newest`")
	verbose = pflag.BoolP("verbose", "v", false,
		"Whether to turn on sarama logging")
	limit = pflag.IntP("limit", "n", 10,
		"Limit number of messages to consume")
	user = pflag.StringP("user", "u", "",
		"SASL Plaintext user")
	pass = pflag.StringP("pass", "p", "",
		"SASL Plaintext password")
	kafkaVersion = pflag.StringP("kafka_version", "K", "0.10.0.0",
		"Kafka version")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	pflag.Parse()

	if *groupID == "" {
		printUsageErrorAndExit("You have to provide a -group name.")
	} else if len(*brokerList) == 0 {
		printUsageErrorAndExit("You have to provide -brokers.")
	} else if len(*topicList) == 0 {
		printUsageErrorAndExit("You have to provide -topics.")
	}

	// Init config
	c := cluster.NewConfig()
	if *verbose {
		sarama.Logger = logger
	} else {
		c.Consumer.Return.Errors = true
		c.Group.Return.Notifications = true
	}

	kv, err := sarama.ParseKafkaVersion(*kafkaVersion)
	if err != nil {
		printErrorAndExit(68, "parse kafka version failed: %v", err)
	}
	c.Version = kv

	if *user != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = *user
		c.Net.SASL.Password = *pass
		c.Net.SASL.Handshake = true
		c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		c.Net.SASL.Version = sarama.SASLHandshakeV0
	}

	switch *offset {
	case "oldest":
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		c.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	// Init consumer, consume errors & messages
	consumer, err := cluster.NewConsumer(*brokerList, *groupID, *topicList, c)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}
	defer consumer.Close()

	// Create signal channel
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// Consume all channels, wait for signal to exit
	for i := 1; *limit <= 0 || i <= *limit; {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "#%d: Topic: %s Partition: %d Offset: %d\n     Vaule: %s\n",
					i, msg.Topic, msg.Partition, msg.Offset, msg.Value)
				i++
				consumer.MarkOffset(msg, "")
			}
		case ntf, ok := <-consumer.Notifications():
			if ok {
				logger.Printf("Rebalanced: %+v\n", ntf)
			}
		case err, ok := <-consumer.Errors():
			if ok {
				logger.Printf("Error: %s\n", err.Error())
			}
		case <-sigCh:
			return
		}
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n\n", values...)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n\n", values...)
	flag.Usage()
	os.Exit(64)
}
