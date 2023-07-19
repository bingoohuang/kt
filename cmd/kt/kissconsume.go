package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bingoohuang/gg/pkg/fla9"
	. "github.com/bingoohuang/kt/pkg/kt"
)

type kissConsumer struct {
	brokers string
	topic   string
	version string
}

func (p *kissConsumer) run(args []string) {
	f := fla9.NewFlagSet("kiss-consume", fla9.ContinueOnError)
	f.StringVar(&p.brokers, "brokers,b", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.StringVar(&p.topic, "topic", "", "Kafka topic to send messages to")
	f.StringVar(&p.version, "version,v", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of kiss-consume:")
		f.PrintDefaults()
		fmt.Fprintln(os.Stderr, fmt.Sprintf(`
The value for -brokers can also be set via environment variables %s.
The value supplied on the command line wins over the environment variable value.

kt kiss-consume -brokers 192.168.126.200:9092,192.168.126.200:9192,192.168.126.200:9292 -topic=elastic.backup -version 2.5.1 )`, EnvBrokers))
	}

	f.Parse(args)

	consumer, err := sarama.NewConsumer(strings.Split(p.brokers, ","), nil)
	if err != nil {
		log.Panicln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	pc, err := consumer.ConsumePartition(p.topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panicln(err)
	}

	log.Println("Start")
	i := 0
	for ; ; i++ {
		msg := <-pc.Messages()
		if string(msg.Value) == "THE END" {
			break
		}

		log.Printf("Received: %s", msg.Value)
	}

	log.Printf("Finished. Received %d messages.\n", i)
}
