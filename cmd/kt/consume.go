package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	. "github.com/bingoohuang/kt/pkg/kt"

	"github.com/Shopify/sarama"
)

type consumeCmd struct {
	conf ConsumerConfig
}

func (c *consumeCmd) run(args []string) {
	c.parseArgs(args)
	if _, err := StartConsume(c.conf); err != nil {
		failStartup(err.Error())
	}
}

type consumeArgs struct {
	topic   string
	brokers string
	auth    string
	timeout time.Duration
	offsets string
	verbose bool
	version string
	encVal  string
	encKey  string
	pretty  bool
	group   string
}

func failStartup(msg string) {
	log.Print(msg)
	failf(`"use "kt command -help" for more information"`)
}

func (c *consumeCmd) parseArgs(as []string) {
	a := c.parseFlags(as)

	if a.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	conf := ConsumerConfig{
		Brokers: ParseBrokers(a.brokers),
		Timeout: a.timeout,
		Group:   a.group,
		Offsets: a.offsets,
	}

	var err error

	if conf.Topic, err = ParseTopic(a.topic); err != nil {
		failStartup(err.Error())
	}

	if conf.Version, err = ParseKafkaVersion(a.version); err != nil {
		failStartup(err.Error())
	}

	if err = conf.Auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}

	var valEncoder, keyEncoder BytesEncoder

	if valEncoder, err = ParseBytesEncoder(a.encVal); err != nil {
		failStartup(err.Error())
	}
	if keyEncoder, err = ParseBytesEncoder(a.encKey); err != nil {
		failStartup(err.Error())
	}

	conf.MessageConsumer = NewPrintMessageConsumer(a.pretty, keyEncoder, valEncoder)

	c.conf = conf
}

func (c *consumeCmd) parseFlags(as []string) consumeArgs {
	var a consumeArgs
	f := flag.NewFlagSet("consume", flag.ContinueOnError)
	f.StringVar(&a.topic, "topic", "", "Topic to consume (required).")
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env", EnvAuth))
	f.StringVar(&a.offsets, "offsets", "newest", "Specifies what messages to read by partition and Offset range (defaults to newest).")
	f.DurationVar(&a.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.BoolVar(&a.pretty, "pretty", false, "Control Output pretty printing.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.StringVar(&a.encVal, "enc.value", "string", "Present message value as string|hex|base64, defaults to string.")
	f.StringVar(&a.encKey, "enc.key", "string", "Present message key as string|hex|base64, defaults to string.")
	f.StringVar(&a.group, "group", "", "Consumer group to use for marking offsets. kt will mark offsets if this arg is supplied.")

	f.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage of consume:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, consumeDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return a
}

var consumeDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

Offsets can be specified as a comma-separated list of intervals:
  [[partition=Start:End],...]

The default is to consume from the oldest Offset on every partition for the given topic.
 - partition is the numeric identifier for a partition. You can use "all" to
   specify a default OffsetInterval for all partitions.
 - Start is the included Offset where consumption should Start.
 - End is the included Offset where consumption should End.

The following syntax is supported for each Offset:
  (oldest|newest|resume)?(+|-)?(\d+)?
 - "oldest" and "newest" refer to the oldest and newest offsets known for a given partition.
 - "resume" can be used in combination with -group.
 - You can use "+" with a numeric value to skip the given number of messages since the oldest Offset. 
   For example, "1=+20" will skip 20 Offset value since the oldest Offset for partition 1.
 - You can use "-" with a numeric value to refer to only the given number of messages before the newest Offset. 
   For example, "1=-10" will refer to the last 10 Offset values before the newest Offset for partition 1.
 - Relative offsets are based on numeric values and will not take skipped offsets (e.g. due to compaction) into account.
 - Given only a numeric value, it is interpreted as an absolute Offset value.

More examples:
 - To consume messages from partition 0 between offsets 10 and 20 (inclusive).
     0=10:20
 - To define an OffsetInterval for all partitions use -1 as the partition identifier:
     all=2:10
 - You can also override the offsets for a single partition, in this case 2:
     all=1-10,2=5-10
 - To consume from multiple partitions:
     0=4:,2=1:10,6
 - This would consume messages from three partitions:
     - Anything from partition 0 starting at Offset 4.
     - Messages between offsets 1 and 10 from partition 2.
     - Anything from partition 6.
 - To Start at the latest Offset for each partition:
     all=newest:
 - Or shorter:
     newest:
 - To consume the last 10 messages:
     newest-10
 - To skip the first 15 messages starting with the oldest Offset:
     oldest+10
 - In both cases you can omit "newest" and "oldest":
     -10
 - and
     +10
`, EnvTopic, EnvBrokers)
