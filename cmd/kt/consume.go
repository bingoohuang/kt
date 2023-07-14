package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bingoohuang/gg/pkg/netx/freeport"
	"github.com/bingoohuang/gg/pkg/osx"
	"github.com/bingoohuang/godaemon"
	. "github.com/bingoohuang/kt/pkg/kt"
)

type consumeCmd struct {
	sseSender *SSESender
	conf      ConsumerConfig
}

func (c *consumeCmd) run(args []string) {
	c.parseArgs(args)
	if _, err := StartConsume(c.conf); err != nil {
		failStartup(err.Error())
	}
}

type consumeArgs struct {
	grepExpr                       *regexp.Regexp
	webContext                     string
	offsets, group, encKey, encVal string
	topic, brokers, auth, version  string

	grep    string
	timeout time.Duration
	webPort int
	n       int64

	web    bool
	daemon bool

	verbose, pretty bool
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
		Timeout: a.timeout, Group: a.group, Offsets: a.offsets,
	}

	var err error
	if conf.Topic, err = ParseTopic(a.topic, true); err != nil {
		failStartup(err.Error())
	}
	if conf.Version, err = ParseKafkaVersion(a.version); err != nil {
		failStartup(err.Error())
	}
	if err = conf.Auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}

	valEncoder := ParseBytesEncoder(a.encVal)
	keyEncoder := ParseBytesEncoder(a.encKey)

	conf.MessageConsumer = NewPrintMessageConsumer(a.pretty, keyEncoder, valEncoder, c.sseSender, a.grepExpr, a.n)
	c.conf = conf
}

func (c *consumeCmd) parseFlags(as []string) consumeArgs {
	var a consumeArgs
	f := flag.NewFlagSet("consume", flag.ContinueOnError)
	f.StringVar(&a.topic, "topic", "", "Topic to consume (required)")
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port set to 9092 when omitted (defaults to localhost:9092)")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, or by env %s", EnvAuth))
	f.StringVar(&a.offsets, "offsets", "newest", "Specifies what messages to read by partition and offset range (defaults to newest)")
	f.DurationVar(&a.timeout, "timeout", 0, "Timeout after not reading messages (default 0 to disable)")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr")
	f.BoolVar(&a.pretty, "pretty", false, "Control Output pretty printing")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or by env %s", EnvVersion))
	f.StringVar(&a.encVal, "enc.val", "string", "Present message value as string|hex|base64 or combinations(e.g. string,base64), defaults to string")
	f.StringVar(&a.encKey, "enc.key", "string", "Present message key as string|hex|base64, defaults to string")
	f.StringVar(&a.group, "group", "", "Consumer group to use for marking offsets. kt will mark offsets if this arg is supplied")
	f.StringVar(&a.grep, "grep", "", "Grep")
	f.BoolVar(&a.web, "web", false, `Start web server for HTTP requests and responses event`)
	f.IntVar(&a.webPort, "webport", 0, `Web server port if web is enable`)
	f.Int64Var(&a.n, "n", 0, `Max message to consume`)
	f.StringVar(&a.webContext, "webcontext", "", `Web server context path if web is enable`)
	f.BoolVar(&a.daemon, "daemon", false, `Run in daemonize mode`)
	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, consumeDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	if a.grep != "" {
		if a.grepExpr, err = regexp.Compile(a.grep); err != nil {
			log.Fatalf("compile regex %s failed: %v", a.grep, err)
		}
	}

	godaemon.Daemonize(a.daemon)
	c.parseWeb(&a)

	return a
}

func (c *consumeCmd) parseWeb(a *consumeArgs) {
	if !a.web {
		return
	}

	var port int
	if a.webPort > 0 {
		port = freeport.PortStart(a.webPort)
	} else {
		port = freeport.Port()
	}

	stream := NewSSEStream()
	c.sseSender = &SSESender{Stream: stream}
	contextPath := path.Join("/", a.webContext)
	log.Printf("contextPath: %s", contextPath)

	http.Handle("/", http.HandlerFunc(SSEWebHandler(contextPath, stream)))
	log.Printf("start to listen on %d", port)
	go func() {
		addr := fmt.Sprintf(":%d", port)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("listen and serve failed: %v", err)
		}
	}()

	go osx.OpenBrowser(fmt.Sprintf("http://127.0.0.1:%d%s", port, contextPath))
}

var consumeDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via env variables %s and %s respectively.
The values supplied on the command line win over env variable values.
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
 - Use "+" with a numeric value to skip the given number of messages since the oldest Offset. 
   For example, "1=+20" will skip 20 Offset value since the oldest Offset for partition 1.
 - Use "-" with a numeric value to refer to only the given number of messages before the newest Offset. 
   For example, "1=-10" will refer to the last 10 Offset values before the newest Offset for partition 1.
 - Relative offsets are based on numeric values and will not take skipped offsets (e.g. due to compaction) into account.
 - Given only a numeric value, it is interpreted as an absolute Offset value.
More examples:
 - 0=10:20       To consume messages from partition 0 between offsets 10 and 20 (inclusive).
 - all=2:10      To define an OffsetInterval for all partitions use -1 as the partition identifier:
 - all=1-5,2=5-7 Override the offsets for a single partition, in this case 2:
 - 0=4:,2=1:10,6 To consume from multiple partitions:
 - This would consume messages from three partitions: p=0 offset>=4,  p=2 1<=offsets<=10, p=6 all offsets.
 - all=newest    To Start at the latest Offset for each partition
 - newest        Shorter of above
 - newest-10     To consume the last 10 messages
 - -10           Omit "newest", same with above
 - oldest+10     To skip the first 15 messages starting with the oldest Offset
 - +10           Omit "oldest",, same with above
`, EnvTopic, EnvBrokers)
