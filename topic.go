package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

type topicArgs struct {
	brokers    string
	auth       string
	filter     string
	partitions bool
	leaders    bool
	replicas   bool
	config     bool
	verbose    bool
	pretty     bool
	version    string
}

type topicCmd struct {
	brokers    []string
	auth       authConfig
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	config     bool
	verbose    bool
	pretty     bool
	version    sarama.KafkaVersion

	client sarama.Client
	admin  sarama.ClusterAdmin
}

type topic struct {
	Name       string            `json:"name"`
	Partitions []partition       `json:"partitions,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
}

type partition struct {
	ID           int32   `json:"id"`
	OldestOffset int64   `json:"oldest"`
	NewestOffset int64   `json:"newest"`
	Leader       string  `json:"leader,omitempty"`
	Replicas     []int32 `json:"replicas,omitempty"`
	ISRs         []int32 `json:"isrs,omitempty"`
}

func (c *topicCmd) parseFlags(as []string) topicArgs {
	var a topicArgs

	f := flag.NewFlagSet("topic", flag.ContinueOnError)
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", envAuth))
	f.BoolVar(&a.partitions, "partitions", false, "Include information per partition.")
	f.BoolVar(&a.leaders, "leaders", false, "Include leader information per partition.")
	f.BoolVar(&a.replicas, "replicas", false, "Include replica ids per partition.")
	f.BoolVar(&a.config, "config", false, "Include topic configuration.")
	f.StringVar(&a.filter, "filter", "", "Regex to filter topics by name.")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.BoolVar(&a.pretty, "pretty", true, "Control output pretty printing.")
	f.StringVar(&a.version, "version", "", "Kafka protocol version")
	f.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage of topic:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, topicDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return a
}

func (c *topicCmd) parseArgs(as []string) {
	var (
		err error
		re  *regexp.Regexp

		args = c.parseFlags(as)
	)

	c.brokers = parseBrokers(args.brokers)

	if re, err = regexp.Compile(args.filter); err != nil {
		failf("invalid regex for filter err=%s", err)
	}

	readAuthFile(args.auth, os.Getenv(envAuth), &c.auth)

	c.filter = re
	c.partitions = args.partitions
	c.leaders = args.leaders
	c.replicas = args.replicas
	c.config = args.config
	c.pretty = args.pretty
	c.verbose = args.verbose
	c.version = kafkaVersion(args.version)
}

func (c *topicCmd) connect() {
	var err error

	cfg := sarama.NewConfig()
	cfg.Version = c.version
	cfg.ClientID = "kt-topic-" + currentUserName()
	if c.verbose {
		log.Printf("sarama client configuration %#v\n", cfg)
	}

	if err := setupAuth(c.auth, cfg); err != nil {
		log.Printf("Failed to setupAuth err=%v", err)
	}

	if c.client, err = sarama.NewClient(c.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
	if c.admin, err = sarama.NewClusterAdmin(c.brokers, cfg); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}
}

func (c *topicCmd) run(as []string) {
	c.parseArgs(as)
	if c.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	c.connect()
	defer c.client.Close()
	defer c.admin.Close()

	var all []string
	var err error
	if all, err = c.client.Topics(); err != nil {
		failf("failed to read topics err=%v", err)
	}

	var topics []string
	for _, a := range all {
		if c.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	out := make(chan printContext)
	go print(out, c.pretty)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			c.print(top, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (c *topicCmd) print(name string, out chan printContext) {
	var (
		top topic
		err error
	)

	if top, err = c.readTopic(name); err != nil {
		log.Printf("failed to read info for topic %s. err=%v", name, err)
		return
	}

	ctx := printContext{output: top, done: make(chan struct{})}
	out <- ctx
	<-ctx.done
}

func (c *topicCmd) readTopic(name string) (topic, error) {
	var (
		err           error
		ps            []int32
		led           *sarama.Broker
		top           = topic{Name: name}
		configEntries []sarama.ConfigEntry
	)

	if c.config {

		resource := sarama.ConfigResource{Name: name, Type: sarama.TopicResource}
		if configEntries, err = c.admin.DescribeConfig(resource); err != nil {
			return top, err
		}

		top.Config = make(map[string]string)
		for _, entry := range configEntries {
			top.Config[entry.Name] = entry.Value
		}
	}

	if !c.partitions {
		return top, nil
	}

	if ps, err = c.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{ID: p}

		if np.OldestOffset, err = c.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = c.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if c.leaders {
			if led, err = c.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if c.replicas {
			if np.Replicas, err = c.client.Replicas(name, p); err != nil {
				return top, err
			}

			if np.ISRs, err = c.client.InSyncReplicas(name, p); err != nil {
				return top, err
			}
		}

		top.Partitions = append(top.Partitions, np)
	}

	return top, nil
}

var topicDocString = fmt.Sprintf(`
The values for -brokers can also be set via the environment variable %s respectively.
The values supplied on the command line win over environment variable values.`,
	envBrokers)
