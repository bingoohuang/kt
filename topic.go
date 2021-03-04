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

func (r *topicCmd) parseFlags(as []string) topicArgs {
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
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", envVersion))
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

func (r *topicCmd) parseArgs(as []string) {
	var err error
	var re *regexp.Regexp

	a := r.parseFlags(as)
	r.brokers = parseBrokers(a.brokers)

	if re, err = regexp.Compile(a.filter); err != nil {
		failf("invalid regex for filter err=%s", err)
	}

	readAuthFile(a.auth, os.Getenv(envAuth), &r.auth)

	r.filter = re
	r.partitions = a.partitions
	r.leaders = a.leaders
	r.replicas = a.replicas
	r.config = a.config
	r.pretty = a.pretty
	r.verbose = a.verbose
	r.version = kafkaVersion(a.version)
}

func (r *topicCmd) connect() {
	var err error

	cfg := sarama.NewConfig()
	cfg.Version = r.version
	cfg.ClientID = "kt-topic-" + currentUserName()
	if r.verbose {
		log.Printf("sarama client configuration %#v\n", cfg)
	}

	if err := setupAuth(r.auth, cfg); err != nil {
		log.Printf("Failed to setupAuth err=%v", err)
	}

	if r.client, err = sarama.NewClient(r.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
	if r.admin, err = sarama.NewClusterAdmin(r.brokers, cfg); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}
}

func (r *topicCmd) run(as []string) {
	r.parseArgs(as)
	if r.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	r.connect()
	defer r.client.Close()
	defer r.admin.Close()

	var all []string
	var err error
	if all, err = r.client.Topics(); err != nil {
		failf("failed to read topics err=%v", err)
	}

	var topics []string
	for _, a := range all {
		if r.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	out := make(chan printContext)
	go print(out, r.pretty)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			r.print(top, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (r *topicCmd) print(name string, out chan printContext) {
	var (
		top topic
		err error
	)

	if top, err = r.readTopic(name); err != nil {
		log.Printf("failed to read info for topic %s. err=%v", name, err)
		return
	}

	ctx := printContext{output: top, done: make(chan struct{})}
	out <- ctx
	<-ctx.done
}

func (r *topicCmd) readTopic(name string) (topic, error) {
	var (
		err           error
		ps            []int32
		led           *sarama.Broker
		top           = topic{Name: name}
		configEntries []sarama.ConfigEntry
	)

	if r.config {

		resource := sarama.ConfigResource{Name: name, Type: sarama.TopicResource}
		if configEntries, err = r.admin.DescribeConfig(resource); err != nil {
			return top, err
		}

		top.Config = make(map[string]string)
		for _, entry := range configEntries {
			top.Config[entry.Name] = entry.Value
		}
	}

	if !r.partitions {
		return top, nil
	}

	if ps, err = r.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{ID: p}

		if np.OldestOffset, err = r.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = r.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if r.leaders {
			if led, err = r.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if r.replicas {
			if np.Replicas, err = r.client.Replicas(name, p); err != nil {
				return top, err
			}

			if np.ISRs, err = r.client.InSyncReplicas(name, p); err != nil {
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
