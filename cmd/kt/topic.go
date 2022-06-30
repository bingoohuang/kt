package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	. "github.com/bingoohuang/kt/pkg/kt"

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
	auth       AuthConfig
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
	out    chan PrintContext
}

type topicInfo struct {
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
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", EnvAuth))
	f.BoolVar(&a.partitions, "partitions", false, "Include information per partition.")
	f.BoolVar(&a.leaders, "leaders", false, "Include leader information per partition.")
	f.BoolVar(&a.replicas, "replicas", false, "Include replica ids per partition.")
	f.BoolVar(&a.config, "config", false, "Include topic configuration.")
	f.StringVar(&a.filter, "filter", "", "Regex to filter topics by name.")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.BoolVar(&a.pretty, "pretty", false, "Control Output pretty printing.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, fmt.Sprintf(`
The values for -brokers can also be set via the environment variable %s respectively.
The values supplied on the command line win over environment variable values.
`, EnvBrokers))
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
	a := r.parseFlags(as)
	r.brokers = ParseBrokers(a.brokers)

	re, err := regexp.Compile(a.filter)
	if err != nil {
		failf("invalid regex for filter err=%s", err)
	}

	if err := r.auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}

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
	cfg := sarama.NewConfig()
	cfg.Version = r.version
	cfg.ClientID = "kt-topic-" + CurrentUserName()
	if r.verbose {
		log.Printf("sarama client configuration %#v\n", cfg)
	}

	if err := r.auth.SetupAuth(cfg); err != nil {
		log.Printf("Failed to setupAuth err=%v", err)
	}

	var err error
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

	r.out = make(chan PrintContext)
	go PrintOut(r.out, r.pretty)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			r.print(top)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (r *topicCmd) print(name string) {
	top, err := r.readTopic(name)
	if err != nil {
		log.Printf("failed to read info for topic %s. err=%v", name, err)
		return
	}

	ctx := PrintContext{Output: top, Done: make(chan struct{})}
	r.out <- ctx
	<-ctx.Done
}

func (r *topicCmd) readTopic(name string) (topicInfo, error) {
	top := topicInfo{Name: name}
	if r.config {
		resource := sarama.ConfigResource{Name: name, Type: sarama.TopicResource}
		configEntries, err := r.admin.DescribeConfig(resource)
		if err != nil {
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

	var err error
	var ps []int32
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
			led, err := r.client.Leader(name, p)
			if err != nil {
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
