package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	. "github.com/bingoohuang/kt/pkg/kt"
)

type adminCmd struct {
	admin       sarama.ClusterAdmin
	timeout     *time.Duration
	topicDetail *sarama.TopicDetail
	auth        AuthConfig

	topicCreate string
	topicDelete string

	brokers      []string
	version      sarama.KafkaVersion
	verbose      bool
	validateOnly bool
}

type adminArgs struct {
	brokers string
	version string
	timeout string
	auth    string

	topicCreate  string
	topicConfig  string
	topicDelete  string
	verbose      bool
	validateOnly bool
}

func (r *adminCmd) parseArgs(as []string) {
	a := r.parseFlags(as)

	r.verbose = a.verbose
	r.version = kafkaVersion(a.version)
	r.timeout = parseTimeout(a.timeout)
	if err := r.auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}
	r.brokers = ParseBrokers(a.brokers)
	r.validateOnly = a.validateOnly
	r.topicCreate = a.topicCreate
	r.topicDelete = a.topicDelete

	if r.topicCreate != "" {
		var (
			err error
			buf []byte
		)
		if strings.HasPrefix(a.topicConfig, "@") {
			buf, err = ioutil.ReadFile(a.topicConfig[1:])
			if err != nil {
				failf("failed to read topic detail err=%v", err)
			}
		} else {
			buf = []byte(a.topicConfig)
		}

		var detail TopicDetail
		if err = json.Unmarshal(buf, &detail); err != nil {
			failf("failed to unmarshal topic detail err=%v", err)
		}
		r.topicDetail = detail.ToSaramaTopicDetail()
	}
}

// TopicDetail is structure convenient of topic.config  in topic.create.
type TopicDetail struct {
	NumPartitions  *int32 `json:"NumPartitions,omitempty"`
	NumPartitions2 *int32 `json:"numPartitions,omitempty"`
	NumPartitions3 *int32 `json:"mp,omitempty"`

	ReplicationFactor  *int16 `json:"ReplicationFactor,omitempty"`
	ReplicationFactor2 *int16 `json:"replicationFactor,omitempty"`
	ReplicationFactor3 *int16 `json:"rf,omitempty"`

	ReplicaAssignment  *map[int32][]int32 `json:"ReplicaAssignment,omitempty"`
	ReplicaAssignment2 *map[int32][]int32 `json:"replicaAssignment,omitempty"`
	ReplicaAssignment3 *map[int32][]int32 `json:"ra,omitempty"`

	ConfigEntries  *map[string]*string `json:"ConfigEntries,omitempty"`
	ConfigEntries2 *map[string]*string `json:"configEntries,omitempty"`
	ConfigEntries3 *map[string]*string `json:"ce,omitempty"`
}

// ToSaramaTopicDetail convert to *sarama.TopicDetail.
func (r *TopicDetail) ToSaramaTopicDetail() *sarama.TopicDetail {
	d := &sarama.TopicDetail{}
	d.NumPartitions = FirstNotNil(r.NumPartitions, r.NumPartitions2, r.NumPartitions3)
	d.ReplicationFactor = FirstNotNil(r.ReplicationFactor, r.ReplicationFactor2, r.ReplicationFactor3)
	d.ReplicaAssignment = FirstNotNil(r.ReplicaAssignment, r.ReplicaAssignment2, r.ReplicaAssignment3)
	d.ConfigEntries = FirstNotNil(r.ConfigEntries, r.ConfigEntries2, r.ConfigEntries3)

	return d
}

func (r *adminCmd) run(args []string) {
	r.parseArgs(args)

	if r.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	var err error
	if r.admin, err = sarama.NewClusterAdmin(r.brokers, r.saramaConfig()); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}

	if r.topicCreate != "" {
		r.runCreateTopic()
	}

	if r.topicDelete != "" {
		r.runDeleteTopic()
	}
}

func (r *adminCmd) runCreateTopic() {
	if err := r.admin.CreateTopic(r.topicCreate, r.topicDetail, r.validateOnly); err != nil {
		failf("failed to create topic err=%v", err)
	}
}

func (r *adminCmd) runDeleteTopic() {
	if err := r.admin.DeleteTopic(r.topicDelete); err != nil {
		failf("failed to delete topic err=%v", err)
	}
}

func (r *adminCmd) saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = r.version
	cfg.ClientID = "kt-admin-" + CurrentUserName()

	if r.timeout != nil {
		cfg.Admin.Timeout = *r.timeout
	}

	if err := r.auth.SetupAuth(cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	return cfg
}

func (r *adminCmd) parseFlags(as []string) adminArgs {
	var a adminArgs
	f := flag.NewFlagSet("admin", flag.ContinueOnError)
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.StringVar(&a.timeout, "timeout", "", "Timeout for request to Kafka (default: 3s)")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", EnvAuth))

	f.StringVar(&a.topicCreate, "topic.create", "", "Name of the topic that should be created.")
	f.StringVar(&a.topicConfig, "topic.config", "", "Direct JSON string or @file.json of topic detail. cf sarama.TopicDetail")
	f.BoolVar(&a.validateOnly, "validate.only", false, "Flag to indicate whether operation should only validate input (supported for topic.create).")
	f.StringVar(&a.topicDelete, "topic.delete", "", "Name of the topic that should be deleted.")

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of admin:")
		f.PrintDefaults()
		fmt.Fprintln(os.Stderr, adminDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return a
}

var adminDocString = fmt.Sprintf(`
The value for -brokers can also be set via environment variables %s.
The value supplied on the command line wins over the environment variable value.

The topic details should be passed via a JSON file that represents a sarama.TopicDetail struct.
cf https://godoc.org/github.com/IBM/sarama#TopicDetail

A simple way to pass a JSON file is to use a tool like https://github.com/fgeller/jsonify and shell's process substition:

kt admin -topic.create morenews -topic.config $(jsonify --NumPartitions 1 --ReplicationFactor 1)`, EnvBrokers)
