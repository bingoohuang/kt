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

	"github.com/Shopify/sarama"
)

type adminCmd struct {
	brokers []string
	verbose bool
	version sarama.KafkaVersion
	timeout *time.Duration
	auth    authConfig

	topicCreate  string
	topicDetail  *sarama.TopicDetail
	validateOnly bool
	topicDelete  string

	admin sarama.ClusterAdmin
}

type adminArgs struct {
	brokers string
	verbose bool
	version string
	timeout string
	auth    string

	topicCreate  string
	topicConfig  string
	validateOnly bool
	topicDelete  string
}

func (r *adminCmd) parseArgs(as []string) {
	a := r.parseFlags(as)

	r.verbose = a.verbose
	r.version = kafkaVersion(a.version)
	r.timeout = parseTimeout(os.Getenv(envAdminTimeout))
	if a.timeout != "" {
		r.timeout = parseTimeout(a.timeout)
	}

	readAuthFile(a.auth, os.Getenv(envAuth), &r.auth)

	r.brokers = parseBrokers(a.brokers)
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
				failf("failed to read topicInfo detail err=%v", err)
			}
		} else {
			buf = []byte(a.topicConfig)
		}

		var detail TopicDetail
		if err = json.Unmarshal(buf, &detail); err != nil {
			failf("failed to unmarshal topicInfo detail err=%v", err)
		}
		r.topicDetail = detail.ToSaramaTopicDetail()
	}
}

// TopicDetail is structure convenient of topicInfo.config  in topicInfo.create.
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
	d.NumPartitions = FirstNotNilInt32(r.NumPartitions, r.NumPartitions2, r.NumPartitions3)
	d.ReplicationFactor = FirstNotNilInt16(r.ReplicationFactor, r.ReplicationFactor2, r.ReplicationFactor3)
	d.ReplicaAssignment = FirstNotNilMapInt32(r.ReplicaAssignment, r.ReplicaAssignment2, r.ReplicaAssignment3)
	d.ConfigEntries = FirstNotNilMapString(r.ConfigEntries, r.ConfigEntries2, r.ConfigEntries3)

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
		failf("failed to create topicInfo err=%v", err)
	}
}

func (r *adminCmd) runDeleteTopic() {
	if err := r.admin.DeleteTopic(r.topicDelete); err != nil {
		failf("failed to delete topicInfo err=%v", err)
	}
}

func (r *adminCmd) saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = r.version
	cfg.ClientID = "kt-admin-" + currentUserName()

	if r.timeout != nil {
		cfg.Admin.Timeout = *r.timeout
	}

	if err := setupAuth(r.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	return cfg
}

func (r *adminCmd) parseFlags(as []string) adminArgs {
	var a adminArgs
	f := flag.NewFlagSet("consume", flag.ContinueOnError)
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", envVersion))
	f.StringVar(&a.timeout, "timeout", "", "Timeout for request to Kafka (default: 3s)")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", envAuth))

	f.StringVar(&a.topicCreate, "topicInfo.create", "", "Name of the topicInfo that should be created.")
	f.StringVar(&a.topicConfig, "topicInfo.config", "", "Direct JSON string or @file.json of topicInfo detail. cf sarama.TopicDetail")
	f.BoolVar(&a.validateOnly, "validate.only", false, "Flag to indicate whether operation should only validate input (supported for topicInfo.create).")
	f.StringVar(&a.topicDelete, "topicInfo.delete", "", "Name of the topicInfo that should be deleted.")

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

The topicInfo details should be passed via a JSON file that represents a sarama.TopicDetail struct.
cf https://godoc.org/github.com/Shopify/sarama#TopicDetail

A simple way to pass a JSON file is to use a tool like https://github.com/fgeller/jsonify and shell's process substition:

kt admin -topicInfo.create morenews -topicInfo.config $(jsonify --NumPartitions 1 --ReplicationFactor 1)`, envBrokers)
