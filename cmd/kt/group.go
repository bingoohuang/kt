package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	. "github.com/bingoohuang/kt/pkg/kt"
	"github.com/elliotchance/pie/v2"
	"github.com/samber/lo"
)

type groupCmd struct {
	client       sarama.Client
	filterGroups *regexp.Regexp
	filterTopics *regexp.Regexp
	out          chan PrintContext
	auth         AuthConfig
	group        string
	topic        string
	tags         string
	partitions   []int32
	brokers      []string
	version      sarama.KafkaVersion
	reset        int64
	verbose      bool
	pretty       bool
	offsets      bool
}

const (
	allPartitionsHuman = "all"
	resetNotSpecified  = -23
)

func (c *groupCmd) run(args []string) {
	var err error

	c.parseArgs(args)

	if c.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if c.client, err = sarama.NewClient(c.brokers, c.saramaConfig()); err != nil {
		failf("E! create client: %v", err)
	}

	brokers := c.client.Brokers()
	log.Printf("found %v brokers: %s", len(brokers),
		ColorJSON(pie.Map(brokers, func(b *sarama.Broker) string { return b.Addr() })))

	groups := []string{c.group}
	if c.group == "" {
		groups = pie.Of(c.findGroups(brokers)).Filter(c.filterGroups.MatchString).Result
		groups = lo.Uniq(groups)
		log.Printf("found %d groups: %s", len(groups), ColorJSON(groups))
	}

	topics := []string{c.topic}
	if c.topic == "" {
		topics = pie.Of(c.fetchTopics()).Filter(c.filterTopics.MatchString).Result
		topics = lo.Uniq(topics)
		log.Printf("found %d topics: %s", len(topics), ColorJSON(topics))
	}

	c.out = make(chan PrintContext)
	go PrintOut(c.out, c.pretty)

	if !c.offsets {
		for i, group := range groups {
			ctx := PrintContext{Output: GroupInfo{Group: group}, Done: make(chan struct{})}
			c.out <- ctx
			<-ctx.Done

			if c.verbose {
				log.Printf("%v/%v\n", i+1, len(groups))
			}
		}
		return
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		parts := c.partitions
		if len(parts) == 0 {
			parts = c.fetchPartitions(topic)
			log.Printf("found %d partitions %s for topic %s", len(parts), ColorJSON(parts), topic)
		}
		topicPartitions[topic] = parts
	}

	wg := &sync.WaitGroup{}
	for _, group := range groups {
		for topic, partitions := range topicPartitions {
			wg.Add(1)
			go func(group, topic string, partitions []int32) {
				defer wg.Done()
				c.printGroupTopicOffset(group, topic, partitions)
			}(group, topic, partitions)
		}
	}
	wg.Wait()
}

func (c *groupCmd) printGroupTopicOffset(group, topic string, partitions []int32) {
	target := GroupInfo{Group: group, Topic: topic, Offsets: make([]GroupOffset, 0, len(partitions))}
	results := make(chan GroupOffset)

	wg := &sync.WaitGroup{}
	for _, part := range partitions {
		wg.Add(1)
		go func(part int32) {
			defer wg.Done()
			c.fetchGroupOffset(group, topic, part, results)
		}(part)
	}
	go func() { wg.Wait(); close(results) }()

	for res := range results {
		target.Offsets = append(target.Offsets, res)
	}

	if len(target.Offsets) > 0 {
		sort.Slice(target.Offsets, func(i, j int) bool {
			return target.Offsets[j].Partition > target.Offsets[i].Partition
		})
		ctx := PrintContext{Output: target, Done: make(chan struct{})}
		c.out <- ctx
		<-ctx.Done
	}
}

func (c *groupCmd) resolveOffset(top string, part int32, off int64) int64 {
	resolvedOff, err := c.client.GetOffset(top, part, off)
	if err != nil {
		failf("get Offset for partition=%d: %v", part, err)
	}

	if c.verbose {
		log.Printf("resolved Offset %v for topic=%s partition=%d to %v", off, top, part, resolvedOff)
	}

	return resolvedOff
}

func (c *groupCmd) fetchGroupOffset(group, topic string, part int32, results chan<- GroupOffset) {
	if c.verbose {
		log.Printf("fetching Offset information for group=%v topic=%v partition=%v", group, topic, part)
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, c.client)
	if err != nil {
		failf("create client: %v", err)
	}
	defer LogClose("Offset manager", offsetManager)

	pom, err := offsetManager.ManagePartition(topic, part)
	if err != nil {
		failf("manage partition group=%s topic=%s partition=%d: %v", group, topic, part, err)
	}
	defer LogClose("partition Offset manager", pom)

	specialOffset := c.reset == sarama.OffsetNewest || c.reset == sarama.OffsetOldest

	groupOff, metadata := pom.NextOffset()
	if c.reset >= 0 || specialOffset {
		resolvedOff := c.reset
		if specialOffset {
			resolvedOff = c.resolveOffset(topic, part, c.reset)
		}
		if resolvedOff > groupOff {
			pom.MarkOffset(resolvedOff, "")
		} else {
			pom.ResetOffset(resolvedOff, "")
		}

		groupOff = resolvedOff
	}

	// we haven't reset it, and it wasn't set before - lag depends on client's config
	if specialOffset {
		results <- GroupOffset{Partition: part}
		return
	}

	partOff := c.resolveOffset(topic, part, sarama.OffsetNewest)
	lag := partOff - groupOff

	if groupOff > 0 || strings.Contains(c.tags, "allOffSets") {
		results <- GroupOffset{Partition: part, PartitionOffset: partOff, GroupOffset: groupOff, Lag: lag, Metadata: metadata}
	}
}

func (c *groupCmd) fetchTopics() []string {
	tps, err := c.client.Topics()
	if err != nil {
		failf("read topics: %v", err)
	}
	return tps
}

func (c *groupCmd) fetchPartitions(top string) []int32 {
	ps, err := c.client.Partitions(top)
	if err != nil {
		failf("read partitions for topic=%s: %v", top, err)
	}
	return ps
}

func (c *groupCmd) findGroups(brokers []*sarama.Broker) (groups []string) {
	var wg sync.WaitGroup

	groupCh := make(chan string)
	for _, broker := range brokers {
		wg.Add(1)
		go func(broker *sarama.Broker) {
			defer wg.Done()
			c.findGroupsOnBroker(broker, groupCh)
		}(broker)
	}

	go func() {
		wg.Wait()
		close(groupCh)
	}()

	for group := range groupCh {
		groups = append(groups, group)
	}

	return groups
}

func (c *groupCmd) findGroupsOnBroker(broker *sarama.Broker, groupCh chan string) {
	if err := c.connect(broker); err != nil {
		failf("failed to connect to broker %s err=%q", broker.Addr(), err)
	}

	resp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		failf("failed to list brokers on %s err=%q", broker.Addr(), err)
	}

	if resp.Err != sarama.ErrNoError {
		failf("failed to list brokers on %s err=%v", broker.Addr(), resp.Err)
	}

	for name := range resp.Groups {
		groupCh <- name
	}
}

func (c *groupCmd) connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	if err := broker.Open(c.saramaConfig()); err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}

	return nil
}

func (c *groupCmd) saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = c.version
	cfg.ClientID = "kt-group-" + CurrentUserName()

	if err := c.auth.SetupAuth(cfg); err != nil {
		log.Printf("setupAuth: %v", err)
	}

	return cfg
}

func (c *groupCmd) failStartup(msg string) {
	log.Print(msg)
	failf("use \"kt group -help\" for more information")
}

func (c *groupCmd) parseArgs(as []string) {
	var err error

	a := c.parseFlags(as)
	c.topic = getKtTopic(a.topic, false)
	c.group = a.group
	c.verbose = a.verbose
	c.pretty = a.pretty
	c.offsets = a.offsets
	c.tags = a.tags
	c.version = kafkaVersion(a.version)
	if err := c.auth.ReadConfigFile(a.auth); err != nil {
		failStartup(err.Error())
	}

	switch a.partitions {
	case "", "all":
		c.partitions = []int32{}
	default:
		pss := strings.Split(a.partitions, ",")
		for _, ps := range pss {
			p, err := strconv.ParseInt(ps, 10, 32)
			if err != nil {
				failf("partition id invalid err=%v", err)
			}
			c.partitions = append(c.partitions, int32(p))
		}
	}

	if c.partitions == nil {
		failf(`failed to interpret partitions flag %#v. Should be a comma separated list of partitions or "all".`, a.partitions)
	}

	if c.filterGroups, err = regexp.Compile(a.filterGroups); err != nil {
		failf("groups filter regexp invalid err=%v", err)
	}

	if c.filterTopics, err = regexp.Compile(a.filterTopics); err != nil {
		failf("topics filter regexp invalid err=%v", err)
	}

	if a.reset != "" && (a.topic == "" || a.group == "") {
		failf("group and topic are required to reset offsets.")
	}

	switch a.reset {
	case "newest":
		c.reset = sarama.OffsetNewest
	case "oldest":
		c.reset = sarama.OffsetOldest
	case "":
		// optional flag
		c.reset = resetNotSpecified
	default:
		c.reset, err = strconv.ParseInt(a.reset, 10, 64)
		if err != nil {
			if c.verbose {
				log.Printf("failed to parse set %#v err=%v", a.reset, err)
			}
			c.failStartup(fmt.Sprintf(`set value %#v not valid. either newest, oldest or specific Offset expected.`, a.reset))
		}
	}

	c.brokers = ParseBrokers(a.brokers)
}

type groupArgs struct {
	tags         string
	topic        string
	brokers      string
	auth         string
	partitions   string
	group        string
	filterGroups string
	filterTopics string
	reset        string
	version      string
	verbose      bool
	pretty       bool
	offsets      bool
}

func (c *groupCmd) parseFlags(as []string) groupArgs {
	var a groupArgs
	f := flag.NewFlagSet("group", flag.ContinueOnError)
	f.StringVar(&a.topic, "topic", "", "Topic to consume (required).")
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", EnvAuth))
	f.StringVar(&a.group, "group", "", "Consumer group name.")
	f.StringVar(&a.filterGroups, "filter-groups", "", "Regex to filter groups.")
	f.StringVar(&a.filterTopics, "filter-topics", "", "Regex to filter topics.")
	f.StringVar(&a.reset, "reset", "", "Target Offset to reset for consumer group (newest, oldest, or specific Offset)")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.BoolVar(&a.pretty, "pretty", false, "Control Output pretty printing.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", EnvVersion))
	f.StringVar(&a.partitions, "partitions", allPartitionsHuman, "comma separated list of partitions to limit offsets to, or all")
	f.StringVar(&a.tags, "tags", "", "available tags: allOffSets")
	f.BoolVar(&a.offsets, "offsets", true, "Controls if offsets should be fetched (defaults to true)")

	f.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of group:")
		f.PrintDefaults()
		fmt.Fprint(os.Stderr, groupDocString)
	}

	err := f.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return a
}

var groupDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

The group command can be used to list groups, their offsets and lag and to reset a group's Offset.

When an explicit Offset hasn't been set yet, kt prints out the respective sarama constants, cf. https://godoc.org/github.com/IBM/sarama#pkg-constants

To simply list all groups:

kt group

This is faster when not fetching offsets:

kt group -offsets=false

To filter by regex:

kt group -filter specials

To filter by topic:

kt group -topic fav-topic

To reset a consumer group's Offset:

kt group -reset 23 -topic fav-topic -group specials -partitions 2

To reset a consumer group's Offset for all partitions:

kt group -reset newest -topic fav-topic -group specials -partitions all
`, EnvTopic, EnvBrokers)
