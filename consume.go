package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consumeCmd struct {
	sync.Mutex

	topic                  string
	brokers                []string
	auth                   authConfig
	offsets                map[int32]interval
	timeout                time.Duration
	verbose, pretty        bool
	version                sarama.KafkaVersion
	keyEncoder, valEncoder bytesEncoder
	group                  string

	client        sarama.Client
	consumer      sarama.Consumer
	offsetManager sarama.OffsetManager
	poms          map[int32]sarama.PartitionOffsetManager

	out chan printContext
}

var offsetResume int64 = -3

type offset struct {
	relative bool
	start    int64
	diff     int64
}

func (c *consumeCmd) resolveOffset(o offset, partition int32) (int64, error) {
	if !o.relative {
		return o.start, nil
	}

	var res int64
	var err error

	if o.start == sarama.OffsetNewest || o.start == sarama.OffsetOldest {
		if res, err = c.client.GetOffset(c.topic, partition, o.start); err != nil {
			return 0, err
		}

		if o.start == sarama.OffsetNewest {
			res = res - 1
		}

		return res + o.diff, nil
	} else if o.start == offsetResume {
		if c.group == "" {
			return 0, fmt.Errorf("cannot resume without -group argument")
		}
		pom := c.getPOM(partition)
		next, _ := pom.NextOffset()
		return next, nil
	}

	return o.start + o.diff, nil
}

type interval struct {
	start offset
	end   offset
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
	var err error

	a := c.parseFlags(as)
	c.topic = getKtTopic(a.topic)
	c.timeout = a.timeout
	c.verbose = a.verbose
	c.pretty = a.pretty
	c.version = kafkaVersion(a.version)
	c.group = a.group

	readAuthFile(a.auth, os.Getenv(envAuth), &c.auth)

	c.valEncoder = parseEncodeBytesFn(a.encVal)
	c.keyEncoder = parseEncodeBytesFn(a.encKey)
	c.brokers = parseBrokers(a.brokers)
	if c.offsets, err = parseOffsets(a.offsets); err != nil {
		failStartup(fmt.Sprintf("%s", err))
	}
}

// parseOffsets parses a set of partition-offset specifiers in the following
// syntax. The grammar uses the BNF-like syntax defined in https://golang.org/ref/spec.
//
//	offsets := [ partitionInterval { "," partitionInterval } ]
//
//	partitionInterval :=
//		partition "=" interval |
//		partition |
//		interval
//
//	partition := "all" | number
//
//	interval := [ offset ] [ ":" [ offset ] ]
//
//	offset :=
//		number |
//		namedRelativeOffset |
//		numericRelativeOffset |
//		namedRelativeOffset numericRelativeOffset
//
//	namedRelativeOffset := "newest" | "oldest" | "resume"
//
//	numericRelativeOffset := "+" number | "-" number
//
//	number := {"0"| "1"| "2"| "3"| "4"| "5"| "6"| "7"| "8"| "9"}
func parseOffsets(str string) (map[int32]interval, error) {
	result := map[int32]interval{}
	for _, partitionInfo := range strings.Split(str, ",") {
		partitionInfo = strings.TrimSpace(partitionInfo)
		// There's a grammatical ambiguity between a partition
		// number and an interval, because both allow a single
		// decimal number. We work around that by trying an explicit
		// partition first.
		p, err := parsePartition(partitionInfo)
		if err == nil {
			result[p] = interval{start: oldestOffset(), end: lastOffset()}
			continue
		}
		intervalStr := partitionInfo
		if i := strings.Index(partitionInfo, "="); i >= 0 {
			// There's an explicitly specified partition.
			p, err = parsePartition(partitionInfo[0:i])
			if err != nil {
				return nil, err
			}
			intervalStr = partitionInfo[i+1:]
		} else {
			// No explicit partition, so implicitly use "all".
			p = -1
		}
		intv, err := parseInterval(intervalStr)
		if err != nil {
			return nil, err
		}
		result[p] = intv
	}
	return result, nil
}

// parseRelativeOffset parses a relative offset, such as "oldest", "newest-30", or "+20".
func parseRelativeOffset(s string) (offset, error) {
	o, ok := parseNamedRelativeOffset(s)
	if ok {
		return o, nil
	}
	i := strings.IndexAny(s, "+-")
	if i == -1 {
		return offset{}, fmt.Errorf("invalid offset %q", s)
	}
	switch {
	case i > 0:
		// The + or - isn't at the start, so the relative offset must start
		// with a named relative offset.
		o, ok = parseNamedRelativeOffset(s[0:i])
		if !ok {
			return offset{}, fmt.Errorf("invalid offset %q", s)
		}
	case s[i] == '+':
		// Offset +99 implies oldest+99.
		o = oldestOffset()
	default:
		// Offset -99 implies newest-99.
		o = newestOffset()
	}
	// Note: we include the leading sign when converting to int
	// so the diff ends up with the correct sign.
	diff, err := strconv.ParseInt(s[i:], 10, 64)
	if err != nil {
		if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
			return offset{}, fmt.Errorf("offset %q is too large", s)
		}
		return offset{}, fmt.Errorf("invalid offset %q", s)
	}
	o.diff = diff
	return o, nil
}

func parseNamedRelativeOffset(s string) (offset, bool) {
	switch s {
	case "newest":
		return newestOffset(), true
	case "oldest":
		return oldestOffset(), true
	case "resume":
		return offset{relative: true, start: offsetResume}, true
	default:
		return offset{}, false
	}
}

func parseInterval(s string) (interval, error) {
	if s == "" {
		// An empty string implies all messages.
		return interval{
			start: oldestOffset(),
			end:   lastOffset(),
		}, nil
	}
	var start, end string
	i := strings.Index(s, ":")
	if i == -1 {
		// No colon, so the whole string specifies the start offset.
		start = s
	} else {
		// We've got a colon, so there are explicitly specified
		// start and end offsets.
		start = s[0:i]
		end = s[i+1:]
	}
	startOff, err := parseIntervalPart(start, oldestOffset())
	if err != nil {
		return interval{}, err
	}
	endOff, err := parseIntervalPart(end, lastOffset())
	if err != nil {
		return interval{}, err
	}
	return interval{
		start: startOff,
		end:   endOff,
	}, nil
}

// parseIntervalPart parses one half of an interval pair.
// If s is empty, the given default offset will be used.
func parseIntervalPart(s string, defaultOffset offset) (offset, error) {
	if s == "" {
		return defaultOffset, nil
	}
	n, err := strconv.ParseUint(s, 10, 63)
	if err == nil {
		// It's an explicit numeric offset.
		return offset{
			start: int64(n),
		}, nil
	}
	if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
		return offset{}, fmt.Errorf("offset %q is too large", s)
	}
	o, err := parseRelativeOffset(s)
	if err != nil {
		return offset{}, err
	}
	return o, nil
}

// parsePartition parses a partition number, or the special word "all", meaning all partitions.
func parsePartition(s string) (int32, error) {
	if s == "all" {
		return -1, nil
	}
	p, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
			return 0, fmt.Errorf("partition number %q is too large", s)
		}
		return 0, fmt.Errorf("invalid partition number %q", s)
	}
	return int32(p), nil
}

func oldestOffset() offset { return offset{relative: true, start: sarama.OffsetOldest} }
func newestOffset() offset { return offset{relative: true, start: sarama.OffsetNewest} }
func lastOffset() offset   { return offset{relative: false, start: 1<<63 - 1} }

func (c *consumeCmd) parseFlags(as []string) consumeArgs {
	var a consumeArgs
	f := flag.NewFlagSet("consume", flag.ContinueOnError)
	f.StringVar(&a.topic, "topicInfo", "", "Topic to consume (required).")
	f.StringVar(&a.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	f.StringVar(&a.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env", envAuth))
	f.StringVar(&a.offsets, "offsets", "newest", "Specifies what messages to read by partition and offset range (defaults to newest).")
	f.DurationVar(&a.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	f.BoolVar(&a.verbose, "verbose", false, "More verbose logging to stderr.")
	f.BoolVar(&a.pretty, "pretty", false, "Control output pretty printing.")
	f.StringVar(&a.version, "version", "", fmt.Sprintf("Kafka protocol version, like 0.10.0.0, or env %s", envVersion))
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

func (c *consumeCmd) setupClient() {
	cfg := sarama.NewConfig()
	cfg.Version = c.version
	cfg.ClientID = "kt-consume-" + currentUserName()
	if c.verbose {
		log.Printf("sarama client configuration %#v", cfg)
	}

	if err := setupAuth(c.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	var err error
	if c.client, err = sarama.NewClient(c.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
}

func (c *consumeCmd) run(args []string) {
	c.parseArgs(args)

	if c.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	c.setupClient()
	c.setupOffsetManager()

	var err error
	if c.consumer, err = sarama.NewConsumerFromClient(c.client); err != nil {
		failf("failed to create consumer err=%v", err)
	}
	defer logClose("consumer", c.consumer)

	partitions := c.findPartitions()
	if len(partitions) == 0 {
		failf("Found no partitions to consume")
	}
	defer c.closePOMs()

	c.consume(partitions)
}

func (c *consumeCmd) setupOffsetManager() {
	if c.group == "" {
		return
	}

	var err error
	if c.offsetManager, err = sarama.NewOffsetManagerFromClient(c.group, c.client); err != nil {
		failf("failed to create offsetmanager err=%v", err)
	}
}

func (c *consumeCmd) consume(partitions []int32) {
	c.out = make(chan printContext)

	go printOut(c.out, c.pretty)
	c.consumePartitions(partitions)
}

func (c *consumeCmd) consumePartitions(partitions []int32) {
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(p int32) { defer wg.Done(); c.consumePartition(p) }(p)
	}
	wg.Wait()
}

func (c *consumeCmd) consumePartition(partition int32) {
	offsets, ok := c.offsets[partition]
	if !ok {
		offsets = c.offsets[-1]
	}

	var err error
	var start, end int64
	if start, err = c.resolveOffset(offsets.start, partition); err != nil {
		log.Printf("Failed to read start offset for partition %v err=%v\n", partition, err)
		return
	}

	if end, err = c.resolveOffset(offsets.end, partition); err != nil {
		log.Printf("Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}

	var pc sarama.PartitionConsumer
	if pc, err = c.consumer.ConsumePartition(c.topic, partition, start); err != nil {
		log.Printf("Failed to consume partition %v err=%v\n", partition, err)
		return
	}

	c.partitionLoop(pc, partition, end)
}

type consumedMessage struct {
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

func newConsumedMessage(m *sarama.ConsumerMessage, keyEncoder, valEncoder bytesEncoder) consumedMessage {
	result := consumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       encodeBytes(m.Key, keyEncoder),
		Value:     encodeBytes(m.Value, valEncoder),
	}

	if !m.Timestamp.IsZero() {
		result.Timestamp = &m.Timestamp
	}

	return result
}

func (c *consumeCmd) closePOMs() {
	c.Lock()
	defer c.Unlock()

	for p, pom := range c.poms {
		logClose(fmt.Sprintf("partition offset manager for partition %v", p), pom)
	}
}

func (c *consumeCmd) getPOM(p int32) sarama.PartitionOffsetManager {
	c.Lock()
	defer c.Unlock()

	if c.poms == nil {
		c.poms = map[int32]sarama.PartitionOffsetManager{}
	}

	pom, ok := c.poms[p]
	if ok {
		return pom
	}

	pom, err := c.offsetManager.ManagePartition(c.topic, p)
	if err != nil {
		failf("failed to create partition offset manager err=%v", err)
	}
	c.poms[p] = pom

	return pom
}

func (c *consumeCmd) partitionLoop(pc sarama.PartitionConsumer, p int32, end int64) {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)

	var timer *time.Timer
	timeout := make(<-chan time.Time)

	var pom sarama.PartitionOffsetManager
	if c.group != "" {
		pom = c.getPOM(p)
	}

	for {
		if c.timeout > 0 {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(c.timeout)
			timeout = timer.C
		}

		select {
		case <-timeout:
			log.Printf("consuming from partition %v timed out after %s\n", p, c.timeout)
			return
		case err := <-pc.Errors():
			log.Printf("partition %v consumer encountered err %s", p, err)
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				log.Printf("unexpected closed messages chan")
				return
			}

			m := newConsumedMessage(msg, c.keyEncoder, c.valEncoder)
			ctx := printContext{output: m, done: make(chan struct{})}
			c.out <- ctx
			<-ctx.done

			if pom != nil {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return
			}
		}
	}
}

func (c *consumeCmd) findPartitions() []int32 {
	all, err := c.consumer.Partitions(c.topic)
	if err != nil {
		failf("failed to read partitions for topicInfo %v err=%v", c.topic, err)
	}

	if _, hasDefault := c.offsets[-1]; hasDefault {
		return all
	}

	var res []int32
	for _, p := range all {
		if _, ok := c.offsets[p]; ok {
			res = append(res, p)
		}
	}

	return res
}

var consumeDocString = fmt.Sprintf(`
The values for -topicInfo and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

Offsets can be specified as a comma-separated list of intervals:
  [[partition=start:end],...]

The default is to consume from the oldest offset on every partition for the given topicInfo.
 - partition is the numeric identifier for a partition. You can use "all" to
   specify a default interval for all partitions.
 - start is the included offset where consumption should start.
 - end is the included offset where consumption should end.

The following syntax is supported for each offset:
  (oldest|newest|resume)?(+|-)?(\d+)?
 - "oldest" and "newest" refer to the oldest and newest offsets known for a given partition.
 - "resume" can be used in combination with -group.
 - You can use "+" with a numeric value to skip the given number of messages since the oldest offset. 
   For example, "1=+20" will skip 20 offset value since the oldest offset for partition 1.
 - You can use "-" with a numeric value to refer to only the given number of messages before the newest offset. 
   For example, "1=-10" will refer to the last 10 offset values before the newest offset for partition 1.
 - Relative offsets are based on numeric values and will not take skipped offsets (e.g. due to compaction) into account.
 - Given only a numeric value, it is interpreted as an absolute offset value.

More examples:
 - To consume messages from partition 0 between offsets 10 and 20 (inclusive).
     0=10:20
 - To define an interval for all partitions use -1 as the partition identifier:
     all=2:10
 - You can also override the offsets for a single partition, in this case 2:
     all=1-10,2=5-10
 - To consume from multiple partitions:
     0=4:,2=1:10,6
 - This would consume messages from three partitions:
     - Anything from partition 0 starting at offset 4.
     - Messages between offsets 1 and 10 from partition 2.
     - Anything from partition 6.
 - To start at the latest offset for each partition:
     all=newest:
 - Or shorter:
     newest:
 - To consume the last 10 messages:
     newest-10
 - To skip the first 15 messages starting with the oldest offset:
     oldest+10
 - In both cases you can omit "newest" and "oldest":
     -10
 - and
     +10
`, envTopic, envBrokers)
