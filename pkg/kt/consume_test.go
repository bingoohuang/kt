package kt

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestFindPartitionsToConsume(t *testing.T) {
	data := []struct {
		topic    string
		offsets  map[int32]OffsetInterval
		consumer tConsumer
		expected []int32
	}{
		{
			topic: "a",
			offsets: map[int32]OffsetInterval{
				10: {Start: Offset{Start: 2}, End: Offset{Start: 4}},
			},
			consumer: tConsumer{
				topics:              []string{"a"},
				topicsErr:           nil,
				partitions:          map[string][]int32{"a": {0, 10}},
				partitionsErr:       map[string]error{"a": nil},
				consumePartition:    map[tConsumePartition]tPartitionConsumer{},
				consumePartitionErr: map[tConsumePartition]error{},
				closeErr:            nil,
			},
			expected: []int32{10},
		},
		{
			topic: "a",
			offsets: map[int32]OffsetInterval{
				-1: {Start: Offset{Start: 3}, End: Offset{Start: 41}},
			},
			consumer: tConsumer{
				topics:              []string{"a"},
				topicsErr:           nil,
				partitions:          map[string][]int32{"a": {0, 10}},
				partitionsErr:       map[string]error{"a": nil},
				consumePartition:    map[tConsumePartition]tPartitionConsumer{},
				consumePartitionErr: map[tConsumePartition]error{},
				closeErr:            nil,
			},
			expected: []int32{0, 10},
		},
	}

	for _, d := range data {
		target := &Consumer{
			SaramaConsumer: d.consumer,
			Topic:          d.topic,
			Offsets:        d.offsets,
		}
		actual, _ := target.findPartitions()

		if !reflect.DeepEqual(actual, d.expected) {
			t.Errorf(
				`
Expected: %#v
Actual:   %#v
Input:    topic=%#v offsets=%#v
	`,
				d.expected,
				actual,
				d.topic,
				d.offsets,
			)
			return
		}
	}
}

func TestConsume(t *testing.T) {
	closer := make(chan struct{})
	messageChan := make(<-chan *sarama.ConsumerMessage)
	calls := make(chan tConsumePartition)
	consumer := tConsumer{
		consumePartition: map[tConsumePartition]tPartitionConsumer{
			{"hans", 1, 1}: {messages: messageChan},
			{"hans", 2, 1}: {messages: messageChan},
		},
		calls: calls,
	}
	partitions := []int32{1, 2}
	target := &Consumer{
		SaramaConsumer: consumer,
	}
	target.Topic = "hans"

	target.Client, _ = ConsumerConfig{Brokers: []string{"localhost:9092"}}.SetupClient()
	target.Offsets = map[int32]OffsetInterval{
		-1: {Start: Offset{Start: 1}, End: Offset{Start: 5}},
	}

	go target.consume(partitions)
	defer close(closer)

	end := make(chan struct{})
	go func(c chan tConsumePartition, e chan struct{}) {
		var actual []tConsumePartition
		expected := []tConsumePartition{
			{"hans", 1, 1},
			{"hans", 2, 1},
		}
		for {
			select {
			case call := <-c:
				actual = append(actual, call)
				sort.Sort(ByPartitionOffset(actual))
				if reflect.DeepEqual(actual, expected) {
					e <- struct{}{}
					return
				}
				if len(actual) == len(expected) {
					t.Errorf(
						`Got expected number of calls, but they are different.
Expected: %#v
Actual:   %#v
`,
						expected,
						actual,
					)
				}
			case _, ok := <-e:
				if !ok {
					return
				}
			}
		}
	}(calls, end)

	select {
	case <-end:
	case <-time.After(1 * time.Second):
		t.Errorf("Did not receive calls to consume partitions before timeout.")
		close(end)
	}
}

type tConsumePartition struct {
	topic     string
	partition int32
	offset    int64
}

type ByPartitionOffset []tConsumePartition

func (a ByPartitionOffset) Len() int {
	return len(a)
}

func (a ByPartitionOffset) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByPartitionOffset) Less(i, j int) bool {
	return a[i].partition < a[j].partition || a[i].offset < a[j].offset
}

type tPartitionConsumer struct {
	closeErr            error
	highWaterMarkOffset int64
	messages            <-chan *sarama.ConsumerMessage
	errors              <-chan *sarama.ConsumerError
}

func (pc tPartitionConsumer) Pause() {
}

func (pc tPartitionConsumer) Resume() {
}

func (pc tPartitionConsumer) IsPaused() bool {
	return false
}

func (pc tPartitionConsumer) AsyncClose() {}
func (pc tPartitionConsumer) Close() error {
	return pc.closeErr
}

func (pc tPartitionConsumer) HighWaterMarkOffset() int64 {
	return pc.highWaterMarkOffset
}

func (pc tPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

func (pc tPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

type tConsumer struct {
	topics              []string
	topicsErr           error
	partitions          map[string][]int32
	partitionsErr       map[string]error
	consumePartition    map[tConsumePartition]tPartitionConsumer
	consumePartitionErr map[tConsumePartition]error
	closeErr            error
	calls               chan tConsumePartition
}

func (c tConsumer) Pause(topicPartitions map[string][]int32) {
}

func (c tConsumer) Resume(topicPartitions map[string][]int32) {
}

func (c tConsumer) PauseAll() {
	// TODO implement me
	panic("implement me")
}

func (c tConsumer) ResumeAll() {
	// TODO implement me
	panic("implement me")
}

func (c tConsumer) Topics() ([]string, error) {
	return c.topics, c.topicsErr
}

func (c tConsumer) Partitions(topic string) ([]int32, error) {
	return c.partitions[topic], c.partitionsErr[topic]
}

func (c tConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cp := tConsumePartition{topic, partition, offset}
	c.calls <- cp
	return c.consumePartition[cp], c.consumePartitionErr[cp]
}

func (c tConsumer) Close() error {
	return c.closeErr
}

func (c tConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}
