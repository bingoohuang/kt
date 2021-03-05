package main

import (
	"os"
	"reflect"
	"testing"

	. "github.com/bingoohuang/kt/pkg/kt"
)

func TestConsumeParseArgs(t *testing.T) {
	topic := "test-topic"
	givenBroker := "hans:9092"
	brokers := []string{givenBroker}

	os.Setenv(EnvTopic, topic)
	os.Setenv(EnvBrokers, givenBroker)
	target := &consumeCmd{}

	target.parseArgs([]string{})
	if target.conf.Topic != topic ||
		!reflect.DeepEqual(target.conf.Brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}

	// default brokers to localhost:9092
	os.Setenv(EnvTopic, "")
	os.Setenv(EnvBrokers, "")
	brokers = []string{"localhost:9092"}

	target.parseArgs([]string{"-topic", topic})
	if target.conf.Topic != topic ||
		!reflect.DeepEqual(target.conf.Brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}

	// command line arg wins
	os.Setenv(EnvTopic, "BLUBB")
	os.Setenv(EnvBrokers, "BLABB")
	brokers = []string{givenBroker}

	target.parseArgs([]string{"-topic", topic, "-brokers", givenBroker})
	if target.conf.Topic != topic ||
		!reflect.DeepEqual(target.conf.Brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}
}
