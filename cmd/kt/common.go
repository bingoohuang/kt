package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"
	"unicode/utf16"

	"github.com/Shopify/sarama"
	. "github.com/bingoohuang/kt/pkg/kt"
)

func getKtTopic(v string) string {
	topic, err := ParseTopic(v)
	if err != nil {
		failStartup(err.Error())
	}

	return topic
}

func kafkaVersion(v string) sarama.KafkaVersion {
	version, err := ParseKafkaVersion(v)
	if err != nil {
		failStartup(err.Error())
	}

	return version
}

type command interface {
	run(args []string)
}

func listenForInterrupt(q chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	sig := <-signals
	log.Printf("received signal %s\n", sig)
	close(q)
}

func parseTimeout(timeout string) *time.Duration {
	if timeout != "" {
		return parseDuration(timeout)
	}

	return parseDuration(os.Getenv(EnvAdminTimeout))
}

func parseDuration(s string) *time.Duration {
	if s == "" {
		return nil
	}

	v, err := time.ParseDuration(s)
	if err != nil {
		failf(err.Error())
	}

	return &v
}

func quitf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, msg+"\n", args...)
	os.Exit(0)
}

func failf(msg string, args ...interface{}) {
	log.Printf(msg+"\n", args...)
	os.Exit(1)
}

func readStdinLines(max int, out chan string) {
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, max), max)

	for s.Scan() {
		out <- s.Text()
	}

	if err := s.Err(); err != nil {
		log.Printf("scanning input err=%v\n", err)
	}
	close(out)
}

// hashCode imitates the behavior of the JDK's String#hashCode method.
// https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#hashCode()
//
// As strings are encoded in utf16 on the JVM, this implementation checks wether
// s contains non-bmp runes and uses utf16 surrogate pairs for those.
func hashCode(s string) (hc int32) {
	for _, r := range s {
		r1, r2 := utf16.EncodeRune(r)
		if r1 == 0xfffd && r1 == r2 {
			hc = hc*31 + r
		} else {
			hc = (hc*31+r1)*31 + r2
		}
	}
	return
}

func kafkaAbs(i int32) int32 {
	switch {
	case i == -2147483648: // Integer.MIN_VALUE
		return 0
	case i < 0:
		return i * -1
	default:
		return i
	}
}

var random = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

func randPartition(partitions int32) int32 {
	return random.Int31n(partitions)
}

func hashCodePartition(key string, partitions int32) int32 {
	if partitions <= 0 {
		return -1
	}

	return kafkaAbs(hashCode(key)) % partitions
}

func randomString(length int) string {
	buf := make([]byte, length)
	random.Read(buf)
	return fmt.Sprintf("%x", buf)[:length]
}
