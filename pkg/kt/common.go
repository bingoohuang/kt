package kt

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bingoohuang/gg/pkg/man"
	"github.com/bingoohuang/jj"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	EnvAuth         = "KT_AUTH"
	EnvVersion      = "KT_VERSION"
	EnvAdminTimeout = "KT_ADMIN_TIMEOUT"
	EnvBrokers      = "KT_BROKERS"
	EnvTopic        = "KT_TOPIC"
)

type PrintContext struct {
	Output     any
	Done       chan struct{}
	MessageNum int
	ValueSize  int
}

func ParseBrokers(argBrokers string) []string {
	if argBrokers == "" {
		if v := os.Getenv(EnvBrokers); v != "" {
			argBrokers = v
		} else {
			argBrokers = "localhost:9092"
		}
	}

	brokers := strings.Split(argBrokers, ",")
	for i, b := range brokers {
		if !strings.Contains(b, ":") {
			brokers[i] = b + ":9092"
		}
	}

	return brokers
}

func ParseTopic(topic string, required bool) (string, error) {
	if topic != "" {
		return topic, nil
	}

	if v := os.Getenv(EnvTopic); v != "" {
		return v, nil
	}

	if required {
		return "", fmt.Errorf("topic name is required")
	}

	return "", nil
}

func ParseKafkaVersion(s string) (sarama.KafkaVersion, error) {
	if s == "" {
		s = os.Getenv(EnvVersion)
	}

	if s == "" {
		return sarama.V2_0_0_0, nil
	}

	v, err := sarama.ParseKafkaVersion(strings.TrimPrefix(s, "v"))
	if err != nil {
		return v, fmt.Errorf("failed to parse kafka version %s, error %q", s, err)
	}

	return v, nil
}

func PrintOut(in <-chan PrintContext, pretty bool) {
	PrintOutStats(in, pretty, false)
}

func PrintOutStats(in <-chan PrintContext, pretty, stats bool) {
	marshal := json.Marshal

	if pretty && terminal.IsTerminal(syscall.Stdout) {
		marshal = func(i any) ([]byte, error) {
			return json.MarshalIndent(i, "", "  ")
		}
	}

	messageNum := 0
	valueSize := 0
	start := time.Now()
	defer func() {
		cost := time.Since(start)
		fmt.Printf("total messages %d, size %s, cost: %s, TPS: %f message/s %s/s\n",
			messageNum, man.Bytes(uint64(valueSize)), cost,
			float64(messageNum)/cost.Seconds(),
			man.Bytes(uint64(float64(valueSize)/cost.Seconds())))
	}()

	for ctx := range in {
		messageNum += ctx.MessageNum
		valueSize += ctx.ValueSize

		if !stats {
			buf, err := marshal(ctx.Output)
			if err != nil {
				log.Printf("E! marshal Output %#v: %v", ctx.Output, err)
			}

			fmt.Println(string(jj.Color(buf, nil, nil)))
		}
		close(ctx.Done)
	}
}

func LogClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("failed to close %s err=%v", name, err)
	}
}

// FirstNotNil returns the first non-nil string.
func FirstNotNil(a ...*string) string {
	for _, i := range a {
		if i != nil {
			return *i
		}
	}

	return ""
}

// FirstNotNilInt16 returns the first non-nil string.
func FirstNotNilInt16(a ...*int16) int16 {
	for _, i := range a {
		if i != nil {
			return *i
		}
	}

	return 0
}

// FirstNotNilInt32 returns the first non-nil string.
func FirstNotNilInt32(a ...*int32) int32 {
	for _, i := range a {
		if i != nil {
			return *i
		}
	}

	return 0
}

// FirstNotNilMapInt32 returns the first non-nil string.
func FirstNotNilMapInt32(a ...*map[int32][]int32) map[int32][]int32 {
	for _, i := range a {
		if i != nil {
			return *i
		}
	}

	return nil
}

// FirstNotNilMapString returns the first non-nil string.
func FirstNotNilMapString(a ...*map[string]*string) map[string]*string {
	for _, i := range a {
		if i != nil {
			return *i
		}
	}

	return nil
}

func CurrentUserName() string {
	usr, err := user.Current()
	if err != nil {
		log.Printf("Failed to read current user err %v", err)
		return "unknown"
	}

	return sanitizeUsername(usr.Username)
}

var invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}
