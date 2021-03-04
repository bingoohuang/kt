package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"os/user"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"

	"github.com/Shopify/sarama"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	envAuth         = "KT_AUTH"
	envVersion      = "KT_VERSION"
	envAdminTimeout = "KT_ADMIN_TIMEOUT"
	envBrokers      = "KT_BROKERS"
	envTopic        = "KT_TOPIC"
)

func getKtTopic(topic string) string {
	if topic != "" {
		return topic
	}

	if v := os.Getenv(envTopic); v != "" {
		return v
	}

	failStartup("Topic name is required.")
	return ""
}

var invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

type command interface {
	run(args []string)
}

func parseBrokers(argBrokers string) []string {
	if argBrokers == "" {
		if v := os.Getenv(envBrokers); v != "" {
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

func listenForInterrupt(q chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	sig := <-signals
	log.Printf("received signal %s\n", sig)
	close(q)
}

func kafkaVersion(s string) sarama.KafkaVersion {
	if s == "" {
		s = os.Getenv(envVersion)
	}

	if s == "" {
		return sarama.V2_0_0_0
	}

	v, err := sarama.ParseKafkaVersion(strings.TrimPrefix(s, "v"))
	if err != nil {
		failf(err.Error())
	}

	return v
}

func parseTimeout(s string) *time.Duration {
	if s == "" {
		return nil
	}

	v, err := time.ParseDuration(s)
	if err != nil {
		failf(err.Error())
	}

	return &v
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("failed to close %s err=%v", name, err)
	}
}

type printContext struct {
	output interface{}
	done   chan struct{}
}

func printOut(in <-chan printContext, pretty bool) {
	marshal := json.Marshal

	if pretty && terminal.IsTerminal(syscall.Stdout) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}

	for {
		ctx := <-in
		buf, err := marshal(ctx.output)
		if err != nil {
			failf("failed to marshal output %#v, err=%v", ctx.output, err)
		}

		fmt.Println(string(buf))
		close(ctx.done)
	}
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

func currentUserName() string {
	usr, err := user.Current()
	if err != nil {
		log.Printf("Failed to read current user err %v", err)
		return "unknown"
	}

	return sanitizeUsername(usr.Username)
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

func randomString(length int) string {
	buf := make([]byte, length)
	random.Read(buf)
	return fmt.Sprintf("%x", buf)[:length]
}

type authConfig struct {
	Mode          string `json:"mode"`
	CACert        string `json:"ca-cert"`
	ClientCert    string `json:"client-cert"`
	ClientCertKey string `json:"client-cert-key"`
	SASLUsr       string `json:"sasl-usr"`
	SASLPwd       string `json:"sasl-pwd"`
}

func setupAuth(auth authConfig, sc *sarama.Config) error {
	switch auth.Mode {
	case "":
		return nil
	case "TLS":
		return auth.setupAuthTLS(sc)
	case "TLS-1way":
		return auth.setupAuthTLS1Way(sc)
	case "SASL":
		return auth.setupSASL(sc)
	default:
		return fmt.Errorf("unsupport auth mode: %#v", auth.Mode)
	}
}

func (r authConfig) setupSASL(sc *sarama.Config) error {
	sc.Net.SASL.Enable = true
	sc.Net.SASL.User = r.SASLUsr
	sc.Net.SASL.Password = r.SASLPwd
	return nil
}

func (r authConfig) setupAuthTLS1Way(sc *sarama.Config) error {
	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = &tls.Config{}
	return nil
}

func (r authConfig) setupAuthTLS(sc *sarama.Config) error {
	tlsCfg, err := createTLSConfig(r.CACert, r.ClientCert, r.ClientCertKey)
	if err != nil {
		return err
	}

	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = tlsCfg

	return nil
}

func createTLSConfig(caCert, clientCert, certKey string) (*tls.Config, error) {
	if caCert == "" || clientCert == "" || certKey == "" {
		return nil, fmt.Errorf("a-cert, client-cert and client-key are required")
	}

	caString, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, fmt.Errorf("failed to read ca-cert err=%v", err)
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caString); !ok {
		return nil, fmt.Errorf("unable to add ca-cert at %s to certificate pool", caCert)
	}

	cert, err := tls.LoadX509KeyPair(clientCert, certKey)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{RootCAs: caPool, Certificates: []tls.Certificate{cert}}
	return tlsCfg, nil
}

func readAuthFile(argFN string, envFN string, target *authConfig) {
	if argFN == "" && envFN == "" {
		return
	}

	fn := argFN
	if fn == "" {
		fn = envFN
	}

	byts, err := ioutil.ReadFile(fn)
	if err != nil {
		failf("failed to read auth file err=%v", err)
	}

	if err := json.Unmarshal(byts, target); err != nil {
		failf("failed to unmarshal auth file err=%v", err)
	}
}

type bytesEncoder func(src []byte) string

func parseEncodeBytesFn(encoder string) bytesEncoder {
	switch encoder {
	case "hex":
		return hex.EncodeToString
	case "base64":
		return base64.StdEncoding.EncodeToString
	case "string":
		return func(data []byte) string { return string(data) }
	}

	failStartup(fmt.Sprintf(`bad encoder argument %s, only allow string/hex/base64.`, encoder))
	return nil
}

func encodeBytes(data []byte, encoder bytesEncoder) string {
	if data == nil {
		return ""
	}

	return encoder(data)
}

type stringDecoder func(string) ([]byte, error)

func parseStringDecoder(decoder string) stringDecoder {
	switch decoder {
	case "hex":
		return hex.DecodeString
	case "base64":
		return base64.StdEncoding.DecodeString
	case "string":
		return func(s string) ([]byte, error) { return []byte(s), nil }
	}

	failStartup(fmt.Sprintf(`bad decoder %s, only allow string/hex/base64.`, decoder))
	return nil
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
