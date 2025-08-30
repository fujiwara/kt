package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"

	json "github.com/goccy/go-json"

	"github.com/IBM/sarama"
	"github.com/itchyny/gojq"
	"golang.org/x/term"
)

const (
	// Periodic flush interval for buffered output to pipes/files
	flushInterval = 250 * time.Millisecond
)

var (
	stdoutWriter   io.Writer
	bufferedWriter *bufio.Writer
)

func init() {
	setupOutputBuffering()
}

func setupOutputBuffering() {
	if term.IsTerminal(int(os.Stdout.Fd())) {
		stdoutWriter = os.Stdout
	} else {
		bufferedWriter = bufio.NewWriter(os.Stdout)
		stdoutWriter = bufferedWriter
		// Start periodic flushing for non-tty output
		go periodicFlush()
	}
}

func periodicFlush() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		flushOutput()
	}
}

func flushOutput() {
	if bufferedWriter != nil {
		bufferedWriter.Flush()
	}
}

// parseTimeString parses a time string that can be either:
// - Current time: "now"
// - Absolute time: RFC3339 format (2006-01-02T15:04:05Z07:00)
// - Relative duration: +5m, +1h, +30s (future from current time)
// - Negative relative duration: -5m, -1h, -30s (past from current time)
func parseTimeString(s string) (*time.Time, error) {
	if s == "now" {
		// Current time
		t := time.Now()
		return &t, nil
	}

	if strings.HasPrefix(s, "+") {
		// Relative time in the future
		duration, err := time.ParseDuration(s[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid relative duration %q: %v", s, err)
		}
		t := time.Now().Add(duration)
		return &t, nil
	}

	if strings.HasPrefix(s, "-") {
		// Relative time in the past
		duration, err := time.ParseDuration(s[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid relative duration %q: %v", s, err)
		}
		t := time.Now().Add(-duration) // Subtract duration for past time
		return &t, nil
	}

	// Parse as RFC3339 absolute time
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("invalid time format %q (expected RFC3339): %v", s, err)
	}

	return &t, nil
}

const (
	ENV_AUTH          = "KT_AUTH"
	ENV_ADMIN_TIMEOUT = "KT_ADMIN_TIMEOUT"
	ENV_BROKERS       = "KT_BROKERS"
	ENV_TOPIC         = "KT_TOPIC"
	ENV_KAFKA_VERSION = "KT_KAFKA_VERSION"
)

var invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

type baseCmd struct {
	Pretty          bool     `help:"Control output pretty printing." default:"true" negatable:""`
	Verbose         bool     `help:"More verbose logging to stderr."`
	Jq              string   `help:"Apply jq filter to output (e.g., '.value | fromjson | .field')."`
	Raw             bool     `help:"Output raw strings without JSON encoding (like jq -r)."`
	ProtocolVersion string   `help:"Kafka protocol version" env:"KT_KAFKA_VERSION"`
	Brokers         []string `help:"Comma separated list of brokers. Port defaults to 9092 when omitted." env:"KT_BROKERS" default:"localhost:9092"`
	Auth            string   `help:"Path to auth configuration file, can also be set via KT_AUTH env variable." env:"KT_AUTH"`

	jqQuery *gojq.Query
	version sarama.KafkaVersion
	auth    authConfig
}

func (b *baseCmd) prepare() error {
	var err error
	if b.Jq == "" {
		b.jqQuery = nil
	} else {
		if b.jqQuery, err = gojq.Parse(b.Jq); err != nil {
			return fmt.Errorf("failed to parse jq query %q: %v", b.Jq, err)
		}
	}

	// Parse Kafka version
	b.version, err = chooseKafkaVersion(b.ProtocolVersion)
	if err != nil {
		return fmt.Errorf("failed to read kafka version: %v", err)
	}

	// Read auth configuration
	if err = readAuthFile(b.Auth, os.Getenv(ENV_AUTH), &b.auth); err != nil {
		return err
	}

	return nil
}

func (b *baseCmd) infof(msg string, args ...interface{}) {
	if b.Verbose {
		warnf(msg, args...)
	}
}

// getKafkaVersion returns the parsed Kafka version
func (b *baseCmd) getKafkaVersion() sarama.KafkaVersion {
	return b.version
}

// addDefaultPorts adds default port 9092 to broker addresses if missing
func (b *baseCmd) addDefaultPorts(brokers []string) []string {
	result := make([]string, len(brokers))
	for i, broker := range brokers {
		host, port, err := net.SplitHostPort(broker)
		if err != nil {
			// No port specified, add default port
			result[i] = net.JoinHostPort(broker, "9092")
		} else {
			// Port already specified, keep as is
			result[i] = net.JoinHostPort(host, port)
		}
	}
	return result
}

func warnf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
}

func outf(msg string, args ...interface{}) {
	fmt.Fprintf(stdoutWriter, msg, args...)
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		warnf("failed to close %#v err=%v", name, err)
	}
}

func chooseKafkaVersion(v string, ex ...string) (sarama.KafkaVersion, error) {
	if v == "" {
		return sarama.V3_0_0_0, nil // Default to V3.0.0.0 if no version specified
	}
	return sarama.ParseKafkaVersion(strings.TrimPrefix(v, "v"))
}

type printContext struct {
	output object
	done   chan struct{}
	cmd    baseCmd
}

func print(in <-chan printContext, pretty bool) {
	marshal := json.Marshal
	if pretty && term.IsTerminal(int(syscall.Stdout)) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}
	for {
		ctx := <-in

		// Apply jq filter if specified
		if q := ctx.cmd.jqQuery; q != nil {
			if output, multi, err := applyJqFilter(q, ctx.output); err != nil {
				warnf("failed to apply jq filter: %v\n", err)
				return
			} else if multi {
				for _, item := range output.([]any) {
					if err := printOutput(item, marshal, ctx.cmd.Raw); err != nil {
						warnf("failed to print output: %v\n", err)
						return
					}
				}
			} else {
				if err := printOutput(output, marshal, ctx.cmd.Raw); err != nil {
					warnf("failed to print output: %v\n", err)
					return
				}
			}
		} else {
			if err := printOutput(ctx.output, marshal, ctx.cmd.Raw); err != nil {
				warnf("failed to print output: %v\n", err)
				return
			}
		}
		close(ctx.done)
	}
}

func printOutput(output any, marshal func(any) ([]byte, error), raw bool) error {
	if raw {
		switch output := output.(type) {
		case []byte:
			stdoutWriter.Write(output)
			stdoutWriter.Write([]byte{'\n'})
			return nil
		case string:
			io.WriteString(stdoutWriter, output)
			io.WriteString(stdoutWriter, "\n")
			return nil
		case *string:
			io.WriteString(stdoutWriter, *output)
			io.WriteString(stdoutWriter, "\n")
			return nil
		}
	}
	// Normal JSON output
	buf, err := marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output %#v, err=%v", output, err)
	}
	stdoutWriter.Write(buf)
	stdoutWriter.Write([]byte{'\n'})
	return nil
}

type object interface {
	ToMap() map[string]any
}

type mapObject map[string]any

func (o mapObject) ToMap() map[string]any {
	return map[string]any(o)
}

// ptrToValue converts a pointer to its value if not nil, otherwise returns nil
func ptrToValue[T any](ptr *T) any {
	if ptr != nil {
		return *ptr
	}
	return nil
}

// Apply the filter using the cached compiled query
func applyJqFilter(q *gojq.Query, input object) (any, bool, error) {
	iter := q.Run(input.ToMap())
	var results []any
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return nil, false, fmt.Errorf("jq execution error: %v", err)
		}
		results = append(results, v)
	}
	switch len(results) {
	case 0:
		return nil, false, nil // No results found
	case 1:
		return results[0], false, nil // Single result
	default:
		return results, true, nil // Multiple results, return as slice
	}
}

func quietPrint(in <-chan printContext) {
	for {
		ctx := <-in
		close(ctx.done)
	}
}

func failf(msg string, args ...interface{}) {
	exitf(1, msg, args...)
}

func exitf(code int, msg string, args ...interface{}) {
	if code == 0 {
		outf(msg+"\n", args...)
	} else {
		warnf(msg+"\n", args...)
	}
	flushOutput()
	os.Exit(code)
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

func hashCodePartition(key string, partitions int32) int32 {
	if partitions <= 0 {
		return -1
	}

	return kafkaAbs(hashCode(key)) % partitions
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

type authConfig struct {
	Mode              string `json:"mode"`
	CACert            string `json:"ca-certificate"`
	ClientCert        string `json:"client-certificate"`
	ClientCertKey     string `json:"client-certificate-key"`
	SASLPlainUser     string `json:"sasl_plain_user"`
	SASLPlainPassword string `json:"sasl_plain_password"`
}

func setupAuth(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.Mode == "" {
		return nil
	}

	switch auth.Mode {
	case "TLS":
		return setupAuthTLS(auth, saramaCfg)
	case "TLS-1way":
		return setupAuthTLS1Way(auth, saramaCfg)
	case "SASL":
		return setupSASL(auth, saramaCfg)
	case "SASL_SSL", "TLS-1way-SASL":
		// Setup TLS encryption first
		if err := setupAuthTLS1Way(auth, saramaCfg); err != nil {
			return err
		}
		// Then add SASL authentication
		return setupSASL(auth, saramaCfg)
	default:
		return fmt.Errorf("unsupported auth mode: %#v", auth.Mode)
	}
}

func setupSASL(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = auth.SASLPlainUser
	saramaCfg.Net.SASL.Password = auth.SASLPlainPassword
	return nil
}

func setupAuthTLS1Way(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = &tls.Config{}

	if auth.CACert == "" {
		return nil
	}

	caString, err := os.ReadFile(auth.CACert)
	if err != nil {
		return fmt.Errorf("failed to read ca-certificate err=%v", err)
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		return fmt.Errorf("unable to add ca-certificate at %s to certificate pool", auth.CACert)
	}

	tlsCfg := &tls.Config{RootCAs: caPool}
	saramaCfg.Net.TLS.Config = tlsCfg
	return nil
}

func setupAuthTLS(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.CACert == "" || auth.ClientCert == "" || auth.ClientCertKey == "" {
		return fmt.Errorf("client-certificate, client-certificate-key and ca-certificate are required - got auth=%#v", auth)
	}

	caString, err := os.ReadFile(auth.CACert)
	if err != nil {
		return fmt.Errorf("failed to read ca-certificate err=%v", err)
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		return fmt.Errorf("unable to add ca-certificate at %s to certificate pool", auth.CACert)
	}

	clientCert, err := tls.LoadX509KeyPair(auth.ClientCert, auth.ClientCertKey)
	if err != nil {
		return err
	}

	tlsCfg := &tls.Config{RootCAs: caPool, Certificates: []tls.Certificate{clientCert}}
	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = tlsCfg

	return nil
}

func qualifyPath(argFN string, target *string) {
	if *target != "" && !filepath.IsAbs(*target) && filepath.Dir(*target) == "." {
		*target = filepath.Join(filepath.Dir(argFN), *target)
	}
}

func readAuthFile(argFN string, envFN string, target *authConfig) error {
	if argFN == "" && envFN == "" {
		return nil
	}

	fn := argFN
	if fn == "" {
		fn = envFN
	}

	b, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("failed to read auth file err=%v", err)
	}

	if err := json.Unmarshal(b, target); err != nil {
		return fmt.Errorf("failed to unmarshal auth file err=%v", err)
	}

	qualifyPath(fn, &target.CACert)
	qualifyPath(fn, &target.ClientCert)
	qualifyPath(fn, &target.ClientCertKey)
	return nil
}
