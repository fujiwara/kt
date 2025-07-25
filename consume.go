package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type consumeCmd struct {
	sync.Mutex
	baseCmd

	topic       string
	brokers     []string
	auth        authConfig
	offsets     map[int32]interval
	timeout     time.Duration
	version     sarama.KafkaVersion
	encodeValue string
	encodeKey   string
	pretty      bool
	group       string

	client        sarama.Client
	consumer      sarama.Consumer
	consumerGroup sarama.ConsumerGroup
	offsetManager sarama.OffsetManager
	poms          map[int32]sarama.PartitionOffsetManager
	shutdown      chan struct{}
	wg            sync.WaitGroup
}

var offsetResume int64 = -3

func debugf(cmd *consumeCmd, format string, args ...interface{}) {
	if cmd.verbose {
		warnf("DEBUG: "+format, args...)
	}
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	cmd *consumeCmd
	out chan printContext
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	debugf(h.cmd, "ConsumerGroupHandler Setup called, member ID: %s\n", session.MemberID())
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	debugf(h.cmd, "ConsumerGroupHandler Cleanup called, member ID: %s\n", session.MemberID())
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	debugf(h.cmd, "ConsumeClaim starting for partition %v, member ID: %s\n", claim.Partition(), session.MemberID())

	for {
		select {
		case <-h.cmd.shutdown:
			h.cmd.infof("consumer group handler shutting down gracefully for partition %v\n", claim.Partition())
			return nil
		case <-session.Context().Done():
			debugf(h.cmd, "session context cancelled for partition %v, error: %v\n", claim.Partition(), session.Context().Err())
			return nil
		case msg := <-claim.Messages():
			if msg == nil {
				h.cmd.infof("received nil message for partition %v, ending claim\n", claim.Partition())
				return nil
			}

			m := newConsumedMessage(msg, h.cmd.encodeKey, h.cmd.encodeValue)
			ctx := printContext{output: m, done: make(chan struct{})}

			select {
			case h.out <- ctx:
				<-ctx.done
				// Mark message as processed
				session.MarkMessage(msg, "")
			case <-h.cmd.shutdown:
				h.cmd.infof("shutdown during message processing for partition %v\n", claim.Partition())
				return nil
			case <-session.Context().Done():
				h.cmd.infof("session cancelled during message processing for partition %v\n", claim.Partition())
				return nil
			}
		}
	}
}

type offset struct {
	relative bool
	start    int64
	diff     int64
}

func (cmd *consumeCmd) resolveOffset(o offset, partition int32) (int64, error) {
	if !o.relative {
		return o.start, nil
	}

	var (
		res int64
		err error
	)

	if o.start == sarama.OffsetNewest || o.start == sarama.OffsetOldest {
		if res, err = cmd.client.GetOffset(cmd.topic, partition, o.start); err != nil {
			return 0, err
		}

		if o.start == sarama.OffsetNewest {
			res = res - 1
		}

		return res + o.diff, nil
	} else if o.start == offsetResume {
		if cmd.group == "" {
			return 0, fmt.Errorf("cannot resume without -group argument")
		}
		pom := cmd.getPOM(partition)
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
	topic       string
	brokers     string
	auth        string
	timeout     time.Duration
	offsets     string
	verbose     bool
	version     string
	encodeValue string
	encodeKey   string
	pretty      bool
	group       string
}

func (cmd *consumeCmd) failStartup(msg string) {
	warnf(msg)
	failf("use \"kt consume -help\" for more information")
}

func (cmd *consumeCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

	envTopic := os.Getenv(ENV_TOPIC)
	if args.topic == "" {
		if envTopic == "" {
			cmd.failStartup("Topic name is required.")
			return
		}
		args.topic = envTopic
	}
	cmd.topic = args.topic
	cmd.timeout = args.timeout
	cmd.verbose = args.verbose
	cmd.pretty = args.pretty
	cmd.group = args.group

	cmd.version, err = chooseKafkaVersion(args.version, os.Getenv(ENV_KAFKA_VERSION))
	if err != nil {
		failf("failed to read kafka version err=%v", err)
	}

	readAuthFile(args.auth, os.Getenv(ENV_AUTH), &cmd.auth)

	if args.encodeValue != "string" && args.encodeValue != "hex" && args.encodeValue != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported encodevalue argument %#v, only string, hex and base64 are supported.`, args.encodeValue))
		return
	}
	cmd.encodeValue = args.encodeValue

	if args.encodeKey != "string" && args.encodeKey != "hex" && args.encodeKey != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported encodekey argument %#v, only string, hex and base64 are supported.`, args.encodeValue))
		return
	}
	cmd.encodeKey = args.encodeKey

	envBrokers := os.Getenv(ENV_BROKERS)
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}

	cmd.offsets, err = parseOffsets(args.offsets)
	if err != nil {
		cmd.failStartup(fmt.Sprintf("%s", err))
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
			result[p] = interval{
				start: oldestOffset(),
				end:   lastOffset(),
			}
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
	o.diff = int64(diff)
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

// parsePartition parses a partition number, or the special
// word "all", meaning all partitions.
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

func oldestOffset() offset {
	return offset{relative: true, start: sarama.OffsetOldest}
}

func newestOffset() offset {
	return offset{relative: true, start: sarama.OffsetNewest}
}

func lastOffset() offset {
	return offset{relative: false, start: 1<<63 - 1}
}

func (cmd *consumeCmd) parseFlags(as []string) consumeArgs {
	var args consumeArgs
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", ENV_AUTH))
	flags.StringVar(&args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&args.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.encodeValue, "encodevalue", "string", "Present message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.encodeKey, "encodekey", "string", "Present message key as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.group, "group", "", "Consumer group to use for marking offsets. kt will mark offsets if this arg is supplied.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

func (cmd *consumeCmd) setupClient() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)
	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-consume-" + sanitizeUsername(usr.Username)

	// Configure consumer group settings
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Group.Session.Timeout = 10 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Set initial offset based on offsets parameter for consumer groups
	if cmd.group != "" {
		// Check if offsets contain "newest"
		offsetsStr := ""
		for _, interval := range cmd.offsets {
			if interval.start.relative && interval.start.start == sarama.OffsetNewest {
				offsetsStr = "newest"
				break
			}
		}
		if offsetsStr == "newest" {
			cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
		} else {
			cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		}
	}

	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
}

func (cmd *consumeCmd) run(args []string) {
	var err error

	// Initialize shutdown channel
	cmd.shutdown = make(chan struct{})

	// Set up signal handling
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signals
		warnf("received signal %s - triggering shutdown\n", sig)
		close(cmd.shutdown)
	}()

	cmd.parseArgs(args)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.setupClient()

	// Use consumer group if group is specified
	if cmd.group != "" {
		if cmd.consumerGroup, err = sarama.NewConsumerGroupFromClient(cmd.group, cmd.client); err != nil {
			failf("failed to create consumer group err=%v", err)
		}
		defer logClose("consumer group", cmd.consumerGroup)

		cmd.consumeWithGroup()
	} else {
		// Fallback to old behavior for non-group consumers
		cmd.setupOffsetManager()

		if cmd.consumer, err = sarama.NewConsumerFromClient(cmd.client); err != nil {
			failf("failed to create consumer err=%v", err)
		}
		defer logClose("consumer", cmd.consumer)

		partitions := cmd.findPartitions()
		if len(partitions) == 0 {
			failf("Found no partitions to consume")
		}
		defer cmd.closePOMs()

		cmd.consume(partitions)
	}

	// Wait for all goroutines to finish
	cmd.wg.Wait()
}

func (cmd *consumeCmd) consumeWithGroup() {
	ctx, cancel := context.WithCancel(context.Background())

	debugf(cmd, "created context for consumer group\n")

	out := make(chan printContext)
	go print(out, cmd.pretty)

	handler := &ConsumerGroupHandler{
		cmd: cmd,
		out: out,
	}

	cmd.wg.Add(1)
	go func() {
		defer cmd.wg.Done()
		defer cancel() // Move cancel here so it's only called when goroutine exits
		topics := []string{cmd.topic}

		debugf(cmd, "starting consumer group goroutine\n")

		for {
			debugf(cmd, "about to call consumerGroup.Consume, context state: %v\n", ctx.Err())

			// This blocks until an error occurs or context is cancelled
			if err := cmd.consumerGroup.Consume(ctx, topics, handler); err != nil {
				debugf(cmd, "consumerGroup.Consume returned error: %v, context state: %v\n", err, ctx.Err())

				if err == sarama.ErrClosedConsumerGroup {
					warnf("consumer group closed\n")
					return
				}
				if ctx.Err() != nil {
					warnf("context was cancelled during consume: %v, shutting down consumer group\n", ctx.Err())
					return
				}
				warnf("consumer group error: %v\n", err)
				// Don't return immediately on error, retry with backoff
				time.Sleep(time.Second)
				continue
			}

			debugf(cmd, "consumerGroup.Consume returned without error, context state: %v\n", ctx.Err())

			// If Consume returns without error, check if context was cancelled
			if ctx.Err() != nil {
				warnf("context cancelled after successful consume: %v, consumer group shutting down\n", ctx.Err())
				return
			}

			// If we reach here and context is still valid, it means rebalancing occurred
			warnf("consumer group rebalancing occurred, continuing...\n")
		}
	}()

	// Handle shutdown signal
	go func() {
		debugf(cmd, "shutdown handler goroutine started\n")
		<-cmd.shutdown
		debugf(cmd, "shutdown signal received, cancelling context\n")
		cancel()
	}()
}

func (cmd *consumeCmd) setupOffsetManager() {
	if cmd.group == "" {
		return
	}

	var err error
	if cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.group, cmd.client); err != nil {
		failf("failed to create offsetmanager err=%v", err)
	}
}

func (cmd *consumeCmd) consume(partitions []int32) {
	var (
		out = make(chan printContext)
	)

	go print(out, cmd.pretty)

	cmd.wg.Add(len(partitions))
	for _, p := range partitions {
		go func(p int32) {
			defer cmd.wg.Done()
			cmd.consumePartition(out, p)
		}(p)
	}
}

func (cmd *consumeCmd) consumePartition(out chan printContext, partition int32) {
	var (
		offsets interval
		err     error
		pcon    sarama.PartitionConsumer
		start   int64
		end     int64
		ok      bool
	)

	if offsets, ok = cmd.offsets[partition]; !ok {
		offsets, ok = cmd.offsets[-1]
	}

	if start, err = cmd.resolveOffset(offsets.start, partition); err != nil {
		warnf("Failed to read start offset for partition %v err=%v\n", partition, err)
		return
	}

	if end, err = cmd.resolveOffset(offsets.end, partition); err != nil {
		warnf("Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}

	if pcon, err = cmd.consumer.ConsumePartition(cmd.topic, partition, start); err != nil {
		warnf("Failed to consume partition %v err=%v\n", partition, err)
		return
	}

	cmd.partitionLoop(out, pcon, partition, end)
}

type consumedMessage struct {
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
	Key       *string    `json:"key"`
	Value     *string    `json:"value"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

func newConsumedMessage(m *sarama.ConsumerMessage, encodeKey, encodeValue string) consumedMessage {
	result := consumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       encodeBytes(m.Key, encodeKey),
		Value:     encodeBytes(m.Value, encodeValue),
	}

	if !m.Timestamp.IsZero() {
		result.Timestamp = &m.Timestamp
	}

	return result
}

func encodeBytes(data []byte, encoding string) *string {
	if data == nil {
		return nil
	}

	var str string
	switch encoding {
	case "hex":
		str = hex.EncodeToString(data)
	case "base64":
		str = base64.StdEncoding.EncodeToString(data)
	default:
		str = string(data)
	}

	return &str
}

func (cmd *consumeCmd) closePOMs() {
	cmd.Lock()
	for p, pom := range cmd.poms {
		if err := pom.Close(); err != nil {
			warnf("failed to close partition offset manager for partition %v err=%v\n", p, err)
		}
	}
	cmd.Unlock()
}

func (cmd *consumeCmd) getPOM(p int32) sarama.PartitionOffsetManager {
	cmd.Lock()
	if cmd.poms == nil {
		cmd.poms = map[int32]sarama.PartitionOffsetManager{}
	}
	pom, ok := cmd.poms[p]
	if ok {
		cmd.Unlock()
		return pom
	}

	pom, err := cmd.offsetManager.ManagePartition(cmd.topic, p)
	if err != nil {
		cmd.Unlock()
		failf("failed to create partition offset manager err=%v", err)
	}
	cmd.poms[p] = pom
	cmd.Unlock()
	return pom
}

func (cmd *consumeCmd) partitionLoop(out chan printContext, pc sarama.PartitionConsumer, p int32, end int64) {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)
	var (
		timer   *time.Timer
		pom     sarama.PartitionOffsetManager
		timeout = make(<-chan time.Time)
	)

	if cmd.group != "" {
		pom = cmd.getPOM(p)
	}

	for {
		if cmd.timeout > 0 {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(cmd.timeout)
			timeout = timer.C
		}

		select {
		case <-cmd.shutdown:
			warnf("partition %v consumer shutting down gracefully\n", p)
			if cmd.group != "" && pom != nil {
				if err := pom.Close(); err != nil {
					warnf("failed to close partition offset manager for partition %v err=%v\n", p, err)
				}
			}
			return
		case <-timeout:
			warnf("consuming from partition %v timed out after %s\n", p, cmd.timeout)
			return
		case err := <-pc.Errors():
			warnf("partition %v consumer encountered err %s\n", p, err)
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				warnf("unexpected closed messages chan\n")
				return
			}

			m := newConsumedMessage(msg, cmd.encodeKey, cmd.encodeValue)
			ctx := printContext{output: m, done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			if cmd.group != "" {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return
			}
		}
	}
}

func (cmd *consumeCmd) findPartitions() []int32 {
	var (
		all []int32
		res []int32
		err error
	)
	if all, err = cmd.consumer.Partitions(cmd.topic); err != nil {
		failf("failed to read partitions for topic %v err=%v", cmd.topic, err)
	}

	if _, hasDefault := cmd.offsets[-1]; hasDefault {
		return all
	}

	for _, p := range all {
		if _, ok := cmd.offsets[p]; ok {
			res = append(res, p)
		}
	}

	return res
}

var consumeDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

Offsets can be specified as a comma-separated list of intervals:

  [[partition=start:end],...]

The default is to consume from the oldest offset on every partition for the given topic.

 - partition is the numeric identifier for a partition. You can use "all" to
   specify a default interval for all partitions.

 - start is the included offset where consumption should start.

 - end is the included offset where consumption should end.

The following syntax is supported for each offset:

  (oldest|newest|resume)?(+|-)?(\d+)?

 - "oldest" and "newest" refer to the oldest and newest offsets known for a
   given partition.

 - "resume" can be used in combination with -group.

 - You can use "+" with a numeric value to skip the given number of messages
   since the oldest offset. For example, "1=+20" will skip 20 offset value since
   the oldest offset for partition 1.

 - You can use "-" with a numeric value to refer to only the given number of
   messages before the newest offset. For example, "1=-10" will refer to the
   last 10 offset values before the newest offset for partition 1.

 - Relative offsets are based on numeric values and will not take skipped
   offsets (e.g. due to compaction) into account.

 - Given only a numeric value, it is interpreted as an absolute offset value.

More examples:

To consume messages from partition 0 between offsets 10 and 20 (inclusive).

  0=10:20

To define an interval for all partitions use -1 as the partition identifier:

  all=2:10

You can also override the offsets for a single partition, in this case 2:

  all=1-10,2=5-10

To consume from multiple partitions:

  0=4:,2=1:10,6

This would consume messages from three partitions:

  - Anything from partition 0 starting at offset 4.
  - Messages between offsets 1 and 10 from partition 2.
  - Anything from partition 6.

To start at the latest offset for each partition:

  all=newest:

Or shorter:

  newest:

To consume the last 10 messages:

  newest-10:

To skip the first 15 messages starting with the oldest offset:

  oldest+10:

In both cases you can omit "newest" and "oldest":

  -10:

and

  +10:

Will achieve the same as the two examples above.

`, ENV_TOPIC, ENV_BROKERS)
