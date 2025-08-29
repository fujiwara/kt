package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
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

	Topic       string        `help:"Topic to consume." env:"KT_TOPIC" required:""`
	Offsets     string        `help:"Specifies what messages to read by partition and offset range." default:""`
	Timeout     time.Duration `help:"Timeout after not reading messages." default:"0"`
	Until       string        `help:"Stop consuming when message timestamp reaches this time." default:""`
	EncodeValue string        `help:"Present message value as (string|hex|base64)." default:"string" enum:"string,hex,base64"`
	EncodeKey   string        `help:"Present message key as (string|hex|base64)." default:"string" enum:"string,hex,base64"`
	Group       string        `help:"Consumer group to use for marking offsets." default:""`

	offsets       map[int32]interval
	until         *time.Time
	client        sarama.Client
	consumer      sarama.Consumer
	consumerGroup sarama.ConsumerGroup
	offsetManager sarama.OffsetManager
	poms          map[int32]sarama.PartitionOffsetManager
	shutdown      chan struct{}
	untilReached  chan struct{} // Signal when until time is reached
	wg            sync.WaitGroup
}

var offsetResume int64 = -3

const maxInt64 = 1<<63 - 1

func debugf(cmd *consumeCmd, format string, args ...interface{}) {
	if cmd.Verbose {
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
	highWaterMark := claim.HighWaterMarkOffset()

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

			// Check if message timestamp exceeds until time
			if h.cmd.until != nil && !msg.Timestamp.IsZero() && msg.Timestamp.After(*h.cmd.until) {
				h.cmd.infof("consumer group handler reached until time %v (message timestamp: %v) for partition %v\n", h.cmd.until.Format(time.RFC3339), msg.Timestamp.Format(time.RFC3339), claim.Partition())
				// Signal that until time was reached
				select {
				case h.cmd.untilReached <- struct{}{}:
				default:
				}
				return nil
			}

			m := newConsumedMessage(msg, h.cmd.EncodeKey, h.cmd.EncodeValue)
			ctx := printContext{output: m, done: make(chan struct{}), cmd: h.cmd.baseCmd}

			select {
			case h.out <- ctx:
				<-ctx.done
				// Mark message as processed
				session.MarkMessage(msg, "")

				// Check if we've reached the high water mark and until time is set
				if h.cmd.until != nil && msg.Offset >= highWaterMark-1 {
					// We've consumed all available messages, check if we should stop due to until time
					if time.Now().After(*h.cmd.until) {
						h.cmd.infof("consumer group handler reached end of available messages with until time %v for partition %v\n", h.cmd.until.Format(time.RFC3339), claim.Partition())
						// Signal that until time was reached
						select {
						case h.cmd.untilReached <- struct{}{}:
						default:
						}
						return nil
					}
				}
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
	relative  bool
	start     int64
	diff      int64
	timestamp bool // true if start represents a timestamp in milliseconds that needs resolution
}

func (cmd *consumeCmd) resolveOffset(o offset, partition int32) (int64, error) {
	if !o.relative {
		debugf(cmd, "Offset is absolute: %d for partition %d\n", o.start, partition)
		return o.start, nil
	}

	// Handle timestamp-based offsets
	if o.timestamp {
		debugf(cmd, "Resolving timestamp-based offset: %s for partition %d\n", time.UnixMilli(o.start).Format(time.RFC3339), partition)
		res, err := cmd.resolveTimestampOffset(o.start, partition)
		if err != nil {
			return 0, err
		}
		if o.diff != 0 {
			debugf(cmd, "Applying diff %+d to timestamp offset %d, result: %d\n", o.diff, res, res+o.diff)
		}
		return res + o.diff, nil
	}

	var (
		res int64
		err error
	)

	if o.start == sarama.OffsetNewest || o.start == sarama.OffsetOldest {
		baseStr := "oldest"
		if o.start == sarama.OffsetNewest {
			baseStr = "newest"
		}
		debugf(cmd, "Resolving %s offset for partition %d\n", baseStr, partition)
		if res, err = cmd.client.GetOffset(cmd.Topic, partition, o.start); err != nil {
			return 0, err
		}

		if o.start == sarama.OffsetNewest {
			res = res - 1
			debugf(cmd, "Adjusted newest offset from %d to %d (newest-1)\n", res+1, res)
		}

		result := res + o.diff
		if o.diff != 0 {
			debugf(cmd, "Calculated offset: %d (%s=%d, diff=%+d)\n", result, baseStr, res, o.diff)
		} else {
			debugf(cmd, "Resolved %s offset to %d for partition %d\n", baseStr, result, partition)
		}
		return result, nil
	} else if o.start == offsetResume {
		if cmd.Group == "" {
			return 0, fmt.Errorf("cannot resume without -group argument")
		}
		debugf(cmd, "Resolving 'resume' offset for group %s, partition %d\n", cmd.Group, partition)
		pom := cmd.getPOM(partition)
		next, _ := pom.NextOffset()
		result := next + o.diff
		if o.diff != 0 {
			debugf(cmd, "Resume offset with diff: %d (committed=%d, diff=%+d) for group %s, partition %d\n", result, next, o.diff, cmd.Group, partition)
		} else {
			debugf(cmd, "Resume offset: %d for group %s, partition %d\n", next, cmd.Group, partition)
		}
		return result, nil
	}

	result := o.start + o.diff
	if o.diff != 0 {
		debugf(cmd, "Calculated offset: %d (base=%d, diff=%+d) for partition %d\n", result, o.start, o.diff, partition)
	} else {
		debugf(cmd, "Offset: %d for partition %d\n", result, partition)
	}
	return result, nil
}

// resolveTimestampOffset resolves a timestamp in milliseconds to the closest Kafka offset
func (cmd *consumeCmd) resolveTimestampOffset(timestampMs int64, partition int32) (int64, error) {
	debugf(cmd, "Requesting offset for timestamp %s from broker for partition %d\n", time.UnixMilli(timestampMs).Format(time.RFC3339), partition)
	// Create an offset request to find the offset for the given timestamp
	offsetRequest := &sarama.OffsetRequest{
		Version: 1, // Version 1 supports timestamp-based offset lookup
	}

	// Add the partition and timestamp to the request
	offsetRequest.AddBlock(cmd.Topic, partition, timestampMs, 1)

	// Get a broker to send the request to
	broker, err := cmd.client.Leader(cmd.Topic, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get leader broker for topic %s partition %d: %v", cmd.Topic, partition, err)
	}

	// Send the offset request
	offsetResponse, err := broker.GetAvailableOffsets(offsetRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset for timestamp %d: %v", timestampMs, err)
	}

	// Extract the offset from the response
	topicOffsets, ok := offsetResponse.Blocks[cmd.Topic]
	if !ok {
		return 0, fmt.Errorf("no offset response for topic %s", cmd.Topic)
	}

	partitionOffset, ok := topicOffsets[partition]
	if !ok {
		return 0, fmt.Errorf("no offset response for partition %d", partition)
	}

	if partitionOffset.Err != sarama.ErrNoError {
		return 0, fmt.Errorf("error in offset response: %v", partitionOffset.Err)
	}

	// If the offset is -1, it means there are no messages at or after the timestamp
	if partitionOffset.Offset == -1 {
		debugf(cmd, "No messages found at or after timestamp %s for partition %d, using newest offset\n", time.UnixMilli(timestampMs).Format(time.RFC3339), partition)
		// Return the newest offset (next position to be written)
		// This behavior ensures that when starting from a timestamp in the future or
		// after all existing messages, we position at the end and wait for new messages
		return cmd.client.GetOffset(cmd.Topic, partition, sarama.OffsetNewest)
	}

	return partitionOffset.Offset, nil
}

type interval struct {
	start offset
	end   offset
}

func (cmd *consumeCmd) prepare() error {
	if err := cmd.baseCmd.prepare(); err != nil {
		return fmt.Errorf("failed to prepare jq query err=%v", err)
	}

	var err error
	cmd.version, err = chooseKafkaVersion(cmd.ProtocolVersion, os.Getenv(ENV_KAFKA_VERSION))
	if err != nil {
		return fmt.Errorf("failed to read kafka version err=%v", err)
	}

	if cmd.Until != "" {
		cmd.until, err = parseUntilTime(cmd.Until)
		if err != nil {
			return fmt.Errorf("failed to parse until time: %v", err)
		}
		debugf(cmd, "Parsed --until parameter %q as %s\n", cmd.Until, cmd.until.Format(time.RFC3339))
	}

	if err = readAuthFile(cmd.Auth, os.Getenv(ENV_AUTH), &cmd.auth); err != nil {
		return err
	}

	cmd.offsets, err = parseOffsets(cmd.Offsets)
	if err != nil {
		return fmt.Errorf("%s", err)
	}

	// Log parsed offsets when verbose
	if cmd.Offsets != "" {
		debugf(cmd, "Parsed --offsets parameter: %q\n", cmd.Offsets)
		for partition, interval := range cmd.offsets {
			partitionStr := fmt.Sprintf("%d", partition)
			if partition == -1 {
				partitionStr = "all"
			}
			debugf(cmd, "  Partition %s: start=%s, end=%s\n", partitionStr, describeOffset(interval.start), describeOffset(interval.end))
		}
	}

	return nil
}

// describeOffset returns a human-readable description of an offset
func describeOffset(o offset) string {
	if o.timestamp {
		// Always show the actual timestamp, don't try to guess if it was "now"
		return fmt.Sprintf("timestamp(%s)", time.UnixMilli(o.start).Format(time.RFC3339))
	}

	if !o.relative {
		if o.start == maxInt64 {
			return "last"
		}
		return fmt.Sprintf("%d", o.start)
	}

	var base string
	switch o.start {
	case sarama.OffsetOldest:
		base = "oldest"
	case sarama.OffsetNewest:
		base = "newest"
	case offsetResume:
		base = "resume"
	default:
		base = fmt.Sprintf("%d", o.start)
	}

	if o.diff != 0 {
		if o.diff > 0 {
			return fmt.Sprintf("%s+%d", base, o.diff)
		}
		return fmt.Sprintf("%s%d", base, o.diff)
	}
	return base
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

// parseRelativeOffset parses a relative offset, such as "oldest", "newest-30", "+20", or time strings.
func parseRelativeOffset(s string) (offset, error) {
	o, ok := parseNamedRelativeOffset(s)
	if ok {
		return o, nil
	}

	// Check for time string first, before checking for +/- operators
	// This handles RFC3339 times and relative durations
	// Only try parsing as time if it looks like RFC3339 (contains T) or has valid duration syntax
	if strings.Contains(s, "T") && strings.Contains(s, ":") {
		// RFC3339 timestamp
		if t, err := parseTimeString(s); err == nil {
			return offset{relative: true, start: t.UnixMilli(), timestamp: true}, nil
		}
	} else if (strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-")) && (strings.Contains(s, "h") || strings.Contains(s, "m") || strings.Contains(s, "s")) {
		// Relative duration like +1h, -30m, +5s
		if t, err := parseTimeString(s); err == nil {
			return offset{relative: true, start: t.UnixMilli(), timestamp: true}, nil
		}
	}

	i := strings.IndexAny(s, "+-")
	if i == -1 {
		// If no +/- and not a time string, it's invalid
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
	case "now":
		// "now" means current time, which will be resolved to timestamp-based offset
		return offset{relative: true, start: time.Now().UnixMilli(), timestamp: true}, true
	default:
		return offset{}, false
	}
}

// parseUntilTime parses a time string for the until flag
func parseUntilTime(s string) (*time.Time, error) {
	return parseTimeString(s)
}

func parseInterval(s string) (interval, error) {
	if s == "" {
		// An empty string implies all messages.
		return interval{
			start: oldestOffset(),
			end:   lastOffset(),
		}, nil
	}

	// Check if the entire string is an RFC3339 timestamp before splitting on colons
	if strings.Contains(s, "T") && strings.Contains(s, ":") {
		if _, err := parseTimeString(s); err == nil {
			// It's a valid timestamp, treat as start offset only
			startOff, err := parseIntervalPart(s, oldestOffset())
			if err != nil {
				return interval{}, err
			}
			return interval{
				start: startOff,
				end:   lastOffset(),
			}, nil
		}
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
	return offset{relative: false, start: maxInt64}
}

func (cmd *consumeCmd) setupClient() error {
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
	if cmd.Group != "" {
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
		return fmt.Errorf("failed to setup auth err=%v", err)
	}

	if cmd.client, err = sarama.NewClient(cmd.Brokers, cfg); err != nil {
		return fmt.Errorf("failed to create client err=%v", err)
	}
	return nil
}

func (cmd *consumeCmd) run() error {
	if err := cmd.prepare(); err != nil {
		return err
	}
	if cmd.Verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	var err error

	// Initialize channels
	cmd.shutdown = make(chan struct{})
	cmd.untilReached = make(chan struct{}, 1) // Buffered to avoid blocking

	// Set up signal handling
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signals
		warnf("received signal %s - triggering shutdown\n", sig)
		close(cmd.shutdown)
	}()

	if err := cmd.setupClient(); err != nil {
		return err
	}

	// Use consumer group if group is specified
	if cmd.Group != "" {
		if cmd.consumerGroup, err = sarama.NewConsumerGroupFromClient(cmd.Group, cmd.client); err != nil {
			return fmt.Errorf("failed to create consumer group err=%v", err)
		}
		defer logClose("consumer group", cmd.consumerGroup)

		cmd.consumeWithGroup()
	} else {
		// Fallback to old behavior for non-group consumers
		if err = cmd.setupOffsetManager(); err != nil {
			return err
		}

		if cmd.consumer, err = sarama.NewConsumerFromClient(cmd.client); err != nil {
			return fmt.Errorf("failed to create consumer err=%v", err)
		}
		defer logClose("consumer", cmd.consumer)

		partitions := cmd.findPartitions()
		if len(partitions) == 0 {
			return fmt.Errorf("Found no partitions to consume")
		}
		defer cmd.closePOMs()

		cmd.consume(partitions)
	}

	// Wait for all goroutines to finish
	cmd.wg.Wait()
	return nil
}

func (cmd *consumeCmd) consumeWithGroup() {
	ctx, cancel := context.WithCancel(context.Background())

	debugf(cmd, "created context for consumer group\n")

	out := make(chan printContext)
	go print(out, cmd.Pretty)

	handler := &ConsumerGroupHandler{
		cmd: cmd,
		out: out,
	}

	cmd.wg.Add(1)
	go func() {
		defer cmd.wg.Done()
		defer cancel() // Move cancel here so it's only called when goroutine exits
		topics := []string{cmd.Topic}

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

	// Handle shutdown and until-reached signals
	go func() {
		debugf(cmd, "shutdown handler goroutine started\n")
		select {
		case <-cmd.shutdown:
			debugf(cmd, "shutdown signal received, cancelling context\n")
			cancel()
		case <-cmd.untilReached:
			debugf(cmd, "until time reached, cancelling context\n")
			cancel()
		}
	}()

	// Start timeout handler if timeout is specified
	if cmd.Timeout > 0 {
		go func() {
			debugf(cmd, "timeout handler started with timeout %s\n", cmd.Timeout)
			timer := time.NewTimer(cmd.Timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				debugf(cmd, "timeout reached, cancelling context\n")
				cancel()
			case <-ctx.Done():
				// Context already cancelled, exit
			}
		}()
	}

	// Wait for consumer group to finish
	cmd.wg.Wait()
}

func (cmd *consumeCmd) setupOffsetManager() error {
	if cmd.Group == "" {
		return nil
	}

	var err error
	if cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.Group, cmd.client); err != nil {
		return fmt.Errorf("failed to create offsetmanager err=%v", err)
	}
	return nil
}

func (cmd *consumeCmd) consume(partitions []int32) {
	var (
		out = make(chan printContext)
	)

	go print(out, cmd.Pretty)

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
	debugf(cmd, "Partition %d: resolved start offset %s to %d\n", partition, describeOffset(offsets.start), start)

	if end, err = cmd.resolveOffset(offsets.end, partition); err != nil {
		warnf("Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}
	debugf(cmd, "Partition %d: resolved end offset %s to %d\n", partition, describeOffset(offsets.end), end)

	// Check if we should exit early due to until time
	// Only perform expensive high water mark lookup if until time is in the past
	if cmd.until != nil && time.Now().After(*cmd.until) {
		// Get the current high water mark (newest offset) only when necessary
		highWaterMark, err := cmd.client.GetOffset(cmd.Topic, partition, sarama.OffsetNewest)
		if err == nil && start >= highWaterMark {
			// We're starting at or beyond the high water mark and until time has passed
			debugf(cmd, "Partition %d: start offset %d >= high water mark %d and current time is after until time %s, exiting early\n",
				partition, start, highWaterMark, cmd.until.Format(time.RFC3339))
			return
		}
	}

	if pcon, err = cmd.consumer.ConsumePartition(cmd.Topic, partition, start); err != nil {
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

func (c consumedMessage) ToMap() map[string]any {
	m := map[string]any{
		"partition": c.Partition,
		"offset":    c.Offset,
		"key":       ptrToValue(c.Key),
		"value":     ptrToValue(c.Value),
	}
	if c.Timestamp != nil {
		m["timestamp"] = c.Timestamp.Format(time.RFC3339Nano)
	}
	return m
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

	pom, err := cmd.offsetManager.ManagePartition(cmd.Topic, p)
	if err != nil {
		cmd.Unlock()
		warnf("failed to create partition offset manager err=%v", err)
		return nil
	}
	cmd.poms[p] = pom
	cmd.Unlock()
	return pom
}

func (cmd *consumeCmd) partitionLoop(out chan printContext, pc sarama.PartitionConsumer, p int32, end int64) {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)
	var (
		timer        *time.Timer
		untilTimer   *time.Timer
		pom          sarama.PartitionOffsetManager
		timeout      = make(<-chan time.Time)
		untilTimeout = make(<-chan time.Time)
	)

	if cmd.Group != "" {
		pom = cmd.getPOM(p)
	}

	// Set up until timer if specified
	if cmd.until != nil {
		duration := time.Until(*cmd.until)
		if duration > 0 {
			untilTimer = time.NewTimer(duration)
			untilTimeout = untilTimer.C
			debugf(cmd, "Partition %d: will stop consuming at %s (in %v)\n", p, cmd.until.Format(time.RFC3339), duration)
		} else {
			// Until time is already in the past
			debugf(cmd, "Partition %d: until time %s is in the past, exiting immediately\n", p, cmd.until.Format(time.RFC3339))
			return
		}
	}

	for {
		if cmd.Timeout > 0 {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(cmd.Timeout)
			timeout = timer.C
		}

		select {
		case <-cmd.shutdown:
			warnf("partition %v consumer shutting down gracefully\n", p)
			if cmd.Group != "" && pom != nil {
				if err := pom.Close(); err != nil {
					warnf("failed to close partition offset manager for partition %v err=%v\n", p, err)
				}
			}
			return
		case <-timeout:
			warnf("consuming from partition %v timed out after %s\n", p, cmd.Timeout)
			return
		case <-untilTimeout:
			debugf(cmd, "Partition %d: reached until time %s, stopping consumption\n", p, cmd.until.Format(time.RFC3339))
			// Signal that until time was reached
			select {
			case cmd.untilReached <- struct{}{}:
			default:
			}
			return
		case err := <-pc.Errors():
			warnf("partition %v consumer encountered err %s\n", p, err)
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				warnf("unexpected closed messages chan\n")
				return
			}

			// Check if message timestamp exceeds until time
			if cmd.until != nil && !msg.Timestamp.IsZero() && msg.Timestamp.After(*cmd.until) {
				warnf("partition %v consumer reached until time %v (message timestamp: %v)\n", p, cmd.until.Format(time.RFC3339), msg.Timestamp.Format(time.RFC3339))
				// Signal that until time was reached
				select {
				case cmd.untilReached <- struct{}{}:
				default:
				}
				return
			}

			m := newConsumedMessage(msg, cmd.EncodeKey, cmd.EncodeValue)
			ctx := printContext{output: m, done: make(chan struct{}), cmd: cmd.baseCmd}
			out <- ctx
			<-ctx.done

			if cmd.Group != "" {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return
			}

			// Check if we've reached the high water mark and until time is set
			if cmd.until != nil && msg.Offset >= pc.HighWaterMarkOffset()-1 {
				// We've consumed all available messages, check if we should stop due to until time
				// Since all remaining messages would be newer than the until time, we can stop
				if time.Now().After(*cmd.until) {
					warnf("partition %v consumer reached end of available messages with until time %v\n", p, cmd.until.Format(time.RFC3339))
					// Signal that until time was reached
					select {
					case cmd.untilReached <- struct{}{}:
					default:
					}
					return
				}
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
	if all, err = cmd.consumer.Partitions(cmd.Topic); err != nil {
		warnf("failed to read partitions for topic %v err=%v", cmd.Topic, err)
		return []int32{}
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

The -until flag allows you to stop consuming when message timestamps reach a specified time:

Current time:
  -until now

Absolute time (RFC3339 format):
  -until 2006-01-02T15:04:05Z07:00

Relative time from now:
  -until +5m     # 5 minutes from now
  -until +1h30m  # 1 hour 30 minutes from now
  -until +24h    # 24 hours from now

Relative time in the past:
  -until -1h     # 1 hour ago
  -until -30m    # 30 minutes ago
  -until -24h    # 24 hours ago

Common use cases with -group and resume offsets:
  # Resume from last committed offset, stop at current time
  -group mygroup -offsets resume -until now
  
  # Resume from last committed offset, stop at 1 hour ago
  -group mygroup -offsets resume -until -1h
  
  # Process messages from yesterday only
  -group mygroup -offsets resume -until -24h

If the topic only contains messages older than the until time, consumption will stop when reaching the end of available messages.

`, ENV_TOPIC, ENV_BROKERS)
