package main

import (
	"context"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"

	"github.com/IBM/sarama"
)

func TestParseOffsets(t *testing.T) {
	data := []struct {
		testName    string
		input       string
		expected    map[int32]interval
		expectedErr string
	}{
		{
			testName: "empty",
			input:    "",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "single-comma",
			input:    ",",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "all",
			input:    "all",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "oldest",
			input:    "oldest",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "resume",
			input:    "resume",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: offsetResume, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "all-with-space",
			input:    "	all ",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "all-with-zero-initial-offset",
			input:    "all=+0:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "several-partitions",
			input:    "1,2,4",
			expected: map[int32]interval{
				1: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
				2: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
				4: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "one-partition,empty-offsets",
			input:    "0=",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "one-partition,one-offset",
			input:    "0=1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "one-partition,empty-after-colon",
			input:    "0=1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "multiple-partitions",
			input:    "0=4:,2=1:10,6",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 4, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
				2: {
					start: offset{relative: false, start: 1, timestamp: false},
					end:   offset{relative: false, start: 10, timestamp: false},
				},
				6: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "newest-relative",
			input:    "0=-1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "newest-relative,empty-after-colon",
			input:    "0=-1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "resume-relative",
			input:    "0=resume-10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: offsetResume, diff: -10, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "oldest-relative",
			input:    "0=+1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "oldest-relative,empty-after-colon",
			input:    "0=+1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "oldest-relative-to-newest-relative",
			input:    "0=+1:-1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1, timestamp: false},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -1, timestamp: false},
				},
			},
		},
		{
			testName: "specific-partition-with-all-partitions",
			input:    "0=+1:-1,all=1:10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1, timestamp: false},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -1, timestamp: false},
				},
				-1: {
					start: offset{relative: false, start: 1, diff: 0, timestamp: false},
					end:   offset{relative: false, start: 10, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "oldest-to-newest",
			input:    "0=oldest:newest",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0, timestamp: false},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "oldest-to-newest-with-offsets",
			input:    "0=oldest+10:newest-10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10, timestamp: false},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -10, timestamp: false},
				},
			},
		},
		{
			testName: "newest",
			input:    "newest",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: 0, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "single-partition",
			input:    "10",
			expected: map[int32]interval{
				10: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "single-range,all-partitions",
			input:    "10:20",
			expected: map[int32]interval{
				-1: {
					start: offset{start: 10, timestamp: false},
					end:   offset{start: 20, timestamp: false},
				},
			},
		},
		{
			testName: "single-range,all-partitions,open-end",
			input:    "10:",
			expected: map[int32]interval{
				-1: {
					start: offset{start: 10, timestamp: false},
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "all-newest",
			input:    "all=newest:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: 0, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "implicit-all-newest-with-offset",
			input:    "newest-10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -10, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "implicit-all-oldest-with-offset",
			input:    "oldest+10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "implicit-all-neg-offset-empty-colon",
			input:    "-10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -10, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName: "implicit-all-pos-offset-empty-colon",
			input:    "+10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10, timestamp: false},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0, timestamp: false},
				},
			},
		},
		{
			testName:    "invalid-partition",
			input:       "bogus",
			expectedErr: `invalid offset "bogus"`,
		},
		{
			testName:    "several-colons",
			input:       ":::",
			expectedErr: `invalid offset "::"`,
		},
		{
			testName:    "bad-relative-offset-start",
			input:       "foo+20",
			expectedErr: `invalid offset "foo+20"`,
		},
		{
			testName:    "bad-relative-offset-diff",
			input:       "oldest+bad",
			expectedErr: `invalid offset "oldest+bad"`,
		},
		{
			testName:    "bad-relative-offset-diff-at-start",
			input:       "+bad",
			expectedErr: `invalid offset "+bad"`,
		},
		{
			testName:    "relative-offset-too-big",
			input:       "+9223372036854775808",
			expectedErr: `offset "+9223372036854775808" is too large`,
		},
		{
			testName:    "starting-offset-too-big",
			input:       "9223372036854775808:newest",
			expectedErr: `offset "9223372036854775808" is too large`,
		},
		{
			testName:    "ending-offset-too-big",
			input:       "oldest:9223372036854775808",
			expectedErr: `offset "9223372036854775808" is too large`,
		},
		{
			testName:    "partition-too-big",
			input:       "2147483648=oldest",
			expectedErr: `partition number "2147483648" is too large`,
		},
		{
			testName: "time-absolute",
			input:    "all=2023-12-01T15:00:00Z",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: 1701442800000, timestamp: true}, // 2023-12-01T15:00:00Z in millis
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "time-absolute-implicit-all",
			input:    "2023-12-01T15:00:00Z",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: 1701442800000, timestamp: true}, // 2023-12-01T15:00:00Z in millis (implicit all)
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "time-now-implicit-all",
			input:    "now",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: 0, timestamp: true}, // Will be set to current time during parsing
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "time-relative-implicit-all",
			input:    "+1h",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: 0, timestamp: true}, // Will be set to current time + 1h
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
		{
			testName: "mixed-partition-specific-and-implicit",
			input:    "0=oldest,+1h",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, timestamp: false},
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
				-1: {
					start: offset{relative: true, start: 0, timestamp: true}, // +1h for all other partitions
					end:   offset{start: 1<<63 - 1, timestamp: false},
				},
			},
		},
	}

	for _, d := range data {
		t.Run(d.testName, func(t *testing.T) {
			actual, err := parseOffsets(d.input)
			if d.expectedErr != "" {
				if err == nil {
					t.Fatalf("got no error; want error %q", d.expectedErr)
				}
				if got, want := err.Error(), d.expectedErr; got != want {
					t.Fatalf("got unexpected error %q want %q", got, want)
				}
				return
			}

			// Special handling for dynamic timestamp tests
			if strings.Contains(d.testName, "now") || strings.Contains(d.testName, "relative-implicit") || strings.Contains(d.testName, "mixed-partition") {
				// Check structure is correct but don't compare exact timestamp values
				if len(actual) != len(d.expected) {
					t.Errorf("Expected %d intervals, got %d", len(d.expected), len(actual))
					return
				}
				for partition, expectedInterval := range d.expected {
					actualInterval, ok := actual[partition]
					if !ok {
						t.Errorf("Missing partition %d in actual result", partition)
						continue
					}
					// For timestamp offsets, just check the flags are correct
					if expectedInterval.start.timestamp {
						if !actualInterval.start.relative || !actualInterval.start.timestamp {
							t.Errorf("Partition %d start should be relative and timestamp", partition)
						}
						if actualInterval.start.start == 0 {
							t.Errorf("Partition %d start timestamp should not be zero", partition)
						}
					} else {
						// Non-timestamp offsets should match exactly
						if !reflect.DeepEqual(actualInterval.start, expectedInterval.start) {
							t.Errorf("Partition %d start mismatch: expected %+v, got %+v", partition, expectedInterval.start, actualInterval.start)
						}
					}
					// End should always match exactly
					if !reflect.DeepEqual(actualInterval.end, expectedInterval.end) {
						t.Errorf("Partition %d end mismatch: expected %+v, got %+v", partition, expectedInterval.end, actualInterval.end)
					}
				}
			} else if !reflect.DeepEqual(actual, d.expected) {
				t.Errorf(
					`
Expected: %+v, err=%v
Actual:   %+v, err=%v
Input:    %v
	`,
					d.expected,
					d.expectedErr,
					actual,
					err,
					d.input,
				)
			}
		})
	}
}

func TestTimestampOffsetParsing(t *testing.T) {
	now := time.Now()

	// Test "now" parsing
	result, err := parseOffsets("all=now")
	if err != nil {
		t.Fatalf("unexpected error parsing 'now': %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(result))
	}

	interval, ok := result[-1]
	if !ok {
		t.Fatal("expected interval for partition -1")
	}

	if !interval.start.relative || !interval.start.timestamp {
		t.Errorf("expected start to be relative and timestamp, got relative=%v timestamp=%v", interval.start.relative, interval.start.timestamp)
	}

	// Check that the timestamp is close to current time (within 1 second)
	actualTime := time.Unix(0, interval.start.start*int64(time.Millisecond))
	if actualTime.Sub(now).Abs() > time.Second {
		t.Errorf("'now' timestamp %v should be close to test start time %v", actualTime, now)
	}

	// Test relative duration parsing
	result, err = parseOffsets("all=+1h")
	if err != nil {
		t.Fatalf("unexpected error parsing '+1h': %v", err)
	}

	interval = result[-1]
	if !interval.start.relative || !interval.start.timestamp {
		t.Errorf("expected start to be relative and timestamp for +1h")
	}

	// Check that +1h is about 1 hour in the future
	actualTime = time.Unix(0, interval.start.start*int64(time.Millisecond))
	expectedTime := now.Add(time.Hour)
	if actualTime.Sub(expectedTime).Abs() > time.Second {
		t.Errorf("+1h timestamp %v should be about 1 hour from test start time %v", actualTime, expectedTime)
	}
}

func TestFindPartitionsToConsume(t *testing.T) {
	data := []struct {
		topic    string
		offsets  map[int32]interval
		consumer tConsumer
		expected []int32
	}{
		{
			topic: "a",
			offsets: map[int32]interval{
				10: {offset{relative: false, start: 2, diff: 0, timestamp: false}, offset{relative: false, start: 4, diff: 0, timestamp: false}},
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
			offsets: map[int32]interval{
				-1: {offset{relative: false, start: 3, diff: 0, timestamp: false}, offset{relative: false, start: 41, diff: 0, timestamp: false}},
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
		target := &consumeCmd{
			consumer: d.consumer,
			topic:    d.topic,
			offsets:  d.offsets,
		}
		actual := target.findPartitions()

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
	target := consumeCmd{consumer: consumer}
	target.topic = "hans"
	target.brokers = []string{"localhost:9092"}
	target.offsets = map[int32]interval{
		-1: {start: offset{relative: false, start: 1, diff: 0, timestamp: false}, end: offset{relative: false, start: 5, diff: 0, timestamp: false}},
	}

	go target.consume(partitions)
	defer close(closer)

	end := make(chan struct{})
	go func(c chan tConsumePartition, e chan struct{}) {
		actual := []tConsumePartition{}
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

func (pc tPartitionConsumer) IsPaused() bool { return false }
func (pc tPartitionConsumer) Pause()         {}
func (pc tPartitionConsumer) Resume()        {}

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

func (c tConsumer) Pause(topicPartitions map[string][]int32) {}
func (c tConsumer) PauseAll()                                {}

func (c tConsumer) Resume(topicPartitions map[string][]int32) {}
func (c tConsumer) ResumeAll()                                {}

func TestConsumeParseArgs(t *testing.T) {
	topic := "test-topic"
	givenBroker := "hans:9092"
	brokers := []string{givenBroker}

	os.Setenv(ENV_TOPIC, topic)
	os.Setenv(ENV_BROKERS, givenBroker)
	target := &consumeCmd{}

	target.parseArgs([]string{})
	if target.topic != topic ||
		!reflect.DeepEqual(target.brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}

	// default brokers to localhost:9092
	os.Setenv(ENV_TOPIC, "")
	os.Setenv(ENV_BROKERS, "")
	brokers = []string{"localhost:9092"}

	target.parseArgs([]string{"-topic", topic})
	if target.topic != topic ||
		!reflect.DeepEqual(target.brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}

	// command line arg wins
	os.Setenv(ENV_TOPIC, "BLUBB")
	os.Setenv(ENV_BROKERS, "BLABB")
	brokers = []string{givenBroker}

	target.parseArgs([]string{"-topic", topic, "-brokers", givenBroker})
	if target.topic != topic ||
		!reflect.DeepEqual(target.brokers, brokers) {
		t.Errorf("Expected topic %#v and brokers %#v from env vars, got %#v.", topic, brokers, target)
		return
	}
}

func TestParseUntilTime(t *testing.T) {
	now := time.Now()
	data := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "current-time-now",
			input:       "now",
			expectError: false,
		},
		{
			name:        "relative-5m",
			input:       "+5m",
			expectError: false,
		},
		{
			name:        "relative-1h30m",
			input:       "+1h30m",
			expectError: false,
		},
		{
			name:        "relative-past-1h",
			input:       "-1h",
			expectError: false,
		},
		{
			name:        "relative-past-30m",
			input:       "-30m",
			expectError: false,
		},
		{
			name:        "absolute-rfc3339",
			input:       "2023-01-01T12:00:00Z",
			expectError: false,
		},
		{
			name:        "absolute-rfc3339-with-timezone",
			input:       "2023-01-01T12:00:00+09:00",
			expectError: false,
		},
		{
			name:        "invalid-relative",
			input:       "+invalid",
			expectError: true,
		},
		{
			name:        "invalid-absolute",
			input:       "not-a-date",
			expectError: true,
		},
		{
			name:        "invalid-date-format",
			input:       "2023-01-01",
			expectError: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result, err := parseUntilTime(d.input)

			if d.expectError {
				if err == nil {
					t.Errorf("Expected error for input %q, but got none", d.input)
				}
				if result != nil {
					t.Errorf("Expected nil result on error, got %v", result)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for input %q: %v", d.input, err)
				return
			}

			if result == nil {
				t.Errorf("Expected non-nil result for input %q", d.input)
				return
			}

			// Check time relationships
			if d.input == "now" {
				// "now" should be close to the time we captured at the start
				if result.Sub(now).Abs() > time.Second {
					t.Errorf("'now' time %v should be close to test start time %v", result, now)
				}
			} else if strings.HasPrefix(d.input, "+") {
				if !result.After(now) {
					t.Errorf("Positive relative time %q should be after now (%v), got %v", d.input, now, result)
				}
			} else if strings.HasPrefix(d.input, "-") {
				if !result.Before(now) {
					t.Errorf("Negative relative time %q should be before now (%v), got %v", d.input, now, result)
				}
			}
		})
	}
}

func TestUntilTimeStopBehavior(t *testing.T) {
	// Test that partition consumer stops when high water mark is reached with until time
	now := time.Now()
	pastTime := now.Add(-1 * time.Hour)
	futureTime := now.Add(1 * time.Hour)

	// Create test messages with timestamps
	oldMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Timestamp: pastTime,
		Value:     []byte("old message"),
	}

	recentMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Timestamp: now.Add(-30 * time.Minute),
		Value:     []byte("recent message"),
	}

	// Test partition consumer with until time
	t.Run("partition-consumer-stops-at-high-water-mark-with-until", func(t *testing.T) {
		messageChan := make(chan *sarama.ConsumerMessage, 2)
		messageChan <- oldMessage
		messageChan <- recentMessage
		close(messageChan)

		pc := tPartitionConsumer{
			messages:            messageChan,
			highWaterMarkOffset: 2, // High water mark at offset 2
		}

		cmd := &consumeCmd{
			until:        &now,
			untilReached: make(chan struct{}, 1),
		}

		out := make(chan printContext, 2)
		// Process messages in background
		go func() {
			for ctx := range out {
				close(ctx.done)
			}
		}()

		p := int32(0)
		end := int64(1<<63 - 1) // Max end offset
		go cmd.partitionLoop(out, pc, p, end)

		// Should receive until reached signal
		select {
		case <-cmd.untilReached:
			// Expected behavior - consumer should stop when reaching high water mark with until time
		case <-time.After(1 * time.Second):
			t.Error("Expected consumer to stop when reaching high water mark with until time")
		}
	})

	// Test consumer group handler with until time
	t.Run("consumer-group-handler-stops-at-high-water-mark-with-until", func(t *testing.T) {
		messageChan := make(chan *sarama.ConsumerMessage, 2)
		messageChan <- oldMessage
		messageChan <- recentMessage
		close(messageChan)

		claim := &tConsumerGroupClaim{
			messages:            messageChan,
			partition:           0,
			highWaterMarkOffset: 2, // High water mark at offset 2
		}

		session := &tConsumerGroupSession{
			ctx: context.Background(),
		}

		cmd := &consumeCmd{
			until:        &now,
			untilReached: make(chan struct{}, 1),
			shutdown:     make(chan struct{}),
		}

		out := make(chan printContext, 2)
		handler := &ConsumerGroupHandler{
			cmd: cmd,
			out: out,
		}

		// Process messages in background
		go func() {
			for ctx := range out {
				close(ctx.done)
			}
		}()

		go handler.ConsumeClaim(session, claim)

		// Should receive until reached signal
		select {
		case <-cmd.untilReached:
			// Expected behavior - handler should stop when reaching high water mark with until time
		case <-time.After(1 * time.Second):
			t.Error("Expected consumer group handler to stop when reaching high water mark with until time")
		}
	})

	// Test that consumer doesn't stop if until time is in the future
	t.Run("partition-consumer-continues-with-future-until", func(t *testing.T) {
		messageChan := make(chan *sarama.ConsumerMessage, 1)
		messageChan <- oldMessage
		close(messageChan)

		pc := tPartitionConsumer{
			messages:            messageChan,
			highWaterMarkOffset: 1, // High water mark at offset 1
		}

		cmd := &consumeCmd{
			until:        &futureTime, // Until time is in the future
			untilReached: make(chan struct{}, 1),
		}

		out := make(chan printContext, 1)
		// Process messages in background
		go func() {
			for ctx := range out {
				close(ctx.done)
			}
		}()

		p := int32(0)
		end := int64(1<<63 - 1) // Max end offset
		go cmd.partitionLoop(out, pc, p, end)

		// Should NOT receive until reached signal quickly
		select {
		case <-cmd.untilReached:
			t.Error("Consumer should not stop when until time is in the future")
		case <-time.After(100 * time.Millisecond):
			// Expected behavior - consumer should continue waiting
		}
	})
}

// Test helper types for consumer group testing
type tConsumerGroupClaim struct {
	messages            chan *sarama.ConsumerMessage
	partition           int32
	highWaterMarkOffset int64
}

func (c *tConsumerGroupClaim) Topic() string                            { return "test-topic" }
func (c *tConsumerGroupClaim) Partition() int32                         { return c.partition }
func (c *tConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (c *tConsumerGroupClaim) HighWaterMarkOffset() int64               { return c.highWaterMarkOffset }
func (c *tConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

type tConsumerGroupSession struct {
	ctx context.Context
}

func (s *tConsumerGroupSession) Claims() map[string][]int32 { return nil }
func (s *tConsumerGroupSession) MemberID() string           { return "test-member" }
func (s *tConsumerGroupSession) GenerationID() int32        { return 1 }
func (s *tConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *tConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *tConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {}
func (s *tConsumerGroupSession) Context() context.Context                                 { return s.ctx }
func (s *tConsumerGroupSession) Commit()                                                  {}

func TestConsumeJqFlags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectJq    string
		expectRaw   bool
		expectError bool
	}{
		{
			name:        "no jq flags",
			args:        []string{"-topic", "test"},
			expectJq:    "",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "jq flag only",
			args:        []string{"-topic", "test", "-jq", ".value"},
			expectJq:    ".value",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "raw flag only",
			args:        []string{"-topic", "test", "-raw"},
			expectJq:    "",
			expectRaw:   true,
			expectError: false,
		},
		{
			name:        "both jq and raw flags",
			args:        []string{"-topic", "test", "-jq", ".name", "-raw"},
			expectJq:    ".name",
			expectRaw:   true,
			expectError: false,
		},
		{
			name:        "complex jq expression",
			args:        []string{"-topic", "test", "-jq", ".value | fromjson | .user_id"},
			expectJq:    ".value | fromjson | .user_id",
			expectRaw:   false,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &consumeCmd{}
			cmd.parseArgs(tt.args)
			if cmd.Jq != tt.expectJq {
				t.Errorf("expected jq %q, got %q", tt.expectJq, cmd.Jq)
			}
			if cmd.Raw != tt.expectRaw {
				t.Errorf("expected raw %v, got %v", tt.expectRaw, cmd.Raw)
			}
		})
	}
}

func TestConsumedMessageToMap(t *testing.T) {
	timestamp := time.Date(2023, 12, 1, 15, 0, 0, 0, time.UTC)
	key := "test-key"
	value := "test-value"

	msg := consumedMessage{
		Partition: 0,
		Offset:    42,
		Key:       &key,
		Value:     &value,
		Timestamp: &timestamp,
	}

	result := msg.ToMap()
	expected := map[string]any{
		"partition": int32(0),
		"offset":    int64(42),
		"key":       "test-key",
		"value":     "test-value",
		"timestamp": "2023-12-01T15:00:00Z",
	}

	// Compare as JSON to ensure equivalence regardless of Go object structure
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("failed to marshal expected: %v", err)
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}

	if string(expectedJSON) != string(resultJSON) {
		t.Errorf("JSON mismatch:\nexpected: %s\nactual:   %s", expectedJSON, resultJSON)
	}
}

func TestConsumedMessageToMapWithNils(t *testing.T) {
	msg := consumedMessage{
		Partition: 1,
		Offset:    100,
		Key:       nil,
		Value:     nil,
		Timestamp: nil,
	}

	result := msg.ToMap()
	expected := map[string]any{
		"partition": int32(1),
		"offset":    int64(100),
		"key":       nil,
		"value":     nil,
	}

	// Compare as JSON to ensure equivalence regardless of Go object structure
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("failed to marshal expected: %v", err)
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}

	if string(expectedJSON) != string(resultJSON) {
		t.Errorf("JSON mismatch:\nexpected: %s\nactual:   %s", expectedJSON, resultJSON)
	}
}
