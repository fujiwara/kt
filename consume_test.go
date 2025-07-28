package main

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

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
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "single-comma",
			input:    ",",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all",
			input:    "all",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest",
			input:    "oldest",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "resume",
			input:    "resume",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: offsetResume},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-with-space",
			input:    "	all ",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-with-zero-initial-offset",
			input:    "all=+0:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "several-partitions",
			input:    "1,2,4",
			expected: map[int32]interval{
				1: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
				2: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
				4: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,empty-offsets",
			input:    "0=",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,one-offset",
			input:    "0=1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,empty-after-colon",
			input:    "0=1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "multiple-partitions",
			input:    "0=4:,2=1:10,6",
			expected: map[int32]interval{
				0: {
					start: offset{relative: false, start: 4},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
				2: {
					start: offset{relative: false, start: 1},
					end:   offset{relative: false, start: 10},
				},
				6: {
					start: offset{relative: true, start: sarama.OffsetOldest},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "newest-relative",
			input:    "0=-1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "newest-relative,empty-after-colon",
			input:    "0=-1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "resume-relative",
			input:    "0=resume-10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: offsetResume, diff: -10},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative",
			input:    "0=+1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative,empty-after-colon",
			input:    "0=+1:",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1},
					end:   offset{relative: false, start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative-to-newest-relative",
			input:    "0=+1:-1",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -1},
				},
			},
		},
		{
			testName: "specific-partition-with-all-partitions",
			input:    "0=+1:-1,all=1:10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 1},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -1},
				},
				-1: {
					start: offset{relative: false, start: 1, diff: 0},
					end:   offset{relative: false, start: 10, diff: 0},
				},
			},
		},
		{
			testName: "oldest-to-newest",
			input:    "0=oldest:newest",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: 0},
				},
			},
		},
		{
			testName: "oldest-to-newest-with-offsets",
			input:    "0=oldest+10:newest-10",
			expected: map[int32]interval{
				0: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10},
					end:   offset{relative: true, start: sarama.OffsetNewest, diff: -10},
				},
			},
		},
		{
			testName: "newest",
			input:    "newest",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: 0},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "single-partition",
			input:    "10",
			expected: map[int32]interval{
				10: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 0},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "single-range,all-partitions",
			input:    "10:20",
			expected: map[int32]interval{
				-1: {
					start: offset{start: 10},
					end:   offset{start: 20},
				},
			},
		},
		{
			testName: "single-range,all-partitions,open-end",
			input:    "10:",
			expected: map[int32]interval{
				-1: {
					start: offset{start: 10},
					end:   offset{start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-newest",
			input:    "all=newest:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: 0},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-newest-with-offset",
			input:    "newest-10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -10},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-oldest-with-offset",
			input:    "oldest+10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-neg-offset-empty-colon",
			input:    "-10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetNewest, diff: -10},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-pos-offset-empty-colon",
			input:    "+10:",
			expected: map[int32]interval{
				-1: {
					start: offset{relative: true, start: sarama.OffsetOldest, diff: 10},
					end:   offset{relative: false, start: 1<<63 - 1, diff: 0},
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
			if !reflect.DeepEqual(actual, d.expected) {
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
				10: {offset{false, 2, 0}, offset{false, 4, 0}},
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
				-1: {offset{false, 3, 0}, offset{false, 41, 0}},
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
		-1: {start: offset{false, 1, 0}, end: offset{false, 5, 0}},
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
