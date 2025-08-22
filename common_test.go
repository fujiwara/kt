package main

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"

	"github.com/IBM/sarama"
)

func TestChooseKafkaVersion(t *testing.T) {
	td := map[string]struct {
		arg      string
		env      string
		err      error
		expected sarama.KafkaVersion
	}{
		"default": {
			expected: sarama.V3_0_0_0,
		},
		"arg v1": {
			arg:      "v1.0.0",
			expected: sarama.V1_0_0_0,
		},
		"env v2": {
			arg:      "v2.0.0",
			expected: sarama.V2_0_0_0,
		},
		"invalid": {
			arg: "234",
			err: fmt.Errorf("invalid version `234`"),
		},
	}

	for tn, tc := range td {
		actual, err := chooseKafkaVersion(tc.arg)
		if tc.err == nil {
			if actual != tc.expected {
				t.Errorf("%s: expected %v, got %v", tn, tc.expected, actual)
			}
			if err != nil {
				t.Errorf("%s: expected no error, got %v", tn, err)
			}
		} else {
			if err == nil || err.Error() != tc.err.Error() {
				t.Errorf("%s: expected error %v, got %v", tn, tc.err, err)
			}
		}
	}
}

func TestParseTimeString(t *testing.T) {
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
			name:        "relative-past-1h",
			input:       "-1h",
			expectError: false,
		},
		{
			name:        "absolute-rfc3339",
			input:       "2023-01-01T12:00:00Z",
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
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result, err := parseTimeString(d.input)

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

func TestApplyJqFilter(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		input    object
		expected interface{}
		multi    bool
		wantErr  bool
	}{
		{
			name:     "extract simple field",
			query:    ".name",
			input:    mapObject{"name": "test-topic", "partition": 0},
			expected: "test-topic",
		},
		{
			name:     "extract nested field",
			query:    ".config.retention",
			input:    mapObject{"name": "test", "config": map[string]interface{}{"retention": "7d"}},
			expected: "7d",
		},
		{
			name:     "extract array element",
			query:    ".partitions[0]",
			input:    mapObject{"partitions": []interface{}{10, 20, 30}},
			expected: 10,
		},
		{
			name:     "extract with fromjson",
			query:    ".value | fromjson | .user_id",
			input:    mapObject{"value": `{"user_id": 123, "action": "purchase"}`},
			expected: 123,
		},
		{
			name:     "string interpolation",
			query:    `"\(.partition):\(.offset)"`,
			input:    mapObject{"partition": 0, "offset": 42},
			expected: "0:42",
		},
		{
			name:     "extract array elements",
			query:    ".value[]",
			input:    mapObject{"value": []any{1, 2, 3}},
			expected: []any{1, 2, 3},
			multi:    true,
		},
		{
			name:     "filter non-existent field returns null",
			query:    ".nonexistent",
			input:    mapObject{"name": "test"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := baseCmd{Jq: tt.query}
			if err := cmd.prepare(); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			result, multi, err := applyJqFilter(cmd.jqQuery, tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for input %+v, but got none", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for input %+v: %v", tt.input, err)
				return
			}
			// Special handling for fromjson test case where numbers might be float64
			if tt.name == "extract with fromjson" {
				// Allow both int and float64 for JSON numbers
				if result, ok := result.(float64); ok && int(result) == tt.expected.(int) {
					return // Test passes
				}
			}
			if tt.multi != multi {
				t.Error("multiple results expected")
			}
			if !reflect.DeepEqual(tt.expected, result) {
				t.Errorf("expected %v (type %T), got %v (type %T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestPrint(t *testing.T) {
	// Save original stdoutWriter
	originalStdout := stdoutWriter
	defer func() { stdoutWriter = originalStdout }()

	tests := []struct {
		name     string
		query    string
		input    object
		raw      bool
		expected string
	}{
		{
			name:     "normal JSON output",
			query:    "",
			input:    mapObject{"name": "test", "value": 42},
			raw:      false,
			expected: `{"name":"test","value":42}` + "\n",
		},
		{
			name:     "raw string output",
			query:    ".name",
			input:    mapObject{"name": "test", "value": 42},
			raw:      true,
			expected: "test\n",
		},
		{
			name:     "raw number as JSON",
			query:    ".value",
			input:    mapObject{"name": "test", "value": 42},
			raw:      false,
			expected: "42\n",
		},
		{
			name:     "jq filter with JSON output",
			query:    ".name",
			input:    mapObject{"name": "test", "value": 42},
			raw:      false,
			expected: `"test"` + "\n",
		},
		{
			name:     "complex jq with raw output",
			query:    `"\(.name):\(.value)"`,
			input:    mapObject{"name": "topic", "value": 123},
			raw:      true,
			expected: "topic:123\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture output
			var buf bytes.Buffer
			stdoutWriter = &buf
			cmd := baseCmd{Jq: tt.query, Raw: tt.raw}
			if err := cmd.prepare(); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Create print context channel
			in := make(chan printContext, 1)
			done := make(chan struct{})
			// Start print goroutine
			go func() {
				print(in, false) // Use false for pretty to get consistent output
			}()

			// Send test data to print function
			ctx := printContext{
				output: tt.input,
				done:   done,
				cmd:    cmd,
			}
			in <- ctx
			<-done // Wait for processing to complete

			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}

func TestPrintOutput(t *testing.T) {
	// Save original stdoutWriter
	originalStdout := stdoutWriter
	defer func() { stdoutWriter = originalStdout }()

	tests := []struct {
		name     string
		input    any
		raw      bool
		expected string
	}{
		{
			name:     "string with raw=true",
			input:    "test string",
			raw:      true,
			expected: "test string\n",
		},
		{
			name:     "string with raw=false",
			input:    "test string",
			raw:      false,
			expected: `"test string"` + "\n",
		},
		{
			name:     "number with raw=false",
			input:    42,
			raw:      false,
			expected: "42\n",
		},
		{
			name:     "object with raw=false",
			input:    map[string]any{"key": "value"},
			raw:      false,
			expected: `{"key":"value"}` + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture output
			var buf bytes.Buffer
			stdoutWriter = &buf

			marshal := json.Marshal
			printOutput(tt.input, marshal, tt.raw)

			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}

// Test that ToMap results are equivalent to JSON conversion
func TestToMapEquivalence(t *testing.T) {
	tests := []struct {
		name   string
		object object
	}{
		{
			name: "consumedMessage with all fields",
			object: func() object {
				timestamp := time.Date(2023, 12, 1, 15, 0, 0, 0, time.UTC)
				key := "test-key"
				value := "test-value"
				return consumedMessage{
					Partition: 0,
					Offset:    42,
					Key:       &key,
					Value:     &value,
					Timestamp: &timestamp,
				}
			}(),
		},
		{
			name: "consumedMessage with nil fields",
			object: consumedMessage{
				Partition: 1,
				Offset:    100,
				Key:       nil,
				Value:     nil,
				Timestamp: nil,
			},
		},
		{
			name: "topic with partitions and config",
			object: topic{
				Name: "test-topic",
				Partitions: []partition{
					{Id: 0, OldestOffset: 100, NewestOffset: 200, Leader: "1", Replicas: []int32{1, 2}, ISRs: []int32{1, 2}},
					{Id: 1, OldestOffset: 150, NewestOffset: 250, Leader: "2", Replicas: []int32{2, 3}, ISRs: []int32{2}},
				},
				Config: map[string]string{"retention.ms": "604800000"},
			},
		},
		{
			name: "topic with no partitions",
			object: topic{
				Name:       "empty-topic",
				Partitions: []partition{},
				Config:     map[string]string{},
			},
		},
		{
			name: "group with offsets",
			object: func() object {
				offset1 := int64(100)
				lag1 := int64(10)
				offset2 := int64(200)
				lag2 := int64(20)
				return group{
					Name:  "test-group",
					Topic: "test-topic",
					Offsets: []groupOffset{
						{Partition: 0, Offset: &offset1, Lag: &lag1},
						{Partition: 1, Offset: &offset2, Lag: &lag2},
					},
				}
			}(),
		},
		{
			name: "group with nil offsets",
			object: group{
				Name:  "test-group",
				Topic: "test-topic",
				Offsets: []groupOffset{
					{Partition: 0, Offset: nil, Lag: nil},
					{Partition: 1, Offset: nil, Lag: nil},
				},
			},
		},
		{
			name: "group with no offsets",
			object: group{
				Name:    "empty-group",
				Topic:   "",
				Offsets: []groupOffset{},
			},
		},
		{
			name: "partition",
			object: partition{
				Id:           0,
				OldestOffset: 100,
				NewestOffset: 200,
				Leader:       "1",
				Replicas:     []int32{1, 2, 3},
				ISRs:         []int32{1, 2},
			},
		},
		{
			name: "groupOffset with values",
			object: func() object {
				offset := int64(100)
				lag := int64(10)
				return groupOffset{
					Partition: 0,
					Offset:    &offset,
					Lag:       &lag,
				}
			}(),
		},
		{
			name: "groupOffset with nil values",
			object: groupOffset{
				Partition: 1,
				Offset:    nil,
				Lag:       nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get ToMap result
			toMapResult := tt.object.ToMap()

			// Marshal original object to JSON then unmarshal
			originalJSON, err := json.Marshal(tt.object)
			if err != nil {
				t.Fatalf("failed to marshal original object: %v", err)
			}

			var jsonResult map[string]any
			err = json.Unmarshal(originalJSON, &jsonResult)
			if err != nil {
				t.Fatalf("failed to unmarshal to map: %v", err)
			}

			// Compare both results as JSON (to normalize field order)
			toMapJSON, err := json.Marshal(toMapResult)
			if err != nil {
				t.Fatalf("failed to marshal ToMap result: %v", err)
			}

			jsonResultJSON, err := json.Marshal(jsonResult)
			if err != nil {
				t.Fatalf("failed to marshal JSON result: %v", err)
			}

			if string(toMapJSON) != string(jsonResultJSON) {
				t.Errorf("ToMap result differs from JSON conversion:\nToMap:    %s\nJSON:     %s",
					string(toMapJSON), string(jsonResultJSON))

				// Add debug information
				t.Logf("Original object: %+v", tt.object)
				t.Logf("ToMap result: %+v", toMapResult)
				t.Logf("JSON result: %+v", jsonResult)
			}
		})
	}
}

// Test that ToMap results can be properly processed by jq
func TestToMapWithJq(t *testing.T) {
	tests := []struct {
		name     string
		object   object
		jqQuery  string
		expected any
	}{
		{
			name: "extract message partition",
			object: consumedMessage{
				Partition: 5,
				Offset:    42,
				Key:       nil,
				Value:     nil,
				Timestamp: nil,
			},
			jqQuery:  ".partition",
			expected: 5,
		},
		{
			name: "extract topic name",
			object: topic{
				Name:       "my-topic",
				Partitions: []partition{},
				Config:     map[string]string{},
			},
			jqQuery:  ".name",
			expected: "my-topic",
		},
		{
			name: "extract group name",
			object: group{
				Name:    "my-group",
				Topic:   "my-topic",
				Offsets: []groupOffset{},
			},
			jqQuery:  ".name",
			expected: "my-group",
		},
		{
			name: "extract partition leader",
			object: partition{
				Id:           0,
				OldestOffset: 100,
				NewestOffset: 200,
				Leader:       "broker-1",
				Replicas:     []int32{1, 2},
				ISRs:         []int32{1},
			},
			jqQuery:  ".leader",
			expected: "broker-1",
		},
		{
			name: "extract first replica",
			object: partition{
				Id:           0,
				OldestOffset: 100,
				NewestOffset: 200,
				Leader:       "broker-1",
				Replicas:     []int32{1, 2, 3},
				ISRs:         []int32{1},
			},
			jqQuery:  ".replicas[0]",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare jq query
			cmd := baseCmd{Jq: tt.jqQuery}
			if err := cmd.prepare(); err != nil {
				t.Fatalf("failed to prepare jq query: %v", err)
			}

			// Apply jq filter to ToMap result
			result, multi, err := applyJqFilter(cmd.jqQuery, tt.object)
			if err != nil {
				t.Fatalf("jq filter failed: %v", err)
			}

			if multi {
				t.Errorf("expected single result, got multiple")
				return
			}

			// Handle type conversion when needed (JSON numbers become float64)
			if expectedInt, ok := tt.expected.(int); ok {
				if resultFloat, ok := result.(float64); ok && int(resultFloat) == expectedInt {
					return // Test passes
				}
			}

			if result != tt.expected {
				t.Errorf("expected %v (type %T), got %v (type %T)",
					tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestBaseCmdJqFlags(t *testing.T) {
	tests := []struct {
		name        string
		jq          string
		raw         bool
		expectJq    string
		expectRaw   bool
		expectError bool
	}{
		{
			name:        "no jq flags",
			jq:          "",
			raw:         false,
			expectJq:    "",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "jq flag only",
			jq:          ".startOffset",
			raw:         false,
			expectJq:    ".startOffset",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "raw flag only",
			jq:          "",
			raw:         true,
			expectJq:    "",
			expectRaw:   true,
			expectError: false,
		},
		{
			name:        "both jq and raw flags",
			jq:          ".partition",
			raw:         true,
			expectJq:    ".partition",
			expectRaw:   true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &baseCmd{}
			cmd.Jq = tt.jq
			cmd.Raw = tt.raw
			cmd.prepare()
			if cmd.Jq != tt.expectJq {
				t.Errorf("expected jq %q, got %q", tt.expectJq, cmd.Jq)
			}
			if cmd.Raw != tt.expectRaw {
				t.Errorf("expected raw %v, got %v", tt.expectRaw, cmd.Raw)
			}
		})
	}
}

func TestBaseCmdBrokers(t *testing.T) {
	tests := []struct {
		name            string
		brokers         []string
		expectedBrokers []string
	}{
		{
			name:            "localhost without port",
			brokers:         []string{"localhost"},
			expectedBrokers: []string{"localhost:9092"},
		},
		{
			name:            "localhost with port",
			brokers:         []string{"localhost:9092"},
			expectedBrokers: []string{"localhost:9092"},
		},
		{
			name:            "multiple brokers mixed",
			brokers:         []string{"broker1", "broker2:9093"},
			expectedBrokers: []string{"broker1:9092", "broker2:9093"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &baseCmd{}
			cmd.Brokers = tt.brokers
			actual := cmd.addDefaultPorts(cmd.Brokers)
			if len(actual) != len(tt.expectedBrokers) {
				t.Errorf("expected %d brokers, got %d", len(tt.expectedBrokers), len(actual))
			}
			for i, expected := range tt.expectedBrokers {
				if i < len(actual) && actual[i] != expected {
					t.Errorf("expected broker[%d] %s, got %s", i, expected, actual[i])
				}
			}
		})
	}
}
