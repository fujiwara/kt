package main

import (
	"os"
	"reflect"
	"testing"

	json "github.com/goccy/go-json"
)

func TestTopicParseArgs(t *testing.T) {
	target := &topicCmd{}
	givenBroker := "hans:9092"
	expectedBrokers := []string{givenBroker}
	os.Setenv(ENV_BROKERS, givenBroker)

	target.parseArgs([]string{})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}

	os.Setenv(ENV_BROKERS, "")
	expectedBrokers = []string{"localhost:9092"}

	target.parseArgs([]string{})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}

	os.Setenv(ENV_BROKERS, "BLABB")
	expectedBrokers = []string{givenBroker}

	target.parseArgs([]string{"-brokers", givenBroker})
	if !reflect.DeepEqual(target.brokers, expectedBrokers) {
		t.Errorf(
			"Expected brokers %v from env vars, got brokers %v.",
			expectedBrokers,
			target.brokers,
		)
		return
	}
}

func TestTopicJqFlags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectJq    string
		expectRaw   bool
		expectError bool
	}{
		{
			name:        "no jq flags",
			args:        []string{},
			expectJq:    "",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "jq flag only",
			args:        []string{"-jq", ".name"},
			expectJq:    ".name",
			expectRaw:   false,
			expectError: false,
		},
		{
			name:        "raw flag only",
			args:        []string{"-raw"},
			expectJq:    "",
			expectRaw:   true,
			expectError: false,
		},
		{
			name:        "both jq and raw flags",
			args:        []string{"-jq", ".partitions[0].id", "-raw"},
			expectJq:    ".partitions[0].id",
			expectRaw:   true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &topicCmd{}
			cmd.parseArgs(tt.args)
			if cmd.jq != tt.expectJq {
				t.Errorf("expected jq %q, got %q", tt.expectJq, cmd.jq)
			}
			if cmd.raw != tt.expectRaw {
				t.Errorf("expected raw %v, got %v", tt.expectRaw, cmd.raw)
			}
		})
	}
}

func TestTopicToMap(t *testing.T) {
	topic := topic{
		Name: "test-topic",
		Partitions: []partition{
			{Id: 0, OldestOffset: 100, NewestOffset: 200, Leader: "1", Replicas: []int32{1, 2}, ISRs: []int32{1, 2}},
			{Id: 1, OldestOffset: 150, NewestOffset: 250, Leader: "2", Replicas: []int32{2, 3}, ISRs: []int32{2}},
		},
		Config: map[string]string{"retention.ms": "604800000"},
	}

	result := topic.ToMap()
	expected := map[string]any{
		"name": "test-topic",
		"partitions": []map[string]any{
			{
				"id":       int32(0),
				"oldest":   int64(100),
				"newest":   int64(200),
				"leader":   "1",
				"replicas": []int32{1, 2},
				"isrs":     []int32{1, 2},
			},
			{
				"id":       int32(1),
				"oldest":   int64(150),
				"newest":   int64(250),
				"leader":   "2",
				"replicas": []int32{2, 3},
				"isrs":     []int32{2},
			},
		},
		"config": map[string]string{"retention.ms": "604800000"},
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

func TestPartitionToMap(t *testing.T) {
	p := partition{
		Id:           0,
		OldestOffset: 100,
		NewestOffset: 200,
		Leader:       "1",
		Replicas:     []int32{1, 2, 3},
		ISRs:         []int32{1, 2},
	}

	result := p.ToMap()
	expected := map[string]any{
		"id":       int32(0),
		"oldest":   int64(100),
		"newest":   int64(200),
		"leader":   "1",
		"replicas": []int32{1, 2, 3},
		"isrs":     []int32{1, 2},
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
