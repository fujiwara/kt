package main

import (
	"testing"

	json "github.com/goccy/go-json"
)


func TestGroupToMap(t *testing.T) {
	offset1 := int64(100)
	lag1 := int64(10)
	offset2 := int64(200)
	lag2 := int64(20)

	g := group{
		Name:  "test-group",
		Topic: "test-topic",
		Offsets: []groupOffset{
			{Partition: 0, Offset: &offset1, Lag: &lag1},
			{Partition: 1, Offset: &offset2, Lag: &lag2},
		},
	}

	result := g.ToMap()

	// Verify structure without comparing pointer addresses
	if result["name"] != "test-group" {
		t.Errorf("expected name test-group, got %v", result["name"])
	}
	if result["topic"] != "test-topic" {
		t.Errorf("expected topic test-topic, got %v", result["topic"])
	}

	offsets, ok := result["offsets"].([]any)
	if !ok || len(offsets) != 2 {
		t.Errorf("expected 2 offsets, got %v", result["offsets"])
		return
	}

	// Check first offset
	offset0, ok := offsets[0].(map[string]any)
	if !ok {
		t.Errorf("expected first offset to be map, got %T", offsets[0])
		return
	}
	if offset0["partition"] != int32(0) {
		t.Errorf("expected partition 0, got %v", offset0["partition"])
	}
	if offset0["offset"] != offset1 {
		t.Errorf("expected offset %d, got %v", offset1, offset0["offset"])
	}
	if offset0["lag"] != lag1 {
		t.Errorf("expected lag %d, got %v", lag1, offset0["lag"])
	}
}

func TestGroupOffsetToMap(t *testing.T) {
	offset := int64(100)
	lag := int64(10)

	o := groupOffset{
		Partition: 0,
		Offset:    &offset,
		Lag:       &lag,
	}

	result := o.ToMap()
	expected := map[string]any{
		"partition": int32(0),
		"offset":    offset,
		"lag":       lag,
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

func TestGroupOffsetToMapWithNils(t *testing.T) {
	o := groupOffset{
		Partition: 1,
		Offset:    nil,
		Lag:       nil,
	}

	result := o.ToMap()
	expected := map[string]any{
		"partition": int32(1),
		"offset":    (*int64)(nil),
		"lag":       (*int64)(nil),
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
