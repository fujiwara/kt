package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
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
			env:      "v2.0.0",
			expected: sarama.V2_0_0_0,
		},
		"arg v1 wins over env v2": {
			arg:      "v1.0.0",
			env:      "v2.0.0",
			expected: sarama.V1_0_0_0,
		},
		"invalid": {
			arg: "234",
			err: fmt.Errorf("invalid version `234`"),
		},
	}

	for tn, tc := range td {
		actual, err := chooseKafkaVersion(tc.arg, tc.env)
		if tc.err == nil {
			require.Equal(t, tc.expected, actual, tn)
			require.NoError(t, err)
		} else {
			require.Equal(t, tc.err, err)
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
