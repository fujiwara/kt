package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/term"
)

func TestBufferedOutputInitialization(t *testing.T) {
	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Test when stdout is a terminal (should not buffer)
	if term.IsTerminal(int(syscall.Stdout)) {
		if stdoutWriter != os.Stdout {
			t.Error("stdoutWriter should be os.Stdout when terminal is detected")
		}
		if stdoutBuffer != nil {
			t.Error("stdoutBuffer should be nil when terminal is detected")
		}
	} else {
		// Test when stdout is not a terminal (should buffer)
		if stdoutBuffer == nil {
			t.Error("stdoutBuffer should not be nil when terminal is not detected")
		}
		if stdoutWriter == os.Stdout {
			t.Error("stdoutWriter should not be os.Stdout when terminal is not detected")
		}
	}
}

func TestBufferedOutput(t *testing.T) {
	// Create a buffer to capture output
	var buf bytes.Buffer

	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Set up buffered writer for testing
	stdoutBuffer = bufio.NewWriter(&buf)
	stdoutWriter = stdoutBuffer

	// Test outf function
	outf("test message %d", 123)

	// Before flush, buffer should contain the data but buf should be empty
	if buf.Len() > 0 {
		t.Error("Buffer should be empty before flush when using buffered writer")
	}

	// After flush, data should be written to the underlying writer
	flushOutput()

	expected := "test message 123"
	if !strings.Contains(buf.String(), expected) {
		t.Errorf("Expected output to contain %q, got %q", expected, buf.String())
	}
}

func TestUnbufferedOutput(t *testing.T) {
	// Create a buffer to capture output
	var buf bytes.Buffer

	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Set up unbuffered writer for testing (direct to buffer)
	stdoutBuffer = nil
	stdoutWriter = &buf

	// Test outf function
	outf("test message %d", 456)

	// With unbuffered writer, data should be immediately available
	expected := "test message 456"
	if !strings.Contains(buf.String(), expected) {
		t.Errorf("Expected output to contain %q, got %q", expected, buf.String())
	}
}

func TestFlushOutput(t *testing.T) {
	// Create a buffer to capture output
	var buf bytes.Buffer

	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Set up buffered writer for testing
	stdoutBuffer = bufio.NewWriter(&buf)
	stdoutWriter = stdoutBuffer

	// Write some data
	outf("line 1\n")
	outf("line 2\n")

	// Before flush, buffer should be empty
	if buf.Len() > 0 {
		t.Error("Buffer should be empty before flush")
	}

	// Flush should write all data
	flushOutput()

	output := buf.String()
	if !strings.Contains(output, "line 1") {
		t.Error("Output should contain 'line 1'")
	}
	if !strings.Contains(output, "line 2") {
		t.Error("Output should contain 'line 2'")
	}
}

func TestFlushOutputSafety(t *testing.T) {
	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Test flush with nil buffer (should not panic)
	stdoutBuffer = nil
	flushOutput() // Should not panic

	// Test multiple flushes (should not panic)
	var buf bytes.Buffer
	stdoutBuffer = bufio.NewWriter(&buf)
	stdoutWriter = stdoutBuffer

	flushOutput()
	flushOutput()
	flushOutput() // Multiple flushes should be safe
}

// Mock writer that tracks write calls
type mockWriter struct {
	writes []string
	closed bool
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.writes = append(m.writes, string(p))
	return len(p), nil
}

func TestConcurrentOutput(t *testing.T) {
	// Save original state
	originalStdoutWriter := stdoutWriter
	originalStdoutBuffer := stdoutBuffer
	defer func() {
		stdoutWriter = originalStdoutWriter
		stdoutBuffer = originalStdoutBuffer
	}()

	// Set up mock writer
	mock := &mockWriter{}
	stdoutBuffer = bufio.NewWriter(mock)
	stdoutWriter = stdoutBuffer

	// Test concurrent writes
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 10; i++ {
			outf("goroutine1-%d\n", i)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			outf("goroutine2-%d\n", i)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Flush to get all output
	flushOutput()

	// Should have received some writes (exact order not guaranteed due to concurrency)
	if len(mock.writes) == 0 {
		t.Error("Expected some writes from concurrent goroutines")
	}
}
