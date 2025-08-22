//go:build integration

package main

import (
	"bytes"
	"fmt"
	json "github.com/goccy/go-json"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

type cmd struct {
	in string
}

func newCmd() *cmd                  { return &cmd{} }
func (c *cmd) stdIn(in string) *cmd { c.in = in; return c }
func (c *cmd) runWithPort(port int, name string, args ...string) (int, string, string) {
	cmd := exec.Command(name, args...)

	var stdOut, stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=localhost:%d", ENV_BROKERS, port))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=test-secrets/auth.json", ENV_AUTH))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=v3.0.0", ENV_KAFKA_VERSION))

	if len(c.in) > 0 {
		cmd.Stdin = strings.NewReader(c.in)
	}

	_ = cmd.Run()
	status := cmd.ProcessState.Sys().(syscall.WaitStatus)

	strOut := stdOut.String()
	strErr := stdErr.String()

	return status.ExitStatus(), strOut, strErr
}

func (c *cmd) run(name string, args ...string) (int, string, string) {
	return c.runWithPort(9092, name, args...)
}

type testConfig struct {
	topicPrefix string
	keyValue    string
	groupName   string
	useSSL      bool
	authFile    string
	isFullTest  bool
}

func build(t *testing.T) {
	var status int

	status, _, _ = newCmd().run("make", "build")
	require.Zero(t, status)

	status, _, _ = newCmd().run("ls", "kt")
	require.Zero(t, status)
}

func runSystemTest(t *testing.T, config testConfig) {
	build(t)

	var err error
	var status int
	var stdOut, stdErr string

	runMethod := func(c *cmd, name string, args ...string) (int, string, string) {
		if config.authFile != "" {
			args = append(args, "--auth", config.authFile)
		}
		port := 9092
		if config.useSSL {
			port = 9093
		}
		return c.runWithPort(port, name, args...)
	}

	//
	// kt admin -createtopic
	//
	topicName := fmt.Sprintf("%s-%v", config.topicPrefix, randomString(6))
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	buf, err := json.Marshal(topicDetail)
	require.NoError(t, err)
	fnTopicDetail := fmt.Sprintf("topic-detail-%v.json", randomString(6))
	err = os.WriteFile(fnTopicDetail, buf, 0666)
	require.NoError(t, err)
	defer os.RemoveAll(fnTopicDetail)

	status, stdOut, stdErr = runMethod(newCmd().stdIn(string(buf)), "./kt", "admin",
		"--create-topic", topicName,
		"--topic-detail", fnTopicDetail)
	fmt.Printf(">> system test kt admin -createtopic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt admin -createtopic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	fmt.Printf(">> ✓\n")
	//
	// kt produce
	//

	req := map[string]interface{}{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       config.keyValue,
		"partition": float64(0),
	}
	buf, err = json.Marshal(req)
	require.NoError(t, err)
	status, stdOut, stdErr = runMethod(newCmd().stdIn(string(buf)), "./kt", "produce",
		"--topic", topicName)
	fmt.Printf(">> system test kt produce -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt produce -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	var produceMessage map[string]int
	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	require.NoError(t, err)
	require.Equal(t, 1, produceMessage["count"])
	require.Equal(t, 0, produceMessage["partition"])
	require.Equal(t, 0, produceMessage["startOffset"])

	fmt.Printf(">> ✓\n")
	//
	// kt consume
	//

	status, stdOut, stdErr = runMethod(newCmd(), "./kt", "consume",
		"--topic", topicName,
		"--timeout", "5s",
		"--offsets", "all=oldest",
		"--group", config.groupName)
	fmt.Printf(">> system test kt consume -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt consume -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines := strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var lastConsumed map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &lastConsumed)
	require.NoError(t, err)
	require.Equal(t, req["value"], lastConsumed["value"])
	require.Equal(t, req["key"], lastConsumed["key"])
	require.Equal(t, req["partition"], lastConsumed["partition"])
	require.NotEmpty(t, lastConsumed["timestamp"])
	pt, err := time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	require.NoError(t, err)
	require.True(t, pt.After(time.Now().Add(-2*time.Minute)))

	fmt.Printf(">> ✓\n")

	if config.isFullTest {
		runFullSystemTest(t, config, topicName, runMethod, req)
	}

	//
	// kt admin -deletetopic
	//
	status, stdOut, stdErr = runMethod(newCmd().stdIn(string(buf)), "./kt", "admin",
		"--delete-topic", topicName)
	fmt.Printf(">> system test kt admin -deletetopic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt admin -deletetopic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	fmt.Printf(">> ✓\n")
}

func runFullSystemTest(t *testing.T, config testConfig, topicName string, runMethod func(*cmd, string, ...string) (int, string, string), req map[string]interface{}) {
	var err error
	var status int
	var stdOut, stdErr string

	//
	// kt consume (non-group, direct partition consumption)
	//

	status, stdOut, stdErr = runMethod(newCmd(), "./kt", "consume",
		"--topic", topicName,
		"--timeout", "2s",
		"--offsets", "all=oldest")
	fmt.Printf(">> system test kt consume -topic %v (non-group) stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt consume -topic %v (non-group) stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines := strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var lastConsumed map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &lastConsumed)
	require.NoError(t, err)
	require.Equal(t, req["value"], lastConsumed["value"])
	require.Equal(t, req["key"], lastConsumed["key"])
	require.Equal(t, req["partition"], lastConsumed["partition"])
	require.NotEmpty(t, lastConsumed["timestamp"])
	pt, err := time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	require.NoError(t, err)
	require.True(t, pt.After(time.Now().Add(-2*time.Minute)))

	fmt.Printf(">> ✓\n")
	//
	// kt group
	//

	status, stdOut, stdErr = runMethod(newCmd(), "./kt", "group",
		"--verbose",
		"--topic", topicName)
	fmt.Printf(">> system test kt group -verbose -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt group -verbose -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Contains(t, stdErr, fmt.Sprintf("found partitions=[0] for topic=%v", topicName))
	require.Contains(t, stdOut, fmt.Sprintf(`{"name":"%s","topic":"%v","offsets":[{"partition":0,"offset":1,"lag":0}]}`, config.groupName, topicName))

	fmt.Printf(">> ✓\n")
	//
	// kt produce (second message)
	//

	req2 := map[string]interface{}{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       config.keyValue,
		"partition": float64(0),
	}
	buf, err := json.Marshal(req2)
	require.NoError(t, err)
	status, stdOut, stdErr = runMethod(newCmd().stdIn(string(buf)),
		"./kt", "produce",
		"--topic", topicName,
	)
	fmt.Printf(">> system test kt produce -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt produce -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	var produceMessage map[string]int
	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	require.NoError(t, err)
	require.Equal(t, 1, produceMessage["count"])
	require.Equal(t, 0, produceMessage["partition"])
	require.Equal(t, 1, produceMessage["startOffset"])

	fmt.Printf(">> ✓\n")
	//
	// kt consume (resume)
	//

	status, stdOut, stdErr = runMethod(newCmd(),
		"./kt", "consume",
		"--topic", topicName,
		"--offsets", "all=resume",
		"--timeout", "5s",
		"--group", config.groupName,
	)
	fmt.Printf(">> system test kt consume -topic %v -offsets all=resume stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt consume -topic %v -offsets all=resume stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) == 2) // actual line and an empty one

	err = json.Unmarshal([]byte(lines[len(lines)-2]), &lastConsumed)
	require.NoError(t, err)
	require.Equal(t, req2["value"], lastConsumed["value"])
	require.Equal(t, req2["key"], lastConsumed["key"])
	require.Equal(t, req2["partition"], lastConsumed["partition"])
	require.NotEmpty(t, lastConsumed["timestamp"])
	pt, err = time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	require.NoError(t, err)
	require.True(t, pt.After(time.Now().Add(-2*time.Minute)))

	fmt.Printf(">> ✓\n")
	//
	// kt group reset
	//

	status, stdOut, stdErr = runMethod(newCmd(),
		"./kt", "group",
		"--verbose",
		"--topic", topicName,
		"--partitions", "0",
		"--group", config.groupName,
		"--reset", "0",
	)
	fmt.Printf(">> system test kt group -verbose -topic %v -partitions 0 -group %s -reset 0 stdout:\n%s\n", topicName, config.groupName, stdOut)
	fmt.Printf(">> system test kt group -verbose -topic %v -partitions 0 -group %s -reset 0  stderr:\n%s\n", topicName, config.groupName, stdErr)
	require.Zero(t, status)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var groupReset map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &groupReset)
	require.NoError(t, err)

	require.Equal(t, groupReset["name"], config.groupName)
	require.Equal(t, groupReset["topic"], topicName)
	require.Len(t, groupReset["offsets"], 1)
	offsets := groupReset["offsets"].([]interface{})[0].(map[string]interface{})
	require.Equal(t, offsets["partition"], float64(0))
	require.Equal(t, offsets["offset"], float64(0))

	fmt.Printf(">> ✓\n")
	//
	// kt group (after reset)
	//

	status, stdOut, stdErr = runMethod(newCmd(),
		"./kt", "group",
		"--verbose",
		"--topic", topicName,
	)
	fmt.Printf(">> system test kt group -verbose -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt group -verbose -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Contains(t, stdErr, fmt.Sprintf("found partitions=[0] for topic=%v", topicName))
	require.Contains(t, stdOut, fmt.Sprintf(`{"name":"%s","topic":"%v","offsets":[{"partition":0,"offset":0,"lag":2}]}`, config.groupName, topicName))

	fmt.Printf(">> ✓\n")
	//
	// kt topic
	//

	status, stdOut, stdErr = runMethod(newCmd(),
		"./kt", "topic",
		"--filter", topicName,
	)
	fmt.Printf(">> system test kt topic stdout:\n%s\n", stdOut)
	fmt.Printf(">> system test kt topic stderr:\n%s\n", stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 0)

	expectedLines := []string{
		fmt.Sprintf(`{"name": "%v"}`, topicName),
	}
	sort.Strings(lines)
	sort.Strings(expectedLines)

	for i, l := range lines {
		if l == "" { // final newline
			continue
		}
		require.JSONEq(t, expectedLines[i-1], l, fmt.Sprintf("line %d", i-1))
	}
	fmt.Printf(">> ✓\n")
}

func TestSystem(t *testing.T) {
	config := testConfig{
		topicPrefix: "kt-test",
		keyValue:    "boom",
		groupName:   "hans",
		useSSL:      false,
		authFile:    "",
		isFullTest:  true,
	}
	runSystemTest(t, config)

	// Test final topic verification after delete
	status, stdOut, stdErr := newCmd().run("./kt", "topic", "--filter", "kt-test-nonexistent")
	fmt.Printf(">> system test kt topic stdout:\n%s\n", stdOut)
	fmt.Printf(">> system test kt topic stderr:\n%s\n", stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)
	require.Empty(t, stdOut)

	fmt.Printf(">> ✓\n")
}

func TestSystemSSL(t *testing.T) {
	config := testConfig{
		topicPrefix: "kt-test-ssl",
		keyValue:    "boom-ssl",
		groupName:   "hans-ssl",
		useSSL:      true,
		authFile:    "test-secrets/auth-ssl.json",
		isFullTest:  false, // SSL test runs basic operations only
	}
	runSystemTest(t, config)
}
