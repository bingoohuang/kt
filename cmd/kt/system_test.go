package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
	. "github.com/bingoohuang/kt/pkg/kt"
	"github.com/stretchr/testify/require"
)

type cmd struct {
	in string
}

func newCmd() *cmd                  { return &cmd{} }
func (c *cmd) stdIn(in string) *cmd { c.in = in; return c }
func (c *cmd) run(name string, args ...string) (int, string, string) {
	cmd := exec.Command(name, args...)

	var stdOut, stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("%s=localhost:9092", EnvBrokers),
		fmt.Sprintf("%s=testdata/test-secrets/auth.json", EnvAuth),
		"GODEBUG=x509ignoreCN=0")

	if len(c.in) > 0 {
		cmd.Stdin = strings.NewReader(c.in)
	}

	_ = cmd.Run()
	status := cmd.ProcessState.Sys().(syscall.WaitStatus)

	strOut := stdOut.String()
	strErr := stdErr.String()

	return status.ExitStatus(), strOut, strErr
}

func build(t *testing.T) {
	status, _, _ := newCmd().run("go", "build")
	require.Zero(t, status)

	status, _, _ = newCmd().run("ls", "kt")
	require.Zero(t, status)
}

func TestSystem(t *testing.T) {
	build(t)

	var err error
	var status int
	var stdOut, stdErr string

	// kt admin topic.create
	topicName := fmt.Sprintf("kt-test-%v", randomString(6))
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	buf, err := json.Marshal(topicDetail)
	require.NoError(t, err)
	fnTopicDetail := fmt.Sprintf("topic-detail-%v.json", randomString(6))
	err = ioutil.WriteFile(fnTopicDetail, buf, 0o666)
	require.NoError(t, err)
	defer os.RemoveAll(fnTopicDetail)

	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "admin", "-topic.create", topicName, "-topic.config", "@"+fnTopicDetail)
	fmt.Printf(">> system test kt admin -topic.create %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt admin -topic.create %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	fmt.Printf(">> ✓\n")
	// kt produce

	req := map[string]any{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       "boom",
		"partition": float64(0),
	}
	buf, err = json.Marshal(req)
	require.NoError(t, err)
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "produce", "-topic", topicName)
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

	// kt consume
	status, stdOut, stdErr = newCmd().run("./kt", "consume", "-topic", topicName, "-timeout", "500ms", "-group", "hans")
	fmt.Printf(">> system test kt consume -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt consume -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines := strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var lastConsumed map[string]any
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

	// kt group
	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName)
	fmt.Printf(">> system test kt group -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt group -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Contains(t, stdErr, fmt.Sprintf("found partitions=[0] for topic=%v", topicName))
	require.Contains(t, stdOut, fmt.Sprintf(`{"name":"hans","topic":"%v","offsets":[{"partition":0,"Offset":1,"lag":0}]}`, topicName))

	fmt.Printf(">> ✓\n")

	// kt produce
	req = map[string]any{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       "boom",
		"partition": float64(0),
	}
	buf, err = json.Marshal(req)
	require.NoError(t, err)
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "produce", "-topic", topicName)
	fmt.Printf(">> system test kt produce -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt produce -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	require.NoError(t, err)
	require.Equal(t, 1, produceMessage["count"])
	require.Equal(t, 0, produceMessage["partition"])
	require.Equal(t, 1, produceMessage["startOffset"])

	fmt.Printf(">> ✓\n")

	// kt consume
	status, stdOut, stdErr = newCmd().run("./kt", "consume", "-topic", topicName, "-offsets", "all=resume", "-timeout", "500ms", "-group", "hans")
	fmt.Printf(">> system test kt consume -topic %v -offsets all=resume stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt consume -topic %v -offsets all=resume stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) == 2) // actual line and an empty one

	err = json.Unmarshal([]byte(lines[len(lines)-2]), &lastConsumed)
	require.NoError(t, err)
	require.Equal(t, req["value"], lastConsumed["value"])
	require.Equal(t, req["key"], lastConsumed["key"])
	require.Equal(t, req["partition"], lastConsumed["partition"])
	require.NotEmpty(t, lastConsumed["timestamp"])
	pt, err = time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	require.NoError(t, err)
	require.True(t, pt.After(time.Now().Add(-2*time.Minute)))

	fmt.Printf(">> ✓\n")

	// kt group reset
	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName, "-partitions", "0", "-group", "hans", "-reset", "0")
	fmt.Printf(">> system test kt group -topic %v -partitions 0 -group hans -reset 0 stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt group -topic %v -partitions 0 -group hans -reset 0  stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var groupReset map[string]any
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &groupReset)
	require.NoError(t, err)

	require.Equal(t, groupReset["name"], "hans")
	require.Equal(t, groupReset["topic"], topicName)
	require.Len(t, groupReset["offsets"], 1)
	offsets := groupReset["offsets"].([]any)[0].(map[string]any)
	require.Equal(t, offsets["partition"], float64(0))
	require.Equal(t, offsets["Offset"], float64(0))

	fmt.Printf(">> ✓\n")

	// kt group
	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName)
	fmt.Printf(">> system test kt group -topic %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt group -topic %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Contains(t, stdErr, fmt.Sprintf("found partitions=[0] for topic=%v", topicName))
	require.Contains(t, stdOut, fmt.Sprintf(`{"name":"hans","topic":"%v","offsets":[{"partition":0,"Offset":0,"lag":2}]}`, topicName))

	fmt.Printf(">> ✓\n")

	// kt topic
	status, stdOut, stdErr = newCmd().run("./kt", "topic", "-filter", topicName)
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

	// kt admin -topic.delete
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "admin", "-topic.delete", topicName)
	fmt.Printf(">> system test kt admin -topic.delete %v stdout:\n%s\n", topicName, stdOut)
	fmt.Printf(">> system test kt admin -topic.delete %v stderr:\n%s\n", topicName, stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)

	fmt.Printf(">> ✓\n")

	// kt topic
	status, stdOut, stdErr = newCmd().run("./kt", "topic", "-filter", topicName)
	fmt.Printf(">> system test kt topic stdout:\n%s\n", stdOut)
	fmt.Printf(">> system test kt topic stderr:\n%s\n", stdErr)
	require.Zero(t, status)
	require.Empty(t, stdErr)
	require.Empty(t, stdOut)

	fmt.Printf(">> ✓\n")
}
