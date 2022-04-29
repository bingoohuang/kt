package main

import (
	"fmt"
	"os"

	"github.com/bingoohuang/gg/pkg/v"
	. "github.com/bingoohuang/kt/pkg/kt"
)

var usageMessage = fmt.Sprintf(`kt is a tool for Kafka.
Usage:
	kt command [arguments]

The commands are:
	consume    consume messages.
	produce    produce messages.
	topic      topic information.
	group      consumer group information and modification.
	admin      basic cluster administration.

Use "kt [command] -help" for more information about the command.
Use "kt -version" for details on what version you are running.

Authentication:
Authentication with Kafka can be configured via a JSON file.
You can set the file name via an "-auth" flag to each command or
set it via the environment variable %s.

You can find more details at https://github.com/bingoohuang/kt
`, EnvAuth)

func parseArgs() command {
	if len(os.Args) < 2 {
		failf(usageMessage)
	}

	switch os.Args[1] {
	case "consume", "tail":
		return &consumeCmd{}
	case "produce":
		return &produceCmd{}
	case "topic":
		return &topicCmd{}
	case "group":
		return &groupCmd{}
	case "admin":
		return &adminCmd{}
	case "-h", "-help", "--help":
		quitf(usageMessage)
	case "-version", "--version":
		quitf(v.Version())
	default:
		failf(usageMessage)
	}
	return nil
}

func main() {
	cmd := parseArgs()
	cmd.run(os.Args[2:])
}
