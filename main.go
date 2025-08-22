package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kong"
)

const AppVersion = "v14.2.0"

var buildVersion, buildTime string

var versionMessage = fmt.Sprintf(`kt version %s`, AppVersion)

func init() {
	if buildVersion == "" && buildTime == "" {
		return
	}

	versionMessage += " ("
	if buildVersion != "" {
		versionMessage += buildVersion
	}

	if buildTime != "" {
		if buildVersion != "" {
			versionMessage += " @ "
		}
		versionMessage += buildTime
	}
	versionMessage += ")"
}

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

You can find more details at https://github.com/fgeller/kt

%s`, ENV_AUTH, versionMessage)

type CLI struct {
	Consume *consumeCmd      `cmd:"" help:"consume messages."`
	Produce *produceCmd      `cmd:"" help:"produce messages."`
	Topic   *topicCmd        `cmd:"" help:"topic information."`
	Group   *groupCmd        `cmd:"" help:"consumer group information and modification."`
	Admin   *adminCmd        `cmd:"" help:"basic cluster administration."`
	Version kong.VersionFlag `short:"v" help:"Show version and exit."`
}

func main() {
	defer flushOutput()
	sub, cli, err := parseKong(os.Args)
	if err != nil {
		failf(err.Error())
	}
	switch sub {
	case "consume":
		cli.Consume.run()
	case "topic":
		cli.Topic.run()
	case "admin":
		cli.Admin.run()
	case "group":
		cli.Group.run()
	case "produce":
		cli.Produce.run()
	default:
		mainLegacy(os.Args)
	}
}

func parseKong(args []string) (string, *CLI, error) {
	var cli CLI
	parser, err := kong.New(&cli, kong.Vars{"version": versionMessage})
	if err != nil {
		return "", nil, fmt.Errorf("failed to create kong parser: %w", err)
	}
	kongCtx, err := parser.Parse(args[1:])
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse args: %w", err)
	}
	return strings.Fields(kongCtx.Command())[0], &cli, nil
}

func mainLegacy(args []string) {
	if len(args) < 2 {
		failf(usageMessage)
	}
	var cmd command
	switch args[1] {
	case "consume":
		// cmd = &consumeCmd{}
		failf("consume command is now handled by Kong")
	case "produce":
		// cmd = &produceCmd{}
		failf("produce command is now handled by Kong")
	case "topic":
		// cmd = &topicCmd{}
		failf("xxx")
	case "group":
		// cmd = &groupCmd{}
		failf("group command is now handled by Kong")
	case "admin":
		// cmd = &adminCmd{}
		failf("admin command is now handled by Kong")
	case "-h", "-help", "--help":
		quitf(usageMessage)
	case "-version", "--version":
		quitf(versionMessage)
	default:
		failf(usageMessage)
	}

	cmd.run(args[2:])
}
