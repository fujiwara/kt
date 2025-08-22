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

