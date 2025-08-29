package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kong"
)

const AppVersion = "v15.0.0"

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
	var runErr error
	switch sub {
	case "consume":
		runErr = cli.Consume.run()
	case "topic":
		runErr = cli.Topic.run()
	case "admin":
		runErr = cli.Admin.run()
	case "group":
		runErr = cli.Group.run()
	case "produce":
		runErr = cli.Produce.run()
	}
	if runErr != nil {
		failf("%v", runErr)
	}
}

func parseKong(args []string) (string, *CLI, error) {
	var cli CLI
	parser, err := kong.New(
		&cli,
		kong.Vars{"version": versionMessage},
		kong.WithHyphenPrefixedParameters(true),
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create kong parser: %w", err)
	}
	kongCtx, err := parser.Parse(args[1:])
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse args: %w", err)
	}
	return strings.Fields(kongCtx.Command())[0], &cli, nil
}
