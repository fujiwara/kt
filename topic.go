package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

type topicArgs struct {
	brokers    string
	auth       string
	filter     string
	partitions bool
	leaders    bool
	replicas   bool
	config     bool
	verbose    bool
	pretty     bool
	version    string
	jq         string
	raw        bool
}

type topicCmd struct {
	baseCmd

	brokers    []string
	auth       authConfig
	filter     *regexp.Regexp
	partitions bool
	leaders    bool
	replicas   bool
	config     bool
	pretty     bool
	version    sarama.KafkaVersion

	client sarama.Client
	admin  sarama.ClusterAdmin
}

type topic struct {
	Name       string            `json:"name"`
	Partitions []partition       `json:"partitions,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
}

func (t topic) ToMap() map[string]any {
	ps := make([]any, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		ps = append(ps, p.ToMap())
	}
	m := map[string]any{
		"name": t.Name,
	}
	if len(t.Partitions) > 0 {
		m["partitions"] = ps
	}
	if len(t.Config) > 0 {
		m["config"] = t.Config
	}
	return m
}

type partition struct {
	Id           int32   `json:"id"`
	OldestOffset int64   `json:"oldest"`
	NewestOffset int64   `json:"newest"`
	Leader       string  `json:"leader,omitempty"`
	Replicas     []int32 `json:"replicas,omitempty"`
	ISRs         []int32 `json:"isrs,omitempty"`
}

func (p partition) ToMap() map[string]any {
	m := map[string]any{
		"id":     p.Id,
		"oldest": p.OldestOffset,
		"newest": p.NewestOffset,
	}
	if p.Leader != "" {
		m["leader"] = p.Leader
	}
	if len(p.Replicas) > 0 {
		replicas := make([]any, len(p.Replicas))
		for i, r := range p.Replicas {
			replicas[i] = r
		}
		m["replicas"] = replicas
	}
	if len(p.ISRs) > 0 {
		isrs := make([]any, len(p.ISRs))
		for i, isr := range p.ISRs {
			isrs[i] = isr
		}
		m["isrs"] = isrs
	}
	return m
}

func (cmd *topicCmd) parseFlags(as []string) topicArgs {
	var (
		args  topicArgs
		flags = flag.NewFlagSet("topic", flag.ContinueOnError)
	)

	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.StringVar(&args.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", ENV_AUTH))
	flags.BoolVar(&args.partitions, "partitions", false, "Include information per partition.")
	flags.BoolVar(&args.leaders, "leaders", false, "Include leader information per partition.")
	flags.BoolVar(&args.replicas, "replicas", false, "Include replica ids per partition.")
	flags.BoolVar(&args.config, "config", false, "Include topic configuration.")
	flags.StringVar(&args.filter, "filter", "", "Regex to filter topics by name.")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.jq, "jq", "", "Apply jq filter to output (e.g., '.name').")
	flags.BoolVar(&args.raw, "raw", false, "Output raw strings without JSON encoding (like jq -r).")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, topicDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

func (cmd *topicCmd) parseArgs(as []string) {
	var (
		err error
		re  *regexp.Regexp

		args       = cmd.parseFlags(as)
		envBrokers = os.Getenv(ENV_BROKERS)
	)
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}

	if re, err = regexp.Compile(args.filter); err != nil {
		failf("invalid regex for filter err=%s", err)
	}

	readAuthFile(args.auth, os.Getenv(ENV_AUTH), &cmd.auth)

	cmd.filter = re
	cmd.partitions = args.partitions
	cmd.leaders = args.leaders
	cmd.replicas = args.replicas
	cmd.config = args.config
	cmd.pretty = args.pretty
	cmd.verbose = args.verbose
	cmd.jq = args.jq
	cmd.raw = args.raw
	if err := cmd.prepare(); err != nil {
		failf("failed to prepare jq query err=%v", err)
	}

	cmd.version, err = chooseKafkaVersion(args.version, os.Getenv(ENV_KAFKA_VERSION))
	if err != nil {
		failf("failed to read kafka version err=%v", err)
	}
}

func (cmd *topicCmd) connect() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version

	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-topic-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
	if cmd.admin, err = sarama.NewClusterAdmin(cmd.brokers, cfg); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}
}

func (cmd *topicCmd) run(as []string) {
	var (
		err error
		all []string
		out = make(chan printContext)
	)

	cmd.parseArgs(as)
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.connect()
	defer cmd.client.Close()
	defer cmd.admin.Close()

	if all, err = cmd.client.Topics(); err != nil {
		failf("failed to read topics err=%v", err)
	}

	topics := []string{}
	for _, a := range all {
		if cmd.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	go print(out, cmd.pretty)

	var wg sync.WaitGroup
	for _, tn := range topics {
		wg.Add(1)
		go func(top string) {
			cmd.print(top, out)
			wg.Done()
		}(tn)
	}
	wg.Wait()
}

func (cmd *topicCmd) print(name string, out chan printContext) {
	var (
		top topic
		err error
	)

	if top, err = cmd.readTopic(name); err != nil {
		warnf("failed to read info for topic %s. err=%v\n", name, err)
		return
	}

	ctx := printContext{output: top, done: make(chan struct{}), cmd: cmd.baseCmd}
	out <- ctx
	<-ctx.done
}

func (cmd *topicCmd) readTopic(name string) (topic, error) {
	var (
		err           error
		ps            []int32
		led           *sarama.Broker
		top           = topic{Name: name}
		configEntries []sarama.ConfigEntry
	)

	if cmd.config {

		resource := sarama.ConfigResource{Name: name, Type: sarama.TopicResource}
		if configEntries, err = cmd.admin.DescribeConfig(resource); err != nil {
			return top, err
		}

		top.Config = make(map[string]string)
		for _, entry := range configEntries {
			top.Config[entry.Name] = entry.Value
		}
	}

	if !cmd.partitions {
		return top, nil
	}

	if ps, err = cmd.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{Id: p}

		if np.OldestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if cmd.leaders {
			if led, err = cmd.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if cmd.replicas {
			if np.Replicas, err = cmd.client.Replicas(name, p); err != nil {
				return top, err
			}

			if np.ISRs, err = cmd.client.InSyncReplicas(name, p); err != nil {
				return top, err
			}
		}

		top.Partitions = append(top.Partitions, np)
	}

	return top, nil
}

var topicDocString = fmt.Sprintf(`
The values for -brokers can also be set via the environment variable %s respectively.
The values supplied on the command line win over environment variable values.`,
	ENV_BROKERS)
