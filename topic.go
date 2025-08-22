package main

import (
	"log"
	"os"
	"os/user"
	"regexp"
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

	Brokers    []string       `help:"Comma separated list of brokers. Port defaults to 9092 when omitted." env:"KT_BROKERS" default:"localhost:9092"`
	Auth       string         `help:"Path to auth configuration file, can also be set via KT_AUTH env variable." env:"KT_AUTH"`
	Filter     *regexp.Regexp `help:"Regex to filter topics by name."`
	Partitions bool           `help:"Include information per partition."`
	Leaders    bool           `help:"Include leader information per partition."`
	Replicas   bool           `help:"Include replica ids per partition."`
	Config     bool           `help:"Include topic configuration."`

	auth   authConfig
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

func (cmd *topicCmd) prepare() {
	readAuthFile(cmd.Auth, os.Getenv(ENV_AUTH), &cmd.auth) // TODO: remove os.Getenv

	if err := cmd.baseCmd.prepare(); err != nil {
		failf("failed to prepare jq query err=%v", err)
	}
}

func (cmd *topicCmd) connect() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.getKafkaVersion()

	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-topic-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	brokers := cmd.addDefaultPorts(cmd.Brokers)

	if cmd.client, err = sarama.NewClient(brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
	if cmd.admin, err = sarama.NewClusterAdmin(brokers, cfg); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}
}

func (cmd *topicCmd) run() {
	var (
		err error
		all []string
		out = make(chan printContext)
	)
	cmd.prepare()
	if cmd.Verbose {
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
		if cmd.Filter == nil || cmd.Filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	go print(out, cmd.Pretty)

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

	if cmd.Config {
		resource := sarama.ConfigResource{Name: name, Type: sarama.TopicResource}
		if configEntries, err = cmd.admin.DescribeConfig(resource); err != nil {
			return top, err
		}

		top.Config = make(map[string]string)
		for _, entry := range configEntries {
			top.Config[entry.Name] = entry.Value
		}
	}

	if !cmd.Partitions {
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

		if cmd.Leaders {
			if led, err = cmd.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if cmd.Replicas {
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
