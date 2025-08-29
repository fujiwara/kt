package main

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type groupCmd struct {
	baseCmd

	Topic        string         `help:"Topic to consume." env:"KT_TOPIC"`
	Group        string         `help:"Consumer group name."`
	FilterGroups *regexp.Regexp `help:"Regex to filter groups."`
	FilterTopics *regexp.Regexp `help:"Regex to filter topics."`
	Reset        string         `help:"Target offset to reset for consumer group (\"newest\", \"oldest\", a time, or specific offset)"`
	Partitions   string         `help:"Comma separated list of partitions to limit offsets to, or all" default:"all"`
	Offsets      bool           `help:"Controls if offsets should be fetched" default:"true"`

	brokers    []string
	partitions []int32
	reset      int64
	resetTime  bool
	client     sarama.Client
}

type group struct {
	Name    string        `json:"name"`
	Topic   string        `json:"topic,omitempty"`
	Offsets []groupOffset `json:"offsets,omitempty"`
}

func (g group) ToMap() map[string]any {
	off := make([]any, len(g.Offsets))
	for i, o := range g.Offsets {
		off[i] = o.ToMap()
	}
	m := map[string]any{
		"name": g.Name,
	}
	if g.Topic != "" {
		m["topic"] = g.Topic
	}
	if len(off) > 0 {
		m["offsets"] = off
	}
	return m
}

type groupOffset struct {
	Partition int32  `json:"partition"`
	Offset    *int64 `json:"offset"`
	Lag       *int64 `json:"lag"`
}

func (o groupOffset) ToMap() map[string]any {
	return map[string]any{
		"partition": o.Partition,
		"offset":    ptrToValue(o.Offset),
		"lag":       ptrToValue(o.Lag),
	}
}

const (
	allPartitionsHuman = "all"
	resetNotSpecified  = -23
)

func (cmd *groupCmd) run() error {
	var err error

	if err = cmd.prepare(); err != nil {
		return err
	}

	if cmd.Verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cfg, err := cmd.saramaConfig()
	if err != nil {
		return err
	}
	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		return fmt.Errorf("failed to create client err=%v", err)
	}

	brokers := cmd.client.Brokers()
	cmd.infof("found %v brokers\n", len(brokers))

	groups := []string{cmd.Group}
	if cmd.Group == "" {
		groups = []string{}
		allGroups, err := cmd.findGroups(brokers)
		if err != nil {
			return err
		}
		for _, g := range allGroups {
			if cmd.FilterGroups == nil || cmd.FilterGroups.MatchString(g) {
				groups = append(groups, g)
			}
		}
	}
	cmd.infof("found %v groups\n", len(groups))

	topics := []string{cmd.Topic}
	if cmd.Topic == "" {
		topics = []string{}
		allTopics, err := cmd.fetchTopics()
		if err != nil {
			return err
		}
		for _, t := range allTopics {
			if cmd.FilterTopics == nil || cmd.FilterTopics.MatchString(t) {
				topics = append(topics, t)
			}
		}
	}
	cmd.infof("found %v topics\n", len(topics))

	out := make(chan printContext)
	go print(out, cmd.Pretty)

	if !cmd.Offsets {
		for i, grp := range groups {
			ctx := printContext{output: group{Name: grp}, done: make(chan struct{}), cmd: cmd.baseCmd}
			out <- ctx
			<-ctx.done

			cmd.infof("%v/%v\n", i+1, len(groups))
		}
		return nil
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		parts := cmd.partitions
		if len(parts) == 0 {
			var err error
			parts, err = cmd.fetchPartitions(topic)
			if err != nil {
				return err
			}
			cmd.infof("found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = parts
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(groups) * len(topics))
	for _, grp := range groups {
		for top, parts := range topicPartitions {
			go func(grp, topic string, partitions []int32) {
				cmd.printGroupTopicOffset(out, grp, topic, partitions)
				wg.Done()
			}(grp, top, parts)
		}
	}
	wg.Wait()
	return nil
}

func (cmd *groupCmd) printGroupTopicOffset(out chan printContext, grp, top string, parts []int32) {
	target := group{Name: grp, Topic: top, Offsets: make([]groupOffset, 0, len(parts))}
	results := make(chan groupOffset)
	done := make(chan struct{})

	wg := &sync.WaitGroup{}
	wg.Add(len(parts))
	for _, part := range parts {
		go cmd.fetchGroupOffset(wg, grp, top, part, results)
	}
	go func() { wg.Wait(); close(done) }()

awaitGroupOffsets:
	for {
		select {
		case res := <-results:
			target.Offsets = append(target.Offsets, res)
		case <-done:
			break awaitGroupOffsets
		}
	}

	if len(target.Offsets) > 0 {
		sort.Slice(target.Offsets, func(i, j int) bool {
			return target.Offsets[j].Partition > target.Offsets[i].Partition
		})
		ctx := printContext{output: target, done: make(chan struct{}), cmd: cmd.baseCmd}
		out <- ctx
		<-ctx.done
	}
}

func (cmd *groupCmd) resolveOffset(top string, part int32, time int64) (int64, error) {
	resolvedOff, err := cmd.client.GetOffset(top, part, time)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset to reset to for partition=%d err=%v", part, err)
	}

	cmd.infof("resolved offset %v for topic=%s partition=%d to %v\n", time, top, part, resolvedOff)

	return resolvedOff, nil
}

func (cmd *groupCmd) fetchGroupOffset(wg *sync.WaitGroup, grp, top string, part int32, results chan<- groupOffset) {
	defer wg.Done()

	cmd.infof("fetching offset information for group=%v topic=%v partition=%v\n", grp, top, part)

	offsetManager, err := sarama.NewOffsetManagerFromClient(grp, cmd.client)
	if err != nil {
		warnf("failed to create client err=%v", err)
		return
	}
	defer logClose("offset manager", offsetManager)

	pom, err := offsetManager.ManagePartition(top, part)
	if err != nil {
		warnf("failed to manage partition group=%s topic=%s partition=%d err=%v", grp, top, part, err)
		return
	}
	defer logClose("partition offset manager", pom)

	specialOffset := cmd.resetTime || cmd.reset == sarama.OffsetNewest || cmd.reset == sarama.OffsetOldest

	groupOff, _ := pom.NextOffset()
	if cmd.reset >= 0 || specialOffset {
		resolvedOff := cmd.reset
		if specialOffset {
			var err error
			if resolvedOff, err = cmd.resolveOffset(top, part, cmd.reset); err != nil {
				warnf("failed to resolve offset: %v", err)
				return
			}
		}
		if resolvedOff > groupOff {
			pom.MarkOffset(resolvedOff, "")
		} else {
			pom.ResetOffset(resolvedOff, "")
		}

		groupOff = resolvedOff
	}

	partOff, err := cmd.resolveOffset(top, part, sarama.OffsetNewest)
	if err != nil {
		warnf("failed to resolve partition offset: %v", err)
		return
	}
	lag := partOff - groupOff
	results <- groupOffset{Partition: part, Offset: &groupOff, Lag: &lag}
}

func (cmd *groupCmd) fetchTopics() ([]string, error) {
	tps, err := cmd.client.Topics()
	if err != nil {
		return nil, fmt.Errorf("failed to read topics err=%v", err)
	}
	return tps, nil
}

func (cmd *groupCmd) fetchPartitions(top string) ([]int32, error) {
	ps, err := cmd.client.Partitions(top)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions for topic=%s err=%v", top, err)
	}
	return ps, nil
}

type findGroupResult struct {
	done  bool
	group string
}

func (cmd *groupCmd) findGroups(brokers []*sarama.Broker) ([]string, error) {
	var (
		doneCount int
		groups    = []string{}
		results   = make(chan findGroupResult)
		errs      = make(chan error)
	)

	for _, broker := range brokers {
		go cmd.findGroupsOnBroker(broker, results, errs)
	}

awaitGroups:
	for {
		if doneCount == len(brokers) {
			return groups, nil
		}

		select {
		case err := <-errs:
			return nil, fmt.Errorf("failed to find groups err=%v", err)
		case res := <-results:
			if res.done {
				doneCount++
				continue awaitGroups
			}
			groups = append(groups, res.group)
		}
	}
}

func (cmd *groupCmd) findGroupsOnBroker(broker *sarama.Broker, results chan findGroupResult, errs chan error) {
	var (
		err  error
		resp *sarama.ListGroupsResponse
	)
	if err = cmd.connect(broker); err != nil {
		errs <- fmt.Errorf("failed to connect to broker %#v err=%s\n", broker.Addr(), err)
	}

	if resp, err = broker.ListGroups(&sarama.ListGroupsRequest{}); err != nil {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), err)
	}

	if resp.Err != sarama.ErrNoError {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), resp.Err)
	}

	for name := range resp.Groups {
		results <- findGroupResult{group: name}
	}
	results <- findGroupResult{done: true}
}

func (cmd *groupCmd) connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	cfg, err := cmd.saramaConfig()
	if err != nil {
		return err
	}
	if err := broker.Open(cfg); err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}

	return nil
}

func (cmd *groupCmd) saramaConfig() (*sarama.Config, error) {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.getKafkaVersion()
	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-group-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.baseCmd.auth, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup auth err=%v", err)
	}

	return cfg, nil
}

func (cmd *groupCmd) prepare() error {
	if err := cmd.baseCmd.prepare(); err != nil {
		return fmt.Errorf("failed to prepare jq query err=%v", err)
	}
	switch cmd.Partitions {
	case "", "all":
		cmd.partitions = []int32{}
		if cmd.Partitions != "" {
			cmd.infof("Parsed --partitions %q as: all partitions\n", cmd.Partitions)
		}
	default:
		pss := strings.Split(cmd.Partitions, ",")
		for _, ps := range pss {
			p, err := strconv.ParseInt(ps, 10, 32)
			if err != nil {
				return fmt.Errorf("partition id invalid err=%v", err)
			}
			cmd.partitions = append(cmd.partitions, int32(p))
		}
		cmd.infof("Parsed --partitions %q as: %v\n", cmd.Partitions, cmd.partitions)
	}

	if cmd.partitions == nil {
		return fmt.Errorf(`failed to interpret partitions flag %#v. Should be a comma separated list of partitions or "all"`, cmd.Partitions)
	}

	cmd.brokers = cmd.addDefaultPorts(cmd.Brokers)

	if cmd.Reset != "" && (cmd.Topic == "" || cmd.Group == "") {
		return fmt.Errorf("group and topic are required to reset offsets")
	}

	cmd.resetTime = false

	switch cmd.Reset {
	case "newest":
		cmd.reset = sarama.OffsetNewest
		cmd.infof("Parsed --reset %q as: newest offset\n", cmd.Reset)
	case "oldest":
		cmd.reset = sarama.OffsetOldest
		cmd.infof("Parsed --reset %q as: oldest offset\n", cmd.Reset)
	case "":
		// optional flag
		cmd.reset = resetNotSpecified
	default:
		var err error
		cmd.reset, err = strconv.ParseInt(cmd.Reset, 10, 64)
		if err != nil {
			// Try parsing as time string (now, RFC3339, or relative duration)
			var dt *time.Time
			dt, err = parseTimeString(cmd.Reset)
			if err == nil {
				cmd.reset = dt.UnixMilli()
				cmd.resetTime = true
				cmd.infof("Parsed --reset %q as timestamp: %s (milliseconds: %d)\n", cmd.Reset, dt.Format(time.RFC3339), cmd.reset)
			}
		} else {
			cmd.infof("Parsed --reset %q as specific offset: %d\n", cmd.Reset, cmd.reset)
		}
		if err != nil {
			warnf("failed to parse set %#v err=%v", cmd.Reset, err)
			return fmt.Errorf(`set value %#v not valid. either "newest", "oldest", a time, or a specific offset expected. Supported time formats: "now", RFC3339 (2006-01-02T15:04:05Z07:00), or relative duration (+5m, -1h)`, cmd.Reset)
		}
	}
	return nil
}
