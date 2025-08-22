package main

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"time"

	"github.com/IBM/sarama"
	json "github.com/goccy/go-json"
)

type message struct {
	Key       *string `json:"key"`
	Value     *string `json:"value"`
	Partition *int32  `json:"partition"`
}

func (cmd *produceCmd) failStartup(msg string) {
	warnf(msg)
	failf("use \"kt produce -help\" for more information")
}

func (cmd *produceCmd) prepare() {
	if err := cmd.baseCmd.prepare(); err != nil {
		failf("failed to prepare jq query err=%v", err)
	}

	if cmd.Topic == "" {
		cmd.failStartup("Topic name is required.")
	}

	if cmd.DecodeValue != "" && cmd.DecodeValue != "string" && cmd.DecodeValue != "hex" && cmd.DecodeValue != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported decodevalue argument %#v, only string, hex and base64 are supported.`, cmd.DecodeValue))
		return
	}

	if cmd.DecodeKey != "" && cmd.DecodeKey != "string" && cmd.DecodeKey != "hex" && cmd.DecodeKey != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported decodekey argument %#v, only string, hex and base64 are supported.`, cmd.DecodeKey))
		return
	}

	cmd.compression = kafkaCompression(cmd.Compression)
}

func kafkaCompression(codecName string) sarama.CompressionCodec {
	switch codecName {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "":
		return sarama.CompressionNone
	}

	failf("unsupported compression codec %#v - supported: gzip, snappy, lz4", codecName)
	panic("unreachable")
}

func (cmd *produceCmd) findLeaders() {
	var (
		usr *user.User
		err error
		res *sarama.MetadataResponse
		req = sarama.MetadataRequest{Topics: []string{cmd.Topic}}
		cfg = sarama.NewConfig()
	)

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Version = cmd.getKafkaVersion()
	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-produce-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.baseCmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

loop:
	for _, addr := range cmd.addDefaultPorts(cmd.Brokers) {
		broker := sarama.NewBroker(addr)
		if err = broker.Open(cfg); err != nil {
			cmd.infof("Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}
		if connected, err := broker.Connected(); !connected || err != nil {
			cmd.infof("Failed to open broker connection to %v. err=%s\n", addr, err)
			continue loop
		}

		if res, err = broker.GetMetadata(&req); err != nil {
			cmd.infof("Failed to get metadata from %#v. err=%v\n", addr, err)
			continue loop
		}

		brokers := map[int32]*sarama.Broker{}
		for _, b := range res.Brokers {
			brokers[b.ID()] = b
		}

		for _, tm := range res.Topics {
			if tm.Name == cmd.Topic {
				if tm.Err != sarama.ErrNoError {
					cmd.infof("Failed to get metadata from %#v. err=%v\n", addr, tm.Err)
					continue loop
				}

				cmd.leaders = map[int32]*sarama.Broker{}
				for _, pm := range tm.Partitions {
					b, ok := brokers[pm.Leader]
					if !ok {
						failf("failed to find leader in broker response, giving up")
					}

					if err = b.Open(cfg); err != nil && err != sarama.ErrAlreadyConnected {
						failf("failed to open broker connection err=%s", err)
					}
					if connected, err := broker.Connected(); !connected && err != nil {
						failf("failed to wait for broker connection to open err=%s", err)
					}

					cmd.leaders[pm.ID] = b
				}
				return
			}
		}
	}

	failf("failed to find leader for given topic")
}

type produceCmd struct {
	baseCmd

	Topic       string        `help:"Topic to produce to." env:"KT_TOPIC"`
	Partition   int32         `help:"Partition to produce to" default:"0"`
	Batch       int           `help:"Batch size" default:"1"`
	Timeout     time.Duration `help:"Timeout for request to Kafka" default:"5s"`
	Quiet       bool          `help:"Don't output messages during processing"`
	Literal     bool          `help:"Interpret stdin line literally and pass it as value, key as null"`
	Compression string        `help:"Compression codec to use (gzip, snappy, lz4)" default:""`
	Partitioner string        `help:"Partitioner to use (hashCode, hashCodeByValue)" default:""`
	DecodeKey   string        `help:"Decode message key (string, hex, base64)" default:"string"`
	DecodeValue string        `help:"Decode message value (string, hex, base64)" default:"string"`
	BufferSize  int           `help:"Buffer size for producer" default:"8192"`

	compression sarama.CompressionCodec
	leaders     map[int32]*sarama.Broker
}

func (cmd *produceCmd) run() {
	cmd.prepare()
	if cmd.Verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	defer cmd.close()
	cmd.findLeaders()
	stdin := make(chan string)
	lines := make(chan string)
	messages := make(chan message)
	batchedMessages := make(chan []message)
	out := make(chan printContext)
	if cmd.Quiet {
		go quietPrint(out)
	} else {
		go print(out, cmd.Pretty)
	}
	q := make(chan struct{})

	go cmd.readStdinLines(cmd.BufferSize, stdin)
	go cmd.listenForInterrupt(q)
	go cmd.readInput(q, stdin, lines)
	go cmd.deserializeLines(lines, messages, int32(len(cmd.leaders)))
	go cmd.batchRecords(messages, batchedMessages)
	cmd.produce(batchedMessages, out)
}

func (cmd *produceCmd) readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, max), max)

	for scanner.Scan() {
		out <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		warnf("scanning input failed err=%v\n", err)
	}
	close(out)
}

func (cmd *produceCmd) listenForInterrupt(q chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt)
	sig := <-signals
	warnf("received signal %s - triggering shutdown\n", sig)
	close(q)
}

func (cmd *produceCmd) close() {
	for _, b := range cmd.leaders {
		var (
			connected bool
			err       error
		)

		if connected, err = b.Connected(); err != nil {
			cmd.infof("Failed to check if broker is connected. err=%s\n", err)
			continue
		}

		if !connected {
			continue
		}

		if err = b.Close(); err != nil {
			cmd.infof("Failed to close broker %v connection. err=%s\n", b, err)
		}
	}
}

func (cmd *produceCmd) deserializeLines(in chan string, out chan message, partitionCount int32) {
	defer func() { close(out) }()
	for {
		select {
		case l, ok := <-in:
			if !ok {
				return
			}
			var msg message

			switch {
			case cmd.Literal:
				msg.Value = &l
				msg.Partition = &cmd.Partition
			default:
				if err := json.Unmarshal([]byte(l), &msg); err != nil {
					cmd.infof("Failed to unmarshal input [%v], falling back to defaults. err=%v\n", l, err)
					var v *string = &l
					if len(l) == 0 {
						v = nil
					}
					msg = message{Key: nil, Value: v}
				}
			}

			var part int32 = 0
			if msg.Partition == nil {
				if msg.Value != nil && cmd.Partitioner == "hashCodeByValue" {
					part = hashCodePartition(*msg.Value, partitionCount)
				} else if msg.Key != nil && cmd.Partitioner == "hashCode" {
					part = hashCodePartition(*msg.Key, partitionCount)
				}
				msg.Partition = &part
			}

			out <- msg
		}
	}
}

func (cmd *produceCmd) batchRecords(in chan message, out chan []message) {
	defer func() { close(out) }()

	messages := []message{}
	send := func() {
		out <- messages
		messages = []message{}
	}

	for {
		select {
		case m, ok := <-in:
			if !ok {
				send()
				return
			}

			messages = append(messages, m)
			if len(messages) > 0 && len(messages) >= cmd.Batch {
				send()
			}
		case <-time.After(cmd.Timeout):
			if len(messages) > 0 {
				send()
			}
		}
	}
}

type partitionProduceResult struct {
	start int64
	count int64
}

func (cmd *produceCmd) makeSaramaMessage(msg message) (*sarama.Message, error) {
	var (
		err error
		sm  = &sarama.Message{Codec: cmd.compression}
	)

	if msg.Key != nil {
		switch cmd.DecodeKey {
		case "hex":
			if sm.Key, err = hex.DecodeString(*msg.Key); err != nil {
				return sm, fmt.Errorf("failed to decode key as hex string, err=%v", err)
			}
		case "base64":
			if sm.Key, err = base64.StdEncoding.DecodeString(*msg.Key); err != nil {
				return sm, fmt.Errorf("failed to decode key as base64 string, err=%v", err)
			}
		default: // string
			sm.Key = []byte(*msg.Key)
		}
	}

	if msg.Value != nil {
		switch cmd.DecodeValue {
		case "hex":
			if sm.Value, err = hex.DecodeString(*msg.Value); err != nil {
				return sm, fmt.Errorf("failed to decode value as hex string, err=%v", err)
			}
		case "base64":
			if sm.Value, err = base64.StdEncoding.DecodeString(*msg.Value); err != nil {
				return sm, fmt.Errorf("failed to decode value as base64 string, err=%v", err)
			}
		default: // string
			sm.Value = []byte(*msg.Value)
		}
	}

	if cmd.version.IsAtLeast(sarama.V0_10_0_0) {
		sm.Version = 1
		sm.Timestamp = time.Now()
	}

	return sm, nil
}

func (cmd *produceCmd) produceBatch(leaders map[int32]*sarama.Broker, batch []message, out chan printContext) error {
	requests := map[*sarama.Broker]*sarama.ProduceRequest{}
	for _, msg := range batch {
		broker, ok := leaders[*msg.Partition]
		if !ok {
			return fmt.Errorf("non-configured partition %v", *msg.Partition)
		}
		req, ok := requests[broker]
		if !ok {
			req = &sarama.ProduceRequest{RequiredAcks: sarama.WaitForAll, Timeout: 10000}
			requests[broker] = req
		}

		sm, err := cmd.makeSaramaMessage(msg)
		if err != nil {
			return err
		}
		req.AddMessage(cmd.Topic, *msg.Partition, sm)
	}

	for broker, req := range requests {
		resp, err := broker.Produce(req)
		if err != nil {
			return fmt.Errorf("failed to send request to broker %#v. err=%s", broker, err)
		}

		offsets, err := cmd.readPartitionOffsetResults(resp)
		if err != nil {
			return fmt.Errorf("failed to read producer response err=%s", err)
		}

		for p, o := range offsets {
			result := mapObject(map[string]any{"partition": p, "startOffset": o.start, "count": o.count})
			ctx := printContext{output: result, done: make(chan struct{}), cmd: cmd.baseCmd}
			out <- ctx
			<-ctx.done
		}
	}

	return nil
}

func (cmd *produceCmd) readPartitionOffsetResults(resp *sarama.ProduceResponse) (map[int32]partitionProduceResult, error) {
	offsets := map[int32]partitionProduceResult{}
	for _, blocks := range resp.Blocks {
		for partition, block := range blocks {
			if block.Err != sarama.ErrNoError {
				warnf("Failed to send message. err=%s\n", block.Err.Error())
				return offsets, block.Err
			}

			if r, ok := offsets[partition]; ok {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: r.count + 1}
			} else {
				offsets[partition] = partitionProduceResult{start: block.Offset, count: 1}
			}
		}
	}
	return offsets, nil
}

func (cmd *produceCmd) produce(in chan []message, out chan printContext) {
	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			if err := cmd.produceBatch(cmd.leaders, b, out); err != nil {
				warnf(err.Error()) // TODO: failf
				return
			}
		}
	}
}

func (cmd *produceCmd) readInput(q chan struct{}, stdin chan string, out chan string) {
	defer func() { close(out) }()
	for {
		select {
		case l, ok := <-stdin:
			if !ok {
				return
			}
			out <- l
		case <-q:
			return
		}
	}
}
