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

func (cmd *produceCmd) prepare() error {
	if err := cmd.baseCmd.prepare(); err != nil {
		return fmt.Errorf("failed to prepare jq query err=%v", err)
	}

	var err error
	cmd.compression, err = kafkaCompression(cmd.Compression)
	if err != nil {
		return err
	}
	return nil
}

func kafkaCompression(codecName string) (sarama.CompressionCodec, error) {
	switch codecName {
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "":
		return sarama.CompressionNone, nil
	}

	return sarama.CompressionNone, fmt.Errorf("unsupported compression codec %#v - supported: gzip, snappy, lz4", codecName)
}

func (cmd *produceCmd) createProducer() error {
	var (
		usr *user.User
		err error
		cfg = sarama.NewConfig()
	)

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = cmd.compression
	cfg.Version = cmd.getKafkaVersion()

	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-produce-" + sanitizeUsername(usr.Username)
	cmd.infof("Using Kafka version: %v, ClientID: %s\n", cfg.Version, cfg.ClientID)

	// Set up partitioner
	switch cmd.Partitioner {
	case "hashCode":
		cfg.Producer.Partitioner = sarama.NewHashPartitioner
	case "hashCodeByValue":
		cfg.Producer.Partitioner = sarama.NewHashPartitioner // Value-based partitioning handled in message preparation
	default:
		cfg.Producer.Partitioner = sarama.NewManualPartitioner
	}

	if err = setupAuth(cmd.baseCmd.auth, cfg); err != nil {
		return fmt.Errorf("failed to setup auth err=%v", err)
	}

	cmd.producer, err = sarama.NewSyncProducer(cmd.addDefaultPorts(cmd.Brokers), cfg)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}

	return nil
}

type produceCmd struct {
	baseCmd

	Topic       string        `help:"Topic to produce to." env:"KT_TOPIC" required:""`
	Partition   int32         `help:"Partition to produce to" default:"0"`
	Batch       int           `help:"Batch size" default:"1"`
	Timeout     time.Duration `help:"Timeout for request to Kafka" default:"5s"`
	Quiet       bool          `help:"Don't output messages during processing"`
	Literal     bool          `help:"Interpret stdin line literally and pass it as value, key as null"`
	Compression string        `help:"Compression codec to use (gzip, snappy, lz4)" default:""`
	Partitioner string        `help:"Partitioner to use (hashCode, hashCodeByValue)" default:""`
	DecodeKey   string        `help:"Decode message key (string, hex, base64)" default:"string" enum:"string,hex,base64"`
	DecodeValue string        `help:"Decode message value (string, hex, base64)" default:"string" enum:"string,hex,base64"`
	BufferSize  int           `help:"Buffer size for producer" default:"8192"`

	compression sarama.CompressionCodec
	producer    sarama.SyncProducer
}

func (cmd *produceCmd) run() error {
	if err := cmd.prepare(); err != nil {
		return err
	}
	if cmd.Verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	defer cmd.close()
	if err := cmd.createProducer(); err != nil {
		return err
	}
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
	go cmd.deserializeLines(lines, messages, 1) // Partition count not needed with SyncProducer
	go cmd.batchRecords(messages, batchedMessages)
	cmd.produce(batchedMessages, out)
	return nil
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
	if cmd.producer != nil {
		if err := cmd.producer.Close(); err != nil {
			cmd.infof("Failed to close producer. err=%s\n", err)
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

func (cmd *produceCmd) produceBatch(batch []message, out chan printContext) error {
	// Track offsets by partition
	partitionOffsets := map[int32]*partitionProduceResult{}

	for _, msg := range batch {
		sm, err := cmd.makeSaramaMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to make sarama message: %w", err)
		}

		// Create ProducerMessage
		producerMsg := &sarama.ProducerMessage{
			Topic: cmd.Topic,
			Key:   sarama.ByteEncoder(sm.Key),
			Value: sarama.ByteEncoder(sm.Value),
		}

		// Set partition if specified
		if msg.Partition != nil {
			producerMsg.Partition = *msg.Partition
		}

		// Send message
		partition, offset, err := cmd.producer.SendMessage(producerMsg)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}

		// Track offsets
		if r, ok := partitionOffsets[partition]; ok {
			r.count++
			if offset < r.start {
				r.start = offset
			}
		} else {
			partitionOffsets[partition] = &partitionProduceResult{start: offset, count: 1}
		}
	}

	// Output results
	for partition, result := range partitionOffsets {
		resultMap := mapObject(map[string]any{"partition": partition, "startOffset": result.start, "count": result.count})
		ctx := printContext{output: resultMap, done: make(chan struct{}), cmd: cmd.baseCmd}
		out <- ctx
		<-ctx.done
	}

	return nil
}

func (cmd *produceCmd) produce(in chan []message, out chan printContext) {
	for b := range in {
		if err := cmd.produceBatch(b, out); err != nil {
			warnf("failed to produceBatch: %v", err)
			return
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
