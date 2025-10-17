package main

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	json "github.com/goccy/go-json"

	"github.com/IBM/sarama"
)

type brokerInfo struct {
	ID      int32  `json:"id"`
	Host    string `json:"host"`
	Port    int32  `json:"port"`
	Rack    string `json:"rack,omitempty"`
	Version string `json:"version,omitempty"`
}

type clusterInfo struct {
	ClusterID    string       `json:"cluster_id"`
	ControllerID int32        `json:"controller_id"`
	Brokers      []brokerInfo `json:"brokers"`
}

func (c clusterInfo) ToMap() map[string]any {
	brokers := make([]any, len(c.Brokers))
	for i, b := range c.Brokers {
		brokers[i] = map[string]any{
			"id":   b.ID,
			"host": b.Host,
			"port": b.Port,
		}
		if b.Rack != "" {
			brokers[i].(map[string]any)["rack"] = b.Rack
		}
		if b.Version != "" {
			brokers[i].(map[string]any)["version"] = b.Version
		}
	}
	return map[string]any{
		"cluster_id":    c.ClusterID,
		"controller_id": c.ControllerID,
		"brokers":       brokers,
	}
}

type adminCmd struct {
	baseCmd

	Timeout time.Duration `help:"Timeout for request to Kafka" default:"3s"`

	CreateTopic  string `help:"Name of the topic that should be created."`
	TopicDetail  string `help:"Path to JSON encoded topic detail. cf sarama.TopicDetail"`
	ValidateOnly bool   `help:"Flag to indicate whether operation should only validate input (supported for createtopic)."`
	DeleteTopic  string `help:"Name of the topic that should be deleted."`

	brokers     []string
	timeout     *time.Duration
	topicDetail *sarama.TopicDetail
	admin       sarama.ClusterAdmin
}

func (cmd *adminCmd) prepare() error {
	if err := cmd.baseCmd.prepare(); err != nil {
		return fmt.Errorf("failed to prepare jq query err=%v", err)
	}

	if cmd.Timeout > 0 {
		cmd.timeout = &cmd.Timeout
	}

	cmd.brokers = cmd.addDefaultPorts(cmd.Brokers)

	if cmd.CreateTopic != "" {
		buf, err := os.ReadFile(cmd.TopicDetail)
		if err != nil {
			return fmt.Errorf("failed to read topic detail err=%v", err)
		}

		var detail sarama.TopicDetail
		if err = json.Unmarshal(buf, &detail); err != nil {
			return fmt.Errorf("failed to unmarshal topic detail err=%v", err)
		}
		cmd.topicDetail = &detail
	}
	return nil
}

func (cmd *adminCmd) run() error {
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
	if cmd.admin, err = sarama.NewClusterAdmin(cmd.brokers, cfg); err != nil {
		return fmt.Errorf("failed to create cluster admin err=%v", err)
	}

	if cmd.CreateTopic != "" {
		return cmd.runCreateTopic()
	} else if cmd.DeleteTopic != "" {
		return cmd.runDeleteTopic()
	} else {
		return cmd.runClusterInfo(cfg)
	}
}

func (cmd *adminCmd) runCreateTopic() error {
	err := cmd.admin.CreateTopic(cmd.CreateTopic, cmd.topicDetail, cmd.ValidateOnly)
	if err != nil {
		return fmt.Errorf("failed to create topic err=%v", err)
	}
	return nil
}

func (cmd *adminCmd) runDeleteTopic() error {
	err := cmd.admin.DeleteTopic(cmd.DeleteTopic)
	if err != nil {
		return fmt.Errorf("failed to delete topic err=%v", err)
	}
	return nil
}

func (cmd *adminCmd) saramaConfig() (*sarama.Config, error) {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.getKafkaVersion()
	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-admin-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if cmd.timeout != nil {
		cfg.Admin.Timeout = *cmd.timeout
	}

	if err = setupAuth(cmd.baseCmd.auth, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup auth err=%v", err)
	}

	return cfg, nil
}

func parseAddr(addr string) (string, int32) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return addr, 0
	}
	port, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return addr, 0
	}
	return parts[0], int32(port)
}

func getBrokerVersion(broker *sarama.Broker, cfg *sarama.Config) string {
	// Check if broker is already connected
	connected, err := broker.Connected()
	if err != nil || !connected {
		// Try to open connection
		if err := broker.Open(cfg); err != nil {
			return ""
		}
	}

	req := &sarama.ApiVersionsRequest{}
	resp, err := broker.ApiVersions(req)
	if err != nil {
		return ""
	}

	var metadataMaxVersion int16 = -1
	for _, apiKey := range resp.ApiKeys {
		if apiKey.ApiKey == 3 { // Metadata API key is 3
			metadataMaxVersion = apiKey.MaxVersion
			break
		}
	}

	// Map known Metadata API max versions to Kafka versions
	// This is approximate based on Kafka release notes
	// Reference: https://kafka.apache.org/protocol.html#protocol_api_keys
	switch {
	case metadataMaxVersion >= 13: // Kafka 4.0+
		return "4.0+"
	case metadataMaxVersion >= 12: // Kafka 3.0+
		return "3.0+"
	case metadataMaxVersion >= 9: // Kafka 2.4+
		return "2.4+"
	case metadataMaxVersion >= 7: // Kafka 2.1+
		return "2.1+"
	case metadataMaxVersion >= 5: // Kafka 1.0+
		return "1.0+"
	default:
		return fmt.Sprintf("unknown (metadata-api-v%d)", metadataMaxVersion)
	}
}

func (cmd *adminCmd) runClusterInfo(cfg *sarama.Config) error {
	out := make(chan printContext)
	go print(out, cmd.Pretty)

	brokerList, controllerID, err := cmd.admin.DescribeCluster()
	if err != nil {
		return fmt.Errorf("failed to describe cluster err=%v", err)
	}

	brokers := make([]brokerInfo, len(brokerList))
	for i, broker := range brokerList {
		host, port := parseAddr(broker.Addr())
		version := getBrokerVersion(broker, cfg)
		brokers[i] = brokerInfo{
			ID:      broker.ID(),
			Host:    host,
			Port:    port,
			Rack:    broker.Rack(),
			Version: version,
		}
	}

	info := clusterInfo{
		ClusterID:    "", // ClusterID not available from DescribeCluster
		ControllerID: controllerID,
		Brokers:      brokers,
	}

	ctx := printContext{output: info, done: make(chan struct{}), cmd: cmd.baseCmd}
	out <- ctx
	<-ctx.done

	return nil
}
