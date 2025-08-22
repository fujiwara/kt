package main

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"time"

	json "github.com/goccy/go-json"

	"github.com/IBM/sarama"
)

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
		return fmt.Errorf("need to supply at least one sub-command of: createtopic, deletetopic")
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
