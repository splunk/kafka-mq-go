package queue

import (
	"flag"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const (
	maxRedeliveryTimeout = 12 * time.Hour
)

// Config contains the configuration for a Kafka-based queue implementation for task distribution
type Config struct {
	BootstrapServers              string
	MessageTopic                  string        // topic for delivering messages
	MarkerTopic                   string        // topic for markers used for message redelivery
	RedeliveryTimeout             time.Duration // time after which messages will be redelivered (maximum 12 hours)
	ConsumerReceiveBufferCapacity uint          // capacity of the receive buffer for queue consumers
	ConsumerNextMessageTimeout    time.Duration // timeout used in the consumer loop to read messages off of Kafka
	ConsumerMaxMessagesPerCommit  uint          // the maximum number of messages consumed before offsets are committed
	RedeliveryTracker             RedeliveryTrackerConfig
}

// RedeliveryTrackerConfig contain the configuration options for the Kafka-based queue redelivery tracker service
type RedeliveryTrackerConfig struct {
	UseNowIfNoMarkerSeen time.Duration // controls when to use the local time of the redelivery tracker to determine what messages require redelivery
	NumOffsetsPerCommit  uint          // the number of markers that can be consumed between offset commits
}

// config is an alias type for the `Config` structure that can be parsed from YAML format
type config struct {
	BootstrapServers              string `yaml:"bootstrap-servers"`
	MessageTopic                  string `yaml:"message-topic"`
	MarkerTopic                   string `yaml:"marker-topic"`
	RedeliveryTimeout             uint   `yaml:"redelivery-timeout" default:"60000"` // milliseconds
	ConsumerReceiveBufferCapacity uint   `yaml:"consumer-receive-buffer-capacity" default:"1"`
	ConsumerNextMessageTimeout    uint   `yaml:"consumer-next-message-timeout" default:"1000"` // milliseconds
	ConsumerMaxMessagesPerCommit  uint   `yaml:"consumer-max-messages-per-commit" default:"1000"`
	RedeliveryTracker             struct {
		UseNowIfNoMarkerSeen uint `yaml:"use-now-if-no-marker-seen" default:"60000"` // milliseconds
		NumOffsetsPerCommit  uint `yaml:"num-offsets-per-commit" default:"1000"`
	} `yaml:"redelivery-tracker"`
}

func (c Config) validate() error {
	if len(c.BootstrapServers) == 0 {
		return errors.New("invalid config: BootstrapServers must not be empty")
	}
	if len(c.MessageTopic) == 0 {
		return errors.New("invalid config: MessageTopic must not be empty")
	}
	if len(c.MarkerTopic) == 0 {
		return errors.New("invalid config: MarkerTopic must not be empty")
	}
	if c.RedeliveryTimeout <= 0 {
		return errors.New("invalid config: RedeliveryTimeout must be positive")
	}
	if c.RedeliveryTimeout > maxRedeliveryTimeout {
		return errors.New("invalid config: RedeliveryTimeout too large")
	}
	if c.ConsumerReceiveBufferCapacity <= 0 {
		return errors.New("invalid config: ConsumerReceiveBufferCapacity must be positive")
	}
	if c.ConsumerNextMessageTimeout <= 0 {
		return errors.New("invalid config: ConsumerNextMessageTimeout must be positive")
	}
	if c.ConsumerMaxMessagesPerCommit <= 0 {
		return errors.New("invalid config: ConsumerMaxMessagesPerCommit must be positive")
	}
	if c.RedeliveryTracker.UseNowIfNoMarkerSeen < 0 {
		return errors.New("invalid config: RedeliveryTracker.UseNowIfNoMarkerSeen must be non-negative")
	}
	if c.RedeliveryTracker.NumOffsetsPerCommit <= 0 {
		return errors.New("invalid config: RedeliveryTracker.NumOffsetsPerCommit must be positive")
	}
	return nil
}

type createConfigFunc = func() (Config, error)

func newFromBytes(data []byte) (Config, error) {
	var raw config
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, errors.Wrap(err, "Failed to unmarshal config data")
	}

	var config Config
	config.BootstrapServers = raw.BootstrapServers
	config.MessageTopic = raw.MessageTopic
	config.MarkerTopic = raw.MarkerTopic
	config.RedeliveryTimeout = time.Duration(raw.RedeliveryTimeout) * time.Millisecond
	config.ConsumerReceiveBufferCapacity = raw.ConsumerReceiveBufferCapacity
	config.ConsumerNextMessageTimeout = time.Duration(raw.ConsumerNextMessageTimeout) * time.Millisecond
	config.ConsumerMaxMessagesPerCommit = raw.ConsumerMaxMessagesPerCommit
	config.RedeliveryTracker.UseNowIfNoMarkerSeen = time.Duration(raw.RedeliveryTracker.UseNowIfNoMarkerSeen) * time.Millisecond
	config.RedeliveryTracker.NumOffsetsPerCommit = raw.RedeliveryTracker.NumOffsetsPerCommit

	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func newFromFile(filepath string) (Config, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return Config{}, errors.Wrap(err, "Failed to read config file")
	}
	return newFromBytes(data)
}

func newConfig(create createConfigFunc) (Config, error) {
	config, err := create()
	if err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// NewConfigFromCLI returns the config with a specified CLI command
func NewConfigFromCLI() (Config, error) {
	return newConfig(func() (Config, error) {
		configPath := flag.String("config", "", "Configuration file path")
		flag.Parse()
		if *configPath == "" {
			return Config{}, errors.New("Missing config file path")
		}
		return newFromFile(*configPath)
	})
}

// NewConfigFromFile returns the config specified from the filepath
func NewConfigFromFile(filepath string) (Config, error) {
	return newConfig(func() (Config, error) {
		return newFromFile(filepath)
	})
}
