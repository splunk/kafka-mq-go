module cd.splunkdev.com/dferstay/kafka-mq-go

go 1.15

require (
	github.com/cenkalti/backoff v2.2.1+incompatible
	// NOTE: The pkg/service/queue/kafka/redelivery/markerconsumer.go module depends on implementation details
	//       of the confluent-kafka-go library.  Read the comments on the markerconsumer structure
	//       before you upgrade this library.
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/gogo/protobuf v1.3.2
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.4
	github.com/pkg/errors v0.9.1
	github.com/stretchr/objx v0.1.1 // indirect
	github.com/stretchr/testify v1.6.1
	go.uber.org/zap v1.16.0
	gopkg.in/yaml.v2 v2.3.0
)
