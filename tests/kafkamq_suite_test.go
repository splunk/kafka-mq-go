package tests

import (
	"context"
	"testing"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"
	"cd.splunkdev.com/dferstay/kafka-mq-go/redelivery_tracker/cmd"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

var (
	conf           queue.Config
	provisioner    *Provisioner
	markerConsumer redelivery.MarkerConsumer
	err            error
	logger         *logging.Logger
)

func TestKafkaMessageQueue(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("../coverage-dir/kafka-mq-go-functional.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Kafka-based Message Queue Functional Suite", []Reporter{junitReporter})
}

var _ = BeforeSuite(func() {
	logger = logging.NewNoOp()

	// // Uncomment the following to enable DEBUG logging
	// logger = logging.New("functional_test")
	// logger.SetLevel(logging.DebugLevel)
	// logging.SetGlobalLogger(logger)

	configFile := "./config/config.yaml"

	conf, err = queue.NewConfigFromFile(configFile)
	Expect(err).ShouldNot(HaveOccurred())

	provisioner, err = NewProvisioner(conf)
	if err != nil {
		logger.Fatal(err, "failed to initialize provisioner with error")
	}

	ctx := context.Background()
	Expect(provisioner.ProvisionTopics(ctx)).Should(Succeed())

	startRedeliveryTracker()
})

var _ = AfterSuite(func() {
	stopRedliveryTracker()
	ctx := context.Background()
	Expect(provisioner.TeardownTopics(ctx)).Should(Succeed())
})

func startRedeliveryTracker() {
	go func() {
		markerConsumer = cmd.Start(logger, conf)
	}()
}

func stopRedliveryTracker() {
	markerConsumer.Stop()
}
