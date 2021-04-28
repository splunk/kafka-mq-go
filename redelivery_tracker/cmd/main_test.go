package cmd

import (
	"os"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery/mocks"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("Redelivery Tracker", func() {
	var (
		_setupSignalHandling func() <-chan os.Signal
		_newMarkerConsumer   func(config queue.Config, clients kafka.ClientFactory) (redelivery.MarkerConsumer, error)
		_getConfig           func() queue.Config
		markerConsumer       *mocks.MarkerConsumer
	)
	BeforeSuite(func() {
		_setupSignalHandling = setupSignalHandling
		setupSignalHandling = func() <-chan os.Signal {
			c := make(chan os.Signal, 1)
			c <- os.Interrupt
			return c
		}
	})
	AfterSuite(func() {
		setupSignalHandling = _setupSignalHandling
	})
	BeforeEach(func() {
		_newMarkerConsumer = newMarkerConsumer
		_getConfig = getConfig

		markerConsumer = &mocks.MarkerConsumer{}
		markerConsumer.On("Start").Once().Return()
		markerConsumer.On("Stop").Once().Return()

		newMarkerConsumer = func(_ queue.Config, _ kafka.ClientFactory) (redelivery.MarkerConsumer, error) {
			return markerConsumer, nil
		}
		getConfig = func() queue.Config {
			return queue.Config{}
		}
	})
	AfterEach(func() {
		newMarkerConsumer = _newMarkerConsumer
		getConfig = _getConfig
		markerConsumer.AssertExpectations(GinkgoT())
	})
	JustBeforeEach(func() {
		main()
	})
	It("should run", func() {
		// assertions are made on the marker consumer mock
	})
})
