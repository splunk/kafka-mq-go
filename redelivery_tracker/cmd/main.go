package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka"
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"
)

var (
	revision  string
	goVersion string

	logger *logging.Logger

	setupSignalHandling = listenForInterrupt
	newMarkerConsumer   = redelivery.NewMarkerConsumer
	getConfig           = getRealConfig

	// Start is exposed so that it can be called in functional tests
	Start = start
)

func main() {
	defer handlePanic()

	logger = logging.NewGlobal("redelivery_tracker")
	logger.Info("redelivery_tracker starting", "revision", revision, "goVersion", goVersion)

	conf := getConfig()
	markerConsumer := Start(logger, conf)

	logger.Info("setting up signal handling")
	c := setupSignalHandling()

	logger.Info("waiting for interrupt..")
	<-c

	logger.Info("stopping marker consumer")
	markerConsumer.Stop()

	logger.Info("exiting")
}

func start(logger *logging.Logger, conf queue.Config) redelivery.MarkerConsumer {
	logger.Info("creating marker consumer")
	clients := kafka.NewClientFactory()
	markerConsumer, err := newMarkerConsumer(conf, clients)
	if err != nil {
		logger.Fatal(err, "failed to create marker consumer")
	}

	logger.Info("starting marker consumer")
	markerConsumer.Start()

	return markerConsumer
}

func listenForInterrupt() <-chan os.Signal {
	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		os.Interrupt,    // sent from terminal
		syscall.SIGTERM, // sent by kubernetes
	)
	return c
}

func handlePanic() {
	if r := recover(); r != nil {
		logger := logging.Global()
		logger.Fatal(fmt.Errorf("%+v", r), "panic")
	}
}

func getRealConfig() queue.Config {
	conf, err := queue.NewConfigFromCLI()
	if err != nil {
		logger.Fatal(err, "Failed to read configuration")
	}
	return conf
}
