package bench

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/queue"
	"github.com/splunk/kafka-mq-go/tests"
)

const (
	// The maximum number of asynchronous sends send before Flush is called on the producer
	// NB: this is necessary to keep the underlying librdkafka send queue from overflowing
	batchSize = 1000
)

var (
	conf        queue.Config
	provisioner *tests.Provisioner
	err         error
)

func BenchmarkProduce_256(b *testing.B) {
	runProduce(b, 256)
}

func BenchmarkProduce_1024(b *testing.B) {
	runProduce(b, 1024)
}

func BenchmarkProduce_8192(b *testing.B) {
	runProduce(b, 8192)
}

func BenchmarkProduceAsync_256(b *testing.B) {
	runProduceAsync(b, 256)
}

func BenchmarkProduceAsync_1024(b *testing.B) {
	runProduceAsync(b, 1024)
}

func BenchmarkProduceAsync_8192(b *testing.B) {
	runProduceAsync(b, 8192)
}

func runProduce(b *testing.B, payloadSize int) {
	setup(b)

	q := newQueue(b)
	p := newProducer(b, q)
	defer p.Close()

	data := testData(b, payloadSize)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err = p.Send(data)
		if err != nil {
			b.Fatalf("failed to send data, err=%+v", err)
		}
	}
}

func runProduceAsync(b *testing.B, payloadSize int) {
	setup(b)

	q := newQueue(b)
	p := newProducer(b, q)
	defer p.Close()

	ch := make(chan queue.Event, batchSize)
	numSent := 0
	ctx := context.Background()

	data := testData(b, payloadSize)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.SendAsync(data, ch)
		if numSent = (numSent + 1) % batchSize; numSent == 0 {
			p.Flush(ctx)
		}
	ConsumeDeliveryEvents:
		for {
			select {
			case e := <-ch:
				checkDeliveryEvent(b, e)
			default:
				break ConsumeDeliveryEvents
			}
		}
	}
	p.Flush(ctx)
	close(ch)
	for e := range ch {
		checkDeliveryEvent(b, e)
	}
}

func setup(b *testing.B) {
	configFile := "../config/config.yaml"
	conf, err = queue.NewConfigFromFile(configFile)
	if err != nil {
		b.Fatalf("failed to initialize config, err=%+v", err)
	}

	provisioner, err = tests.NewProvisioner(conf)
	if err != nil {
		b.Fatalf("failed to initialize provisioner, err=%+v", err)
	}

	ctx := context.Background()
	if err := provisioner.ProvisionTopics(ctx); err != nil {
		b.Fatalf("failed to provision topics, err=%+v", err)
	}
}

func newQueue(b *testing.B) queue.Queue {
	clientFactory := kafka.NewClientFactory()
	service, err := queue.NewService(clientFactory, conf)
	if err != nil {
		b.Fatalf("failed to create queue service, err=%+v", err)
	}

	return service.GetOrCreateQueue("benchmark-q")
}

func newProducer(b *testing.B, q queue.Queue) queue.Producer {
	p, err := q.Producer()
	if err != nil {
		b.Fatalf("failed to create producer, err=%+v", err)
	}
	return p
}

func testData(b *testing.B, len int) []byte {
	data := make([]byte, len)
	_, err := rand.Read(data)
	if err != nil {
		b.Fatalf("failed to read rand, err=%+v", err)
	}
	return data
}

func checkDeliveryEvent(b *testing.B, e queue.Event) {
	switch e.(type) {
	case *queue.MessageDeliveredEvent:
		return
	default:
		b.Fatalf("message delivery returned unhandled type: %s", e.String())
	}
}
