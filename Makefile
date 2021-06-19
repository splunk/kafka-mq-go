REV := $(shell git rev-parse --short HEAD)
GOVER := $(shell go version | awk {' print $$4,$$3 '})
LDFLAGS = '-X main.revision=$(REV) -X "main.goVersion=$(GOVER)"'

PROTOC_OUTPUT := gogofaster_out

PKGS=$(shell go list ./... | grep -v vendor)
TEST_PKGS=$(shell go list ./... | grep -v tests | grep -v vendor)

.PHONY: proto
proto: queue/proto/kafka_mq_v1.pb.go

queue/proto/kafka_mq_v1.pb.go: queue/proto/kafka_mq_v1.proto
	@protoc --${PROTOC_OUTPUT}=. queue/proto/kafka_mq_v1.proto

mod:
	go mod tidy && go mod vendor

mock:
	mockery --dir=pkg/kafka --name=ClientFactory --output=pkg/kafka/mocks --case=underscore
	mockery --dir=pkg/kafka --name=Consumer --output=pkg/kafka/mocks --case=underscore
	mockery --dir=pkg/kafka --name=Producer --output=pkg/kafka/mocks --case=underscore
	mockery --dir=queue/internal --name=MqClient --output=queue/internal/mocks --case=underscore
	mockery --dir=queue/internal --name=MarkerProducer --output=queue/internal/mocks --case=underscore
	mockery --dir=queue/internal --name=MessageSender --output=queue/internal/mocks --case=underscore
	mockery --dir=queue/redelivery --name=AssignedPartition --output=queue/redelivery/mocks --case=underscore
	mockery --dir=queue/redelivery --name=MarkerConsumer --output=queue/redelivery/mocks --case=underscore
	mockery --dir=queue/redelivery --name=MarkersQueue --output=queue/redelivery/mocks --case=underscore
	mockery --dir=queue/redelivery --name=OffsetCommitter --output=queue/redelivery/mocks --case=underscore
	mockery --dir=queue/redelivery --name=Redeliverer --output=queue/redelivery/mocks --case=underscore
	mockery --dir=queue --name=Service --output=queue/mocks --case=underscore
	mockery --dir=queue --name=Queue --output=queue/mocks --case=underscore
	mockery --dir=queue --name=Producer --output=queue/mocks --case=underscore
	mockery --dir=queue --name=Consumer --output=queue/mocks --case=underscore
	mockery --dir=queue --name=Message --output=queue/mocks --case=underscore


lint: proto
	@set -e; golint $(PKGS)

vet: proto
	@set -e; go vet ${PKGS}

test: proto
	@set -e; go test ${TEST_PKGS}


.PHONY: build/linux
build/linux: mod
	@mkdir -p build/linux
	@GOOS=linux GOARCH=amd64 $(MAKE) build_binary

.PHONY: build_binary
build_binary:
	go build -ldflags ${LDFLAGS} -o ./build/$${GOOS}/$${OUTPUT} $${SOURCE}

# builds redelivery_tracker linux binary
build/linux/redelivery_tracker: proto
	@OUTPUT=redelivery_tracker SOURCE=redelivery_tracker/cmd/main.go $(MAKE) build/linux


functional-test: functional-test/services/start
	@set -e; ginkgo -r -v ./tests/functional

functional-test/services/start: test-network/create zk/start kafka/start

functional-test/services/stop: kafka/stop zk/stop test-network/destroy

bench-test: functional-test/services/start
	@cd tests/bench; go test -bench=. -benchmem

test-network/create:
	@if (2>/dev/null docker network inspect kbmq-test); then \
	    echo "test network already created"; \
	else \
	    docker network create kbmq-test; \
	fi

test-network/destroy:
	@docker network rm kbmq-test

zk/start:
	@if (2>/dev/null docker exec zk-kbmq echo -n); then \
		echo "ZooKeeper is already running"; \
	else \
		mkdir -p $$HOME/zk-kbmq/data $$HOME/zk-kbmq/datalog \
		&& docker run -d --rm --net=kbmq-test --name zk-kbmq \
		-p 2181:2181 \
		-p 2888:2888 \
		-p 3888:3888 \
		-v $$HOME/zk-kbmq/data,target=/data \
		-v $$HOME/zk-kbmq/datalog,target=/datalog \
		zookeeper:3.4.11 >/dev/null \
		&& echo '👉 ZooKeeper started, waiting it to be ready ..' \
		&& echo '⚠️ If this takes too long, use Ctrl+C to stop and check the zk-kmbq container logs ...' \
		&& while [ -z "$$OUTPUT" ]; do OUTPUT=`echo "ruok" | nc -w 30 localhost 2181 | grep imok`; sleep 1; done \
	fi

zk/stop:
	@docker stop zk-kbmq

kafka/start:
	@if (2>/dev/null docker exec kafka-kbmq echo -n); then \
		echo "Kafka is already running"; \
	else \
		mkdir -p $$HOME/kafka-kbmq/data \
		&& docker run -d --rm --net=kbmq-test --name kafka-kbmq \
		-p 9092:9092 \
		-p 9094:9094 \
		-e "KAFKA_ZOOKEEPER_CONNECT=zk-kbmq:2181" \
		-e "KAFKA_ADVERTISED_LISTENERS=OUTSIDE://localhost:9094,INSIDE://localhost:9092" \
		-e "KAFKA_LISTENERS=OUTSIDE://:9094,INSIDE://:9092" \
		-e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT" \
		-e "KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE" \
		-v $$HOME/kafka-kbmq/data,target=/kafka \
		wurstmeister/kafka:2.13-2.7.0 >/dev/null \
		&& echo '👉 Kafka started'; \
	fi

kafka/stop:
	@docker stop kafka-kbmq
