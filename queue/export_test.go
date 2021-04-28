package queue

// Allow unit tests to construct kafkaMessage instances by exporting the `newMessage` factory function
var NewMessage = newMessage

// Allow unit tests to start the keep alive timer thread
type KafkaMessage = kafkaMessage

func (msg *KafkaMessage) StartKeepAliveTimer() {
	msg.startKeepAliveTimer()
}
