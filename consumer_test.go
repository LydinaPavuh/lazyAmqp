package lazyAmqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"lazyAmqp/common"
	"lazyAmqp/test_data/mock"
	"testing"
)

func TestConsumer(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test"}
	pooler := mock.MockedConsumerChannelFactory{ConsumerChan: make(chan amqp.Delivery)}
	callback := func(delivery *amqp.Delivery) { delivery.Ack(false) }

	consumer := newConsumer(consumerCfg, callback, &pooler)

	if err := consumer.RunAsync(); err != nil {
		t.Fatalf("Fail to run consumer")
	}
	if !consumer.IsRunning() {
		t.Fatalf("Consumer is not running")
	}

	asknowleger := &mock.MockedAsknowledger{}
	pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}

	if !asknowleger.Ascked {
		t.Fatalf("Message not been received on consumer")
	}

	consumer.Cancel(false)

	if consumer.IsRunning() {
		t.Fatalf("Fail to cancel consumer")
	}
}
