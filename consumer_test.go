package lazyAmqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"lazyAmqp/common"
	"lazyAmqp/test_data/mock"
	"testing"
	"time"
)

func TestConsumerSimpleReceive(t *testing.T) {
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

func TestConsumerReconnect(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test", RetryDelay: 1 * time.Second}
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

	// delivery channel close, simulate broken connection
	close(pooler.ConsumerChan)
	// New connection
	pooler.ConsumerChan = make(chan amqp.Delivery)
	// Try to send msg to new connection
	asknowleger = &mock.MockedAsknowledger{}
	pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}

	if !asknowleger.Ascked {
		t.Fatalf("Message not been received on consumer")
	}

	consumer.Cancel(false)

	if consumer.IsRunning() {
		t.Fatalf("Fail to cancel consumer")
	}
}

func TestConsumerManyReceive(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test"}
	pooler := mock.MockedConsumerChannelFactory{ConsumerChan: make(chan amqp.Delivery, 100)}
	callback := func(delivery *amqp.Delivery) { delivery.Ack(false) }

	consumer := newConsumer(consumerCfg, callback, &pooler)

	if err := consumer.RunAsync(); err != nil {
		t.Fatalf("Fail to run consumer")
	}
	if !consumer.IsRunning() {
		t.Fatalf("Consumer is not running")
	}

	var asknowlegers []*mock.MockedAsknowledger

	for i := 0; i > 0; i++ {
		asknowleger := &mock.MockedAsknowledger{}
		asknowlegers = append(asknowlegers)
		pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}
	}
	time.Sleep(1 * time.Second)
	for _, asknowleger := range asknowlegers {
		if !asknowleger.Ascked {
			t.Fatalf("Message not been received on consumer")
		}
	}
	consumer.Cancel(false)

	if consumer.IsRunning() {
		t.Fatalf("Fail to cancel consumer")
	}
}
