package consumer

import (
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/LydinaPavuh/lazyAmqp/test_data/mock"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConsumerSimpleReceive(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test"}

	pooler := mock.MockedConsumerChannelFactory{ConsumerChan: make(chan amqp.Delivery)}
	callback := func(delivery *amqp.Delivery) { delivery.Ack(false) }

	consumer := NewConsumer(consumerCfg, callback, &pooler)

	assert.NoError(t, consumer.RunAsync())
	assert.True(t, consumer.IsRunning(), "Consumer is not running")

	asknowleger := &mock.MockedAsknowledger{}
	pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}

	assert.True(t, asknowleger.Ascked, "Message not been received on consumer")

	consumer.Cancel(false)

	assert.False(t, consumer.IsRunning(), "Fail to cancel consumer")
}

func TestConsumerReconnect(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test", RetryDelay: 1 * time.Second}
	pooler := mock.MockedConsumerChannelFactory{ConsumerChan: make(chan amqp.Delivery)}
	callback := func(delivery *amqp.Delivery) { delivery.Ack(false) }

	consumer := NewConsumer(consumerCfg, callback, &pooler)

	assert.NoError(t, consumer.RunAsync())
	assert.True(t, consumer.IsRunning(), "Consumer is not running")

	asknowleger := &mock.MockedAsknowledger{}
	pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}

	assert.True(t, asknowleger.Ascked, "Message not been received on consumer")

	// delivery channel close, simulate broken connection
	close(pooler.ConsumerChan)
	// New connection
	pooler.ConsumerChan = make(chan amqp.Delivery)
	// Try to send msg to new connection
	asknowleger = &mock.MockedAsknowledger{}
	pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}

	assert.True(t, asknowleger.Ascked, "Message not been received on consumer")

	consumer.Cancel(false)
	assert.False(t, consumer.IsRunning(), "Fail to cancel consumer")
}

func TestConsumerManyReceive(t *testing.T) {
	consumerCfg := common.ConsumerConf{Tag: "test", Queue: "test"}
	pooler := mock.MockedConsumerChannelFactory{ConsumerChan: make(chan amqp.Delivery, 100)}
	callback := func(delivery *amqp.Delivery) { delivery.Ack(false) }

	consumer := NewConsumer(consumerCfg, callback, &pooler)

	assert.NoError(t, consumer.RunAsync())
	assert.True(t, consumer.IsRunning(), "Consumer is not running")

	var asknowlegers []*mock.MockedAsknowledger
	for i := 0; i < 1000; i++ {
		asknowleger := &mock.MockedAsknowledger{}
		asknowlegers = append(asknowlegers)
		pooler.ConsumerChan <- amqp.Delivery{Acknowledger: asknowleger}
	}

	time.Sleep(1 * time.Second)
	for _, asknowleger := range asknowlegers {
		assert.True(t, asknowleger.Ascked, "Message not been received on consumer")
	}

	consumer.Cancel(false)
	assert.False(t, consumer.IsRunning(), "Fail to cancel consumer")
}
