package mock

import (
	"context"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MockedChannel struct {
	id           uuid.UUID
	IsOpenFlag   bool
	GetOkFlag    bool
	err          error
	consumerChan chan amqp.Delivery
}

func (channel *MockedChannel) GetId() uuid.UUID {
	return channel.id
}
func (channel *MockedChannel) IsOpen() bool {
	return channel.IsOpenFlag
}

func (channel *MockedChannel) Close() error {
	return channel.err
}

func (channel *MockedChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error {
	return channel.err
}

func (channel *MockedChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return channel.err
}

func (channel *MockedChannel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error {
	return channel.err
}

func (channel *MockedChannel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return channel.err
}

func (channel *MockedChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error {
	return channel.err
}

func (channel *MockedChannel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return channel.err
}

func (channel *MockedChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return channel.err
}

func (channel *MockedChannel) PublishText(ctx context.Context, exchange, key string, mandatory, immediate bool, text string) error {
	return channel.err
}

func (channel *MockedChannel) PublishJson(ctx context.Context, exchange, key string, mandatory, immediate bool, obj any) error {
	return channel.err
}

func (channel *MockedChannel) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	return amqp.Delivery{}, channel.GetOkFlag, channel.err
}

func (channel *MockedChannel) SetQos(prefetchCount int, prefetchSize int) error {
	return channel.err
}

func (channel *MockedChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return channel.consumerChan, channel.err
}

func (channel *MockedChannel) Cancel(consumer string, noWait bool) error {
	return channel.err
}
