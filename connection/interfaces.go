package connection

import (
	"context"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IChannelFactory interface {
	New() (IChannel, error)
	Renew(channel IChannel) error
}

type IChannel interface {
	GetId() uuid.UUID
	IsOpen() bool
	Close() error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Get(queue string, autoAck bool) (amqp.Delivery, bool, error)
	SetQos(prefetchCount int, prefetchSize int) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
}
