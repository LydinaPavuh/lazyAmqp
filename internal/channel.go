package internal

import (
	"context"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
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

type RmqChannel struct {
	origChan *amqp.Channel
	conn     *RmqConnection
	Id       uuid.UUID
	mu       *sync.Mutex
}

type RmqChannelFactory struct {
	connection *RmqConnection
}

func NewRmqChannelFactory(connection *RmqConnection) *RmqChannelFactory {
	return &RmqChannelFactory{connection: connection}
}

// RmqChannelFactory Methods

func (factory RmqChannelFactory) New() (IChannel, error) {
	return factory.connection.NewChannel()
}

func (factory RmqChannelFactory) Renew(channel IChannel) error {
	ch := channel.(*RmqChannel)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return factory.connection.renewChannel(ch)
}

// RmqChannel Methods

func (channel *RmqChannel) GetId() uuid.UUID {
	return channel.Id
}
func (channel *RmqChannel) IsOpen() bool {
	return channel.origChan != nil && !channel.origChan.IsClosed()
}

func (channel *RmqChannel) Close() error {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.origChan.Close()
}

func (channel *RmqChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error {
	var err error
	if passive {
		_, err = channel.origChan.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
	} else {
		_, err = channel.origChan.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	}
	return err
}

func (channel *RmqChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return channel.origChan.QueueBind(name, key, exchange, noWait, args)
}

func (channel *RmqChannel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error {
	_, err := channel.origChan.QueueDelete(name, ifUnused, ifEmpty, noWait)
	return err
}

func (channel *RmqChannel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return channel.origChan.QueueUnbind(name, key, exchange, args)
}

func (channel *RmqChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error {
	if passive {
		return channel.origChan.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
	} else {
		return channel.origChan.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	}
}

func (channel *RmqChannel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return channel.origChan.ExchangeDelete(name, ifUnused, noWait)
}

func (channel *RmqChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return channel.origChan.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (channel *RmqChannel) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	return channel.origChan.Get(queue, autoAck)
}

func (channel *RmqChannel) SetQos(prefetchCount int, prefetchSize int) error {
	return channel.origChan.Qos(prefetchCount, prefetchSize, false)
}

func (channel *RmqChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return channel.origChan.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (channel *RmqChannel) Cancel(consumer string, noWait bool) error {
	return channel.origChan.Cancel(consumer, noWait)
}
