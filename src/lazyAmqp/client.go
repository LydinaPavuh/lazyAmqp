package lazyAmqp

import (
	"context"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type RmqClient struct {
	conf      *RmqClientConf
	conn      *RmqConnection
	chanPool  *ChannelPool
	consumers map[string]*Consumer
	mu        sync.Mutex
}

func NewClient(conf *RmqClientConf) (*RmqClient, error) {
	client := &RmqClient{conf: conf, conn: NewConn(conf), consumers: make(map[string]*Consumer), mu: sync.Mutex{}}
	client.chanPool = NewPool(client.conn, 65535)
	return client, client.Connect()
}

func (client *RmqClient) isOpen() bool {
	return !client.conn.isOpen()
}

func (client *RmqClient) Connect() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.conn.Open()
}

func (client *RmqClient) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.stopAllConsumers(); err != nil {
		return err
	}
	if err := client.chanPool.Discard(); err != nil {
		return err
	}
	return client.conn.Close()
}
func (client *RmqClient) stopAllConsumers() error {
	for k := range client.consumers {
		consumer := client.consumers[k]
		if err := consumer.Cancel(false); err != nil {
			return err
		}
	}
	return nil
}

func (client *RmqClient) PublishText(ctx context.Context, exchange, key string, mandatory, immediate bool, text string) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.PublishText(ctx, exchange, key, mandatory, immediate, text)
}

func (client *RmqClient) PublishJson(ctx context.Context, exchange, key string, mandatory, immediate bool, obj any) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.PublishJson(ctx, exchange, key, mandatory, immediate, obj)
}

func (client *RmqClient) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, passive, args)
}

func (client *RmqClient) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.QueueBind(name, key, exchange, noWait, args)
}

func (client *RmqClient) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (client *RmqClient) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.QueueUnbind(name, key, exchange, args)
}

func (client *RmqClient) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, passive, args)
}

func (client *RmqClient) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Put(ch)
	return ch.ExchangeDelete(name, ifUnused, noWait)
}

func (client *RmqClient) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	ch, err := client.chanPool.Get()
	if err != nil {
		return amqp.Delivery{}, false, err
	}
	defer client.chanPool.Put(ch)
	return ch.Get(queue, autoAck)
}

func (client *RmqClient) CreateConsumer(conf ConsumerConf, callback DeliveryCallback) *Consumer {
	client.mu.Lock()
	defer client.mu.Unlock()
	if conf.Tag == "" {
		conf.Tag = uuid.NewString()
	}
	consumer := newConsumer(conf, callback, client)
	client.consumers[conf.Tag] = consumer
	return consumer
}

func (client *RmqClient) RemoveConsumer(consumer *Consumer) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	err := consumer.Cancel(false)
	if err != nil {
		return err
	}
	delete(client.consumers, consumer.config.Tag)
	return nil
}
