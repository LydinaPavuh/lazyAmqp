package lazyAmqp

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"lazyAmqp/common"
	"lazyAmqp/internal"
	"sync"
)

type RmqClient struct {
	conf      *common.RmqConfig
	conn      *internal.RmqConnection
	chanPool  *internal.ChannelPool
	consumers map[string]*Consumer
	mu        sync.Mutex
}

func NewClient(conf *common.RmqConfig) (*RmqClient, error) {
	conn := internal.NewConn(conf)
	client := &RmqClient{
		conf:      conf,
		conn:      conn,
		chanPool:  internal.NewPool(internal.NewRmqChannelFactory(conn), 65535),
		consumers: make(map[string]*Consumer),
		mu:        sync.Mutex{},
	}
	return client, client.Connect()
}

func (client *RmqClient) isOpen() bool {
	return !client.conn.IsOpen()
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
		consumer.Cancel(false)
	}
	return nil
}

func (client *RmqClient) PublishText(ctx context.Context, exchange, key string, mandatory, immediate bool, text string) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	msg := amqp.Publishing{
		Body:        []byte(text),
		ContentType: common.MimeTextUtf8,
	}
	return ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (client *RmqClient) PublishBinary(ctx context.Context, exchange, key string, mandatory, immediate bool, data []byte) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	msg := amqp.Publishing{
		Body:        data,
		ContentType: common.MimeOctetStream,
	}
	return ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (client *RmqClient) PublishJson(ctx context.Context, exchange, key string, mandatory, immediate bool, obj any) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)

	jsonData, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		Body:        jsonData,
		ContentType: common.MimeApplicationJson,
	}
	return ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (client *RmqClient) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, passive, args)
}

func (client *RmqClient) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueBind(name, key, exchange, noWait, args)
}

func (client *RmqClient) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (client *RmqClient) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueUnbind(name, key, exchange, args)
}

func (client *RmqClient) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, passive, args)
}

func (client *RmqClient) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.ExchangeDelete(name, ifUnused, noWait)
}

func (client *RmqClient) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	ch, err := client.chanPool.Get()
	if err != nil {
		return amqp.Delivery{}, false, err
	}
	defer client.chanPool.Remove(ch)
	return ch.Get(queue, autoAck)
}

func (client *RmqClient) CreateConsumer(conf common.ConsumerConf, callback DeliveryCallback) *Consumer {
	client.mu.Lock()
	defer client.mu.Unlock()
	if conf.Tag == "" {
		conf.Tag = uuid.NewString()
	}
	consumerObj := newConsumer(conf, callback, client.chanPool)
	client.consumers[conf.Tag] = consumerObj
	return consumerObj
}

func (client *RmqClient) RemoveConsumer(consumer *Consumer) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	consumer.Cancel(false)
	delete(client.consumers, consumer.GetTag())
	return nil
}
