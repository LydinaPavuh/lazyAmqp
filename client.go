package lazyAmqp

import (
	"context"
	"encoding/json"
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/LydinaPavuh/lazyAmqp/connection"
	"github.com/LydinaPavuh/lazyAmqp/consumer"
	"github.com/LydinaPavuh/lazyAmqp/pool"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

// RmqClient
// Rabbitmq client opens and maintains the connection automatically restores it in case disconnection
type RmqClient struct {
	conf      *common.RmqConfig
	conn      *connection.RmqConnection
	chanPool  *pool.ChannelPool
	consumers map[string]*consumer.Consumer
	mu        sync.Mutex
	ready     bool
}

func NewClient(conf *common.RmqConfig) *RmqClient {
	conn := connection.NewConn(conf)
	client := &RmqClient{
		conf:      conf,
		conn:      conn,
		chanPool:  pool.NewPool(connection.NewRmqChannelFactory(conn), 65535),
		consumers: make(map[string]*consumer.Consumer),
		mu:        sync.Mutex{},
	}
	return client
}

// IsReady Return true if client ready to consume and publish messages
func (client *RmqClient) IsReady() bool {
	return client.ready
}

// ConnectionIsOpen Return true if connection is open in current time
func (client *RmqClient) ConnectionIsOpen() bool {
	return !client.conn.IsOpen()
}

// Connect Open new connection
func (client *RmqClient) Connect() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.conn.Open(); err != nil {
		return err
	}
	client.ready = true
	return nil
}

// Close cancel all consumers and close active connections
func (client *RmqClient) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.stopAllConsumers(); err != nil {
		return err
	}
	if err := client.chanPool.Discard(); err != nil {
		return err
	}
	if err := client.conn.Close(); err != nil {
		return err
	}
	client.ready = false
	return nil
}

func (client *RmqClient) stopAllConsumers() error {
	for k := range client.consumers {
		consumerObj := client.consumers[k]
		consumerObj.Cancel(false)
	}
	clear(client.consumers)
	return nil
}

// PublishText publish string message
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

// PublishBinary publish binary message
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

// PublishJson publish json object
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

// QueueDeclare declare new queue
func (client *RmqClient) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, passive, args)
}

// QueueBind bind queue to exchange
func (client *RmqClient) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueBind(name, key, exchange, noWait, args)
}

// QueueDelete delete queue
func (client *RmqClient) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// QueueUnbind Unbind queue from exchange
func (client *RmqClient) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.QueueUnbind(name, key, exchange, args)
}

// ExchangeDeclare declare new exchange
func (client *RmqClient) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait, passive bool, args amqp.Table) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, passive, args)
}

// ExchangeDelete delete exchange
func (client *RmqClient) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch, err := client.chanPool.Get()
	if err != nil {
		return err
	}
	defer client.chanPool.Remove(ch)
	return ch.ExchangeDelete(name, ifUnused, noWait)
}

// Get simple message from queue
func (client *RmqClient) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	ch, err := client.chanPool.Get()
	if err != nil {
		return amqp.Delivery{}, false, err
	}
	defer client.chanPool.Remove(ch)
	return ch.Get(queue, autoAck)
}

// CreateConsumer Create new consumer
func (client *RmqClient) CreateConsumer(conf common.ConsumerConf, callback consumer.DeliveryCallback) *consumer.Consumer {
	client.mu.Lock()
	defer client.mu.Unlock()
	if conf.Tag == "" {
		conf.Tag = uuid.NewString()
	}
	consumerObj := consumer.NewConsumer(conf, callback, client.chanPool)
	client.consumers[conf.Tag] = consumerObj
	return consumerObj
}

// RemoveConsumer Cancel and remove consumer
func (client *RmqClient) RemoveConsumer(consumer *consumer.Consumer) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	consumer.Cancel(false)
	delete(client.consumers, consumer.GetTag())
	return nil
}
