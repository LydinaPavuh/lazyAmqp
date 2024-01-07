package lazyAmqp

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log/slog"
	"sync"
	"time"
)

type DeliveryCallback func(delivery *amqp091.Delivery)

type ConsumerConf struct {
	Tag           string
	Queue         string
	PrefetchCount int
	PrefetchSize  int
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          amqp091.Table
	RetryDelay    time.Duration
}

type Consumer struct {
	client    *RmqClient
	channel   *RmqChannel
	callback  DeliveryCallback
	config    ConsumerConf
	mustClose bool
	running   bool
	mu        sync.Mutex
	wg        sync.WaitGroup
}

func newConsumer(conf ConsumerConf, callback DeliveryCallback, client *RmqClient) *Consumer {
	consumer := Consumer{config: conf, callback: callback, mu: sync.Mutex{}, client: client}
	return &consumer
}

func (consumer *Consumer) Cancel(nowait bool) error {
	if err := consumer.cancel(nowait); err != nil {
		return err
	}
	if !nowait {
		consumer.wg.Wait()
	}
	return nil
}

func (consumer *Consumer) Run() error {
	err := consumer.setRunning()
	if err != nil {
		return err
	}
	defer consumer.setInactive()
	consumer.run()
	return nil
}

func (consumer *Consumer) RunAsync() error {
	err := consumer.setRunning()
	if err != nil {
		return err
	}
	go func() {
		defer consumer.setInactive()
		consumer.run()
	}()
	return nil
}

func (consumer *Consumer) run() {
	for !consumer.mustClose {
		err := consumer.consume()
		if err != nil {
			slog.Error(
				fmt.Sprintf("Error on consume: %s, retry after %d", err, consumer.config.RetryDelay),
				slog.Any("error", err),
			)
			time.Sleep(consumer.config.RetryDelay)
		}
	}
}

func (consumer *Consumer) consume() (err error) {
	consumer.channel, err = consumer.client.chanPool.Get()
	if err != nil {
		return err
	}
	defer consumer.client.chanPool.Put(consumer.channel)
	err = consumer.channel.SetQos(consumer.config.PrefetchCount, consumer.config.PrefetchSize)
	if err != nil {
		return err
	}
	// Reset qos
	defer consumer.channel.SetQos(0, 0)

	deliveryCh, err := consumer.channel.Consume(consumer.config.Queue,
		consumer.config.Tag,
		consumer.config.AutoAck,
		consumer.config.Exclusive,
		consumer.config.NoLocal,
		consumer.config.NoWait,
		consumer.config.Args,
	)
	// Reset consumer
	defer consumer.channel.Cancel(consumer.config.Tag, true)
	if err != nil {
		return err
	}
	for !consumer.mustClose {
		msg, ok := <-deliveryCh
		if !ok {
			return err
		}
		slog.Debug("Receive message", slog.String("messageId", msg.MessageId))
		consumer.callback(&msg)
	}
	return nil
}

func (consumer *Consumer) setRunning() error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.running {
		return ConsumerAlreadyRunning
	}
	consumer.mustClose = false
	consumer.running = true
	consumer.wg.Add(1)
	return nil
}

func (consumer *Consumer) setInactive() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.running = false
	consumer.channel = nil
	consumer.wg.Done()
}

func (consumer *Consumer) cancel(nowait bool) error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.mustClose {
		return nil
	}
	consumer.mustClose = true
	if consumer.running {
		if consumer.channel != nil && consumer.channel.IsOpen() {
			err := consumer.channel.Cancel(consumer.config.Tag, nowait)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
