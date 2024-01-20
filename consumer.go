package lazyAmqp

import (
	"fmt"
	"github.com/LydinaPavuh/signal"
	"github.com/rabbitmq/amqp091-go"
	"lazyAmqp/common"
	"lazyAmqp/internal"
	"log/slog"
	"sync"
	"time"
)

type IConsumerChannelFactory interface {
	Get() (internal.IChannel, error)
	Remove(channel internal.IChannel)
}

type DeliveryCallback func(delivery *amqp091.Delivery)

type Consumer struct {
	callback       DeliveryCallback
	config         common.ConsumerConf
	running        bool
	mu             sync.Mutex
	wg             sync.WaitGroup
	mustClose      *signal.Flag
	channelManager IConsumerChannelFactory
}

func newConsumer(conf common.ConsumerConf, callback DeliveryCallback, channelManager IConsumerChannelFactory) *Consumer {
	consumer := Consumer{
		config:         conf,
		callback:       callback,
		channelManager: channelManager,
		mustClose:      signal.NewFlag(),
		mu:             sync.Mutex{},
	}
	return &consumer
}
func (consumer *Consumer) IsRunning() bool {
	return consumer.running
}
func (consumer *Consumer) GetTag() string {
	return consumer.config.Tag
}

func (consumer *Consumer) Cancel(nowait bool) {
	consumer.cancel()
	if !nowait {
		consumer.wg.Wait()
	}
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
	for !consumer.mustClose.IsRaised() {
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
	channel, err := consumer.channelManager.Get()
	if err != nil {
		return err
	}
	defer consumer.channelManager.Remove(channel)

	err = channel.SetQos(consumer.config.PrefetchCount, consumer.config.PrefetchSize)
	if err != nil {
		return err
	}

	// Reset qos
	defer channel.SetQos(0, 0)

	deliveryCh, err := channel.Consume(
		consumer.config.Queue,
		consumer.config.Tag,
		consumer.config.AutoAck,
		consumer.config.Exclusive,
		consumer.config.NoLocal,
		consumer.config.NoWait,
		consumer.config.Args,
	)

	// Reset consumer
	defer channel.Cancel(consumer.config.Tag, true)
	if err != nil {
		return err
	}

	waiter := consumer.mustClose.Subscribe()
	defer waiter.Cancel()
	for !consumer.mustClose.IsRaised() {
		select {
		case <-waiter.Wait():
			slog.Debug("Consumer receive cancel signal")
		case msg, ok := <-deliveryCh:
			if !ok {
				return err
			}
			slog.Debug("Receive message", slog.String("messageId", msg.MessageId))
			consumer.callback(&msg)
		}
	}
	return nil
}

func (consumer *Consumer) setRunning() error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.running {
		return common.ConsumerAlreadyRunning
	}
	consumer.running = true
	consumer.wg.Add(1)
	consumer.mustClose.Reset()
	return nil
}

func (consumer *Consumer) setInactive() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.running = false
	consumer.wg.Done()
}

func (consumer *Consumer) cancel() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.mustClose.IsRaised() {
		return
	}
	consumer.mustClose.Raise()
	return
}
