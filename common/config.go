package common

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type RmqConfig struct {
	amqp.Config
	//ChannelPoolSize uint16
	Url string
}

type ConsumerConf struct {
	Tag           string
	Queue         string
	PrefetchCount int
	PrefetchSize  int
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          amqp.Table
	RetryDelay    time.Duration
}
