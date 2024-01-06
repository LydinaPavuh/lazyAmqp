package lazyAmqp

import amqp "github.com/rabbitmq/amqp091-go"

type RmqClientConf struct {
	amqp.Config
	//ChannelPoolSize uint16
	Url string
}
