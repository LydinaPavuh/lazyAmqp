package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"lazyAmqp/src/lazyAmqp"
	"time"
)

func Must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	conf := lazyAmqp.RmqClientConf{
		Url: "amqp://rmuser:rmpassword@127.0.0.1:5672",
	}

	fmt.Println("Connect")
	client, err := lazyAmqp.NewClient(&conf)
	Must(err)
	fmt.Println("Exchange declare")
	Must(client.ExchangeDeclare("test", "fanout", true, false, false, false, false, nil))
	fmt.Println("Queue declare")
	Must(client.QueueDeclare("test", true, false, false, false, false, nil))
	fmt.Println("Bind")
	Must(client.QueueBind("test", "test", "test", false, nil))
	fmt.Println("Publish 1")
	Must(client.PublishText(context.Background(), "test", "test", true, false, "test1"))

	consumerConf := lazyAmqp.ConsumerConf{Queue: "test", RetryDelay: time.Second * 60}

	consumer := client.CreateConsumer(consumerConf, func(delivery *amqp.Delivery) {
		fmt.Printf("Consumed msg: %s\n", string(delivery.Body))
		Must(delivery.Ack(false))
	})

	consumer2 := client.CreateConsumer(consumerConf, func(delivery *amqp.Delivery) {
		fmt.Printf("Consumed 2 msg: %s\n", string(delivery.Body))
		Must(delivery.Ack(false))
	})

	Must(consumer.RunAsync())
	Must(consumer2.RunAsync())

	for i := 0; i < 1000; i++ {
		time.Sleep(time.Second)
		fmt.Println("Publish")
		Must(client.PublishText(context.Background(), "test", "test", true, false, fmt.Sprintf("test_r_%d", i)))
	}
	Must(consumer.Cancel(false))
	Must(consumer2.Cancel(false))
	Must(client.Close())
}
