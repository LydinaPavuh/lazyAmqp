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

//
//func Getlistener(client *lazyAmqp.RmqClient, controlCh chan string) {
//	t1 := time.Tick(time.Second * 2) // timer loop:
//	for {
//		select {
//		case <-t1:
//			log.Println("Time to get message:")
//			delivery, ok, err := client.Get("test", true)
//			if err != nil {
//				log.Println("Error to get message: ", err)
//			} else if ok == false {
//				log.Println("Queue is empty")
//			} else {
//				log.Println("Message: ", string(delivery.Body))
//			}
//		case chMsg := <-controlCh:
//			if chMsg == "quit" {
//				log.Println("Listener quit")
//				return
//			}
//			log.Println("Unknown control msg")
//		}
//
//	}
//}

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
	consumer := client.CreateConsumer(lazyAmqp.ConsumerConf{Tag: "test", Queue: "test", RetryDelay: time.Second * 60}, func(delivery *amqp.Delivery) {
		fmt.Printf("Consumed msg: %s\n", string(delivery.Body))
		Must(delivery.Ack(false))
	})

	consumer2 := client.CreateConsumer(lazyAmqp.ConsumerConf{Tag: "test", Queue: "test", RetryDelay: time.Second * 60}, func(delivery *amqp.Delivery) {
		fmt.Printf("Consumed 2 msg: %s\n", string(delivery.Body))
		Must(delivery.Ack(false))
	})

	Must(consumer.RunAsync())
	Must(consumer2.RunAsync())
	time.Sleep(time.Second * 10)
	for i := 0; i < 100; i++ {
		Must(client.PublishText(context.Background(), "test", "test", true, false, fmt.Sprintf("test_r_%d", i)))
	}
	fmt.Println("Publish 2")
	Must(client.PublishText(context.Background(), "test", "test", true, false, "test2"))
	time.Sleep(time.Second * 5)
	Must(consumer.Cancel(false))
	Must(consumer2.Cancel(false))
	Must(client.RemoveConsumer(consumer))
	time.Sleep(time.Second * 5)
}
