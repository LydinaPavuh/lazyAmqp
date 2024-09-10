## Lazy AMPQ

The library provides a simple and reliable connection interface for rabbitMQ

### Features

* Simple interface
* Channel pooling
* Automatic reconnect
* Easy to use

## Example

```
package main
import (
    "github.com/LydinaPavuh/lazyAmqp/common"
    "github.com/LydinaPavuh/lazyAmqp/connection"
)

func consumeMsg(delivery *amqp.Delivery) {
    fmt.Printf("Consumed 2 msg: %s\n", string(delivery.Body))
    delivery.Ack(false)
}

func main() {
    conf := common.RmqConfig{Url: "amqp://rmuser:rmpassword@127.0.0.1:5672"}
    client := lazyAmqp.NewClient(&conf)
    client.Connect()
    
    // Declare Exchange
    client.ExchangeDeclare("exchange", "fanout", true, false, false, false, false, nil)
    
    // Queue and bind it to exchange
    client.QueueDeclare("queue", true, false, false, false, false, nil)
    client.QueueBind("queue", "", "exchange", false, nil)
    
    // Make consumer
    consumerConf := common.ConsumerConf{Queue: "queue", RetryDelay: time.Second}
    consumer2Cancel, _ := client.Consume(consumerConf, consumeMsg)
    
    // Publishing
    client.PublishText(context.Background(), "queue", "", true, false, "Hello")
    
    // Cancel consumer
    consumer2Cancel(false)
}
```