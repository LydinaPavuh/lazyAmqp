package mock

import (
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"lazyAmqp/internal"
)

type MockedConsumerChannelFactory struct {
	ConsumerChan chan amqp.Delivery
}

type MockedAsknowledger struct {
	Ascked   bool
	Rejected bool
	Requeed  bool
}

func (inst *MockedAsknowledger) Ack(tag uint64, multiple bool) error {
	inst.Ascked = true
	return nil
}
func (inst *MockedAsknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	inst.Ascked = true
	inst.Requeed = requeue
	return nil
}
func (inst *MockedAsknowledger) Reject(tag uint64, requeue bool) error {
	inst.Rejected = true
	inst.Requeed = requeue
	return nil
}

func (factory *MockedConsumerChannelFactory) Get() (internal.IChannel, error) {
	ch := &MockedChannel{
		id:           uuid.Must(uuid.NewUUID()),
		IsOpenFlag:   true,
		GetOkFlag:    true,
		err:          nil,
		consumerChan: factory.ConsumerChan,
	}
	return ch, nil
}

func (factory *MockedConsumerChannelFactory) Remove(channel internal.IChannel) {}
