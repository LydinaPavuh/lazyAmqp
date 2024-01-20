package mock

import (
	"github.com/google/uuid"
	"lazyAmqp/internal"
)

type MockedChannelFactory struct {
}

func (factory *MockedChannelFactory) New() (internal.IChannel, error) {
	ch := &MockedChannel{
		id:         uuid.Must(uuid.NewUUID()),
		IsOpenFlag: true,
		GetOkFlag:  true,
		err:        nil,
	}
	return ch, nil
}

func (factory *MockedChannelFactory) Renew(channel internal.IChannel) error {
	return nil
}
