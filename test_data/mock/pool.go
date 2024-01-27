package mock

import (
	"github.com/LydinaPavuh/lazyAmqp/connection"
	"github.com/google/uuid"
)

type MockedChannelFactory struct {
}

func (factory *MockedChannelFactory) New() (connection.IChannel, error) {
	ch := &MockedChannel{
		id:         uuid.Must(uuid.NewUUID()),
		IsOpenFlag: true,
		GetOkFlag:  true,
		err:        nil,
	}
	return ch, nil
}

func (factory *MockedChannelFactory) Renew(channel connection.IChannel) error {
	return nil
}
