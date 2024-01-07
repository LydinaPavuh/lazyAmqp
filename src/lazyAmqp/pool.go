package lazyAmqp

import (
	"github.com/google/uuid"
	"log/slog"
	"sync"
)

type channelMap map[uuid.UUID]*RmqChannel

type ChannelPool struct {
	maxSize       uint16
	readyChannels []*RmqChannel
	allChannels   channelMap
	connection    *RmqConnection
	r_mu          *sync.Mutex
	a_mu          *sync.RWMutex
}

func NewPool(conn *RmqConnection, size uint16) *ChannelPool {
	return &ChannelPool{
		connection:  conn,
		maxSize:     size,
		allChannels: make(channelMap),
		r_mu:        &sync.Mutex{},
		a_mu:        &sync.RWMutex{},
	}
}

func (pool *ChannelPool) Get() (*RmqChannel, error) {
	var err error = nil
	channel := pool.popFirstChan()
	if channel == nil {
		slog.Debug("Pool is empty, create new channel")
		channel, err = pool.openNewChannel()
		if err != nil {
			return nil, err
		}
	}
	if !channel.IsOpen() {
		slog.Debug("Channel is closed try reopen")
		if err := channel.renew(); err != nil {
			return nil, err
		}
	}
	return channel, err
}

func (pool *ChannelPool) Put(ch *RmqChannel) {
	pool.a_mu.RLock()
	_, ok := pool.allChannels[ch.Id]
	pool.a_mu.RUnlock()
	if !ok {
		panic("Unknown channel put")
	}
	pool.r_mu.Lock()
	pool.readyChannels = append(pool.readyChannels, ch)
	pool.r_mu.Unlock()
}

func (pool *ChannelPool) openNewChannel() (*RmqChannel, error) {
	pool.a_mu.Lock()
	defer pool.a_mu.Unlock()
	if uint16(len(pool.allChannels)) >= pool.maxSize {
		return nil, PoolLimitReached
	}
	ch, err := pool.connection.newChannel()
	if err != nil {
		return ch, err
	}
	pool.allChannels[ch.Id] = ch
	return ch, nil
}

func (pool *ChannelPool) popFirstChan() *RmqChannel {
	pool.r_mu.Lock()
	defer pool.r_mu.Unlock()
	if len(pool.readyChannels) == 0 {
		return nil
	}
	ch := pool.readyChannels[0]
	pool.readyChannels = pool.readyChannels[1:]
	return ch
}

func (pool *ChannelPool) Discard() error {
	pool.r_mu.Lock()
	defer pool.r_mu.Unlock()
	for k := range pool.allChannels {
		ch := pool.allChannels[k]
		delete(pool.allChannels, k)
		if err := ch.close(); err != nil {
			return err
		}
	}
	clear(pool.readyChannels)
	return nil
}
