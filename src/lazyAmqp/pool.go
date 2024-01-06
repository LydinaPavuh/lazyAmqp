package lazyAmqp

import (
	"github.com/google/uuid"
	"sync"
)

type channelMap map[uuid.UUID]*RmqChannel

type ChannelPool struct {
	maxSize       uint16
	readyChannels channelMap
	allChannels   channelMap
	connection    *RmqConnection
	r_mu          *sync.Mutex
	a_mu          *sync.RWMutex
}

func NewPool(conn *RmqConnection, size uint16) *ChannelPool {
	return &ChannelPool{
		connection:    conn,
		maxSize:       size,
		allChannels:   make(channelMap),
		readyChannels: make(channelMap),
		r_mu:          &sync.Mutex{},
		a_mu:          &sync.RWMutex{},
	}
}

func (pool *ChannelPool) Get() (*RmqChannel, error) {
	var err error = nil
	channel := pool.popFirstChan()
	if channel == nil {
		channel, err = pool.openNewChannel()
		if err != nil {
			return nil, err
		}
	}
	if !channel.IsOpen() {
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
	pool.readyChannels[ch.Id] = ch
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
	for k := range pool.readyChannels {
		ch := pool.readyChannels[k]
		delete(pool.readyChannels, k)
		return ch
	}
	return nil
}
