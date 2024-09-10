package pool

import (
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/LydinaPavuh/lazyAmqp/connection"
	"github.com/google/uuid"
	"log/slog"
	"sync"
)

type channelMap map[uuid.UUID]connection.IChannel

type ChannelPool struct {
	maxSize       uint16
	readyChannels []connection.IChannel
	allChannels   channelMap
	factory       connection.IChannelFactory
	r_mu          *sync.Mutex
	a_mu          *sync.RWMutex
}

func NewPool(factory connection.IChannelFactory, capacity uint16) *ChannelPool {
	return &ChannelPool{
		factory:     factory,
		maxSize:     capacity,
		allChannels: make(channelMap),
		r_mu:        &sync.Mutex{},
		a_mu:        &sync.RWMutex{},
	}
}

func (pool *ChannelPool) Get() (connection.IChannel, error) {
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
		slog.Debug("Chanel is closed try reopen")
		if err := pool.factory.Renew(channel); err != nil {
			return nil, err
		}
	}

	return channel, err
}

func (pool *ChannelPool) Remove(ch connection.IChannel) {
	pool.a_mu.RLock()
	_, ok := pool.allChannels[ch.GetId()]
	pool.a_mu.RUnlock()
	if !ok {
		panic("Unknown channel put")
	}

	pool.r_mu.Lock()
	pool.readyChannels = append(pool.readyChannels, ch)
	pool.r_mu.Unlock()
}

func (pool *ChannelPool) openNewChannel() (connection.IChannel, error) {
	pool.a_mu.Lock()
	defer pool.a_mu.Unlock()
	if uint16(len(pool.allChannels)) >= pool.maxSize {
		return nil, common.PoolLimitReached
	}

	ch, err := pool.factory.New()
	if err != nil {
		return ch, err
	}
	pool.allChannels[ch.GetId()] = ch
	return ch, nil
}

func (pool *ChannelPool) popFirstChan() connection.IChannel {
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
		if err := ch.Close(); err != nil {
			return err
		}
	}
	clear(pool.readyChannels)
	return nil
}

func (pool *ChannelPool) Size() int {
	return len(pool.allChannels)
}

func (pool *ChannelPool) ReadyCount() int {
	return len(pool.readyChannels)
}
