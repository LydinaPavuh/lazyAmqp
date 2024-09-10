package pool_test

import (
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/LydinaPavuh/lazyAmqp/connection"
	"github.com/LydinaPavuh/lazyAmqp/pool"
	"github.com/LydinaPavuh/lazyAmqp/test_data/mock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func checkPoolSize(pool *pool.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.Size()
	assert.Equal(t, expectedSize, curSize, "Pool has incorrect size")
}

func checkPoolReadySize(pool *pool.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.ReadyCount()
	assert.Equal(t, expectedSize, curSize, "Pool has incorrect ready object count")
}

func TestPool(t *testing.T) {
	var poolSize uint16 = 10
	factory := &mock.MockedChannelFactory{}
	chPool := pool.NewPool(factory, poolSize)

	// Pool is empty pool create new channel
	ch, err := chPool.Get()
	assert.NoError(t, err, "Fail to get channel from pool")

	checkPoolSize(chPool, 1, t)
	checkPoolReadySize(chPool, 0, t)

	// Return channel to pool
	chPool.Remove(ch)
	checkPoolSize(chPool, 1, t)
	checkPoolReadySize(chPool, 1, t)

	// Pool has one channel try get him
	ch, err = chPool.Get()
	assert.NoError(t, err, "Fail to get channel from pool")
	checkPoolSize(chPool, 1, t)
	checkPoolReadySize(chPool, 0, t)
	chPool.Remove(ch)

}

func TestPoolOverflow(t *testing.T) {
	var poolSize uint16 = 65535
	factory := &mock.MockedChannelFactory{}
	chPool := pool.NewPool(factory, poolSize)
	var channels []connection.IChannel

	// Get full pool
	for i := 0; i < int(poolSize); i++ {
		ch, err := chPool.Get()
		assert.NoError(t, err, "Fail to get value from pool")
		channels = append(channels, ch)
	}
	// Try to overflow pool
	_, err := chPool.Get()
	assert.ErrorIs(t, err, common.PoolLimitReached)

	for _, ch := range channels {
		chPool.Remove(ch)
	}
}
