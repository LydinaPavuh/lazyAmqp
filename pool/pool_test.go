package pool_test

import (
	"errors"
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/LydinaPavuh/lazyAmqp/connection"
	"github.com/LydinaPavuh/lazyAmqp/pool"
	"github.com/LydinaPavuh/lazyAmqp/test_data/mock"
	"testing"
)

func checkPoolSize(pool *pool.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.Size()
	if curSize != expectedSize {
		t.Fatalf("Pool has incorrect size expected %d, current %d", expectedSize, curSize)
	}
}

func checkPoolReadySize(pool *pool.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.ReadyCount()
	if curSize != expectedSize {
		t.Fatalf("Pool has incorect ready object count expected %d, current %d", expectedSize, curSize)
	}
}

func TestPool(t *testing.T) {
	var poolSize uint16 = 10
	factory := &mock.MockedChannelFactory{}
	chPool := pool.NewPool(factory, poolSize)

	// Pool is empty pool create new channel
	ch, err := chPool.Get()
	if err != nil {
		t.Fatalf("Fail to get channel from pool")
	}

	checkPoolSize(chPool, 1, t)
	checkPoolReadySize(chPool, 0, t)

	// Return channel to pool
	chPool.Remove(ch)
	checkPoolSize(chPool, 1, t)
	checkPoolReadySize(chPool, 1, t)

	// Pool has one channel try get him
	ch, err = chPool.Get()
	if err != nil {
		t.Errorf("Fail to get channel from pool")
	}
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
		if err != nil {
			t.Fatalf("Fail to get value from pool")
		}
		channels = append(channels, ch)
	}
	// Try to overflow pool
	_, err := chPool.Get()
	if !errors.Is(err, common.PoolLimitReached) {
		t.Fatalf("Invalid error on overflow pool, %s", err)
	}
	for _, ch := range channels {
		chPool.Remove(ch)
	}
}
