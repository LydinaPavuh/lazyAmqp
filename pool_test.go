package lazyAmqp_test

import (
	"errors"
	"lazyAmqp/common"
	"lazyAmqp/internal"
	"lazyAmqp/test_data/mock"
	"testing"
)

func checkPoolSize(pool *internal.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.Size()
	if curSize != expectedSize {
		t.Fatalf("Pool has incorrect size expected %d, current %d", expectedSize, curSize)
	}
}

func checkPoolReadySize(pool *internal.ChannelPool, expectedSize int, t *testing.T) {
	curSize := pool.ReadyCount()
	if curSize != expectedSize {
		t.Fatalf("Pool has incorect ready object count expected %d, current %d", expectedSize, curSize)
	}
}

func TestPool(t *testing.T) {
	var poolSize uint16 = 10
	factory := &mock.MockedChannelFactory{}
	pool := internal.NewPool(factory, poolSize)

	// Pool is empty pool create new channel
	ch, err := pool.Get()
	if err != nil {
		t.Fatalf("Fail to get channel from pool")
	}

	checkPoolSize(pool, 1, t)
	checkPoolReadySize(pool, 0, t)

	// Return channel to pool
	pool.Remove(ch)
	checkPoolSize(pool, 1, t)
	checkPoolReadySize(pool, 1, t)

	// Pool has one channel try get him
	ch, err = pool.Get()
	if err != nil {
		t.Errorf("Fail to get channel from pool")
	}
	checkPoolSize(pool, 1, t)
	checkPoolReadySize(pool, 0, t)
	pool.Remove(ch)

}

func TestPoolOverflow(t *testing.T) {
	var poolSize uint16 = 65535
	factory := &mock.MockedChannelFactory{}
	pool := internal.NewPool(factory, poolSize)
	var channels []internal.IChannel

	// Get full pool
	for i := 0; i < int(poolSize); i++ {
		ch, err := pool.Get()
		if err != nil {
			t.Fatalf("Fail to get value from pool")
		}
		channels = append(channels, ch)
	}
	// Try to overflow pool
	_, err := pool.Get()
	if !errors.Is(err, common.PoolLimitReached) {
		t.Fatalf("Invalid error on overflow pool, %s", err)
	}
	for _, ch := range channels {
		pool.Remove(ch)
	}
}
