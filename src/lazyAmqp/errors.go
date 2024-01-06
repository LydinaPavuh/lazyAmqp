package lazyAmqp

import (
	"errors"
)

var PoolLimitReached = errors.New("PoolLimitReached")
var ConsumerAlreadyRunning = errors.New("ConsumerAlreadyRunning")
