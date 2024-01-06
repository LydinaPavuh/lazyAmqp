package lazyAmqp

import (
	"errors"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type RmqConnection struct {
	conf     *RmqClientConf
	origConn *amqp.Connection
	mu       *sync.Mutex
}

func NewConn(conf *RmqClientConf) *RmqConnection {
	return &RmqConnection{conf: conf, mu: &sync.Mutex{}}
}
func (conn *RmqConnection) isOpen() bool {
	return conn.origConn != nil && !conn.origConn.IsClosed()
}

func (conn *RmqConnection) Open() (err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.isOpen() {
		return nil
	}
	conn.origConn, err = amqp.DialConfig(conn.conf.Url, conn.conf.Config)
	return err
}

func (conn *RmqConnection) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if err := conn.origConn.Close(); err != nil {
		if errors.As(err, amqp.ErrClosed) {
			return nil
		}
		return err
	}
	return nil
}

func (conn *RmqConnection) newChannel() (*RmqChannel, error) {
	if !conn.isOpen() {
		if err := conn.Open(); err != nil {
			return nil, err
		}
	}
	rawChan, err := conn.origConn.Channel()
	if err != nil {
		return nil, err
	}
	chUuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return &RmqChannel{Id: chUuid, conn: conn, origChan: rawChan, mu: &sync.Mutex{}}, nil
}

func (conn *RmqConnection) renewChannel(channel *RmqChannel) (err error) {
	if !conn.isOpen() {
		if err := conn.Open(); err != nil {
			return err
		}
	}
	rawChan, err := conn.origConn.Channel()
	if err != nil {
		return err
	}
	channel.origChan = rawChan
	return nil
}
