package lazyAmqp

import (
	"errors"
	"fmt"
	"github.com/LydinaPavuh/lazyAmqp/common"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"sync"
)

type RmqConnection struct {
	conf     *common.RmqConfig
	origConn *amqp.Connection
	mu       *sync.Mutex
}

func NewConn(conf *common.RmqConfig) *RmqConnection {
	return &RmqConnection{conf: conf, mu: &sync.Mutex{}}
}
func (conn *RmqConnection) IsOpen() bool {
	return conn.origConn != nil && !conn.origConn.IsClosed()
}

func (conn *RmqConnection) Open() (err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.IsOpen() {
		return nil
	}
	conn.origConn, err = amqp.DialConfig(conn.conf.Url, conn.conf.Config)
	if err == nil {
		slog.Info("Connected to broker", slog.Any("host", conn.origConn.RemoteAddr()))
	} else {
		slog.Error(fmt.Sprintf("Error on to broker %s", err), slog.Any("error", err))
	}
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

func (conn *RmqConnection) checkConnection() error {
	if !conn.IsOpen() {
		slog.Info("Connection to broker lost, try reconnect", slog.Any("host", conn.origConn.RemoteAddr()))
		return conn.Open()
	}
	return nil
}

func (conn *RmqConnection) NewChannel() (*RmqChannel, error) {
	if err := conn.checkConnection(); err != nil {
		return nil, err
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
	if !conn.IsOpen() {
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
