package connectionpool

import (
	"errors"
	"sync"
)

type Config struct {
	MinConns uint64
	MaxConns uint64
	Factory  func() (Connection, error)
}

type ConnectionPool interface {
	Connection() (Connection, error)
	Close() error
}

type connectionPool struct {
	mutex sync.RWMutex
	conns chan Connection

	cfg *Config

	closed bool
}

func New(cfg *Config) (ConnectionPool, error) {
	if cfg.MaxConns == 0 {
		return nil, errors.New("max conns should be greater than 0")
	} else if cfg.MinConns > cfg.MaxConns {
		return nil, errors.New("min conns cant be lower than max conns")
	}
	if cfg.Factory == nil {
		return nil, errors.New("factory cant be nil")
	}

	pool := &connectionPool{
		mutex:  sync.RWMutex{},
		cfg:    cfg,
		closed: false,
	}

	conns := make(chan Connection, cfg.MaxConns)
	for i := 0; i < int(cfg.MinConns); i++ {
		conn, err := cfg.Factory()
		if err != nil {
			return nil, err
		}

		conns <- conn
	}
	pool.conns = conns

	return pool, nil
}

func (cp *connectionPool) Close() error {
	if cp.closed {
		return nil
	}

	cp.mutex.Lock()
	cp.closed = true
	conns := cp.conns
	cp.conns = nil
	cp.mutex.Unlock()

	if conns == nil {
		return nil
	}

	var err error
	for conn := range conns {
		err = conn.Close()
	}

	return err
}

func (cp *connectionPool) Connection() (Connection, error) {
	conns := cp.getConns()

	select {
	case conn, ok := <-conns:
		if conn == nil || !ok {
			return nil, errors.New("connection already closed")
		}

		return conn, nil
	default:
		conn, err := cp.cfg.Factory()
		if err != nil {
			return nil, err
		}

		return conn, nil
	}
}

func (cp *connectionPool) Put(conn Connection) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	if cp.conns == nil {
		return conn.Close()
	}

	select {
	case cp.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

func (cp *connectionPool) getConns() chan Connection {
	cp.mutex.RLock()
	conns := cp.conns
	cp.mutex.RUnlock()

	return conns
}
