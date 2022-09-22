package connectionpool

import (
	"errors"
	"sync"
)

type Closable interface {
	Close() error
}

type ConnectionPool[T Closable] interface {
	Connection() (T, error)
	Close() error
}

type Config[T Closable] struct {
	MinConns uint64
	MaxConns uint64
	Factory  func() (T, error)
}

type connectionPool[T Closable] struct {
	mutex sync.RWMutex
	conns chan T

	cfg *Config[T]

	closed bool
}

func New[T Closable](cfg *Config[T]) (ConnectionPool[T], error) {
	if cfg.MaxConns == 0 {
		return nil, errors.New("max conns should be greater than 0")
	} else if cfg.MinConns > cfg.MaxConns {
		return nil, errors.New("min conns cant be lower than max conns")
	}
	if cfg.Factory == nil {
		return nil, errors.New("factory cant be nil")
	}

	pool := &connectionPool[T]{
		mutex:  sync.RWMutex{},
		cfg:    cfg,
		closed: false,
	}

	conns := make(chan T, cfg.MaxConns)
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

func (cp *connectionPool[T]) Close() error {
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

func (cp *connectionPool[T]) Connection() (T, error) {
	conns := cp.getConns()

	select {
	case conn, ok := <-conns:
		if !ok {
			return *new(T), errors.New("connection already closed")
		}

		return conn, nil
	default:
		conn, err := cp.cfg.Factory()
		if err != nil {
			return *new(T), err
		}

		return conn, nil
	}
}

func (cp *connectionPool[T]) Put(conn T) error {
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

func (cp *connectionPool[T]) getConns() chan T {
	cp.mutex.RLock()
	conns := cp.conns
	cp.mutex.RUnlock()

	return conns
}
