package connectionpool

import (
	"errors"
	"sync"
)

type Closable interface {
	Close() error
}

type ConnectionPool[T Closable] interface {
	// Retrieves connection from pool if it exists or opens new connection.
	Connection() (T, error)
	// Returns connection to pool.
	Put(conn T) error
	// Closes all connections and pool.
	Close() error
}

type Config struct {
	// Min number of connections that will be opened during New() function.
	MinConns uint64
	// Max number of opened connections.
	MaxConns uint64
}

type connectionPool[T Closable] struct {
	mutex sync.RWMutex
	conns chan T

	minConns uint64
	maxConns uint64
	factory  func() (T, error)

	closed bool
}

// Opens new connection pool.
func New[T Closable](cfg *Config, factory func() (T, error)) (ConnectionPool[T], error) {
	if cfg.MaxConns == 0 {
		return nil, errors.New("max conns should be greater than 0")
	} else if cfg.MinConns > cfg.MaxConns {
		return nil, errors.New("min conns cant be greater than max conns")
	}
	if factory == nil {
		return nil, errors.New("factory cant be nil")
	}

	pool := &connectionPool[T]{
		mutex:    sync.RWMutex{},
		maxConns: cfg.MaxConns,
		minConns: cfg.MinConns,
		factory:  factory,
		closed:   false,
	}

	conns := make(chan T, cfg.MaxConns)
	for i := 0; i < int(cfg.MinConns); i++ {
		conn, err := pool.factory()
		if err != nil {
			return nil, err
		}

		conns <- conn
	}
	pool.conns = conns

	return pool, nil
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool[T]) Connection() (T, error) {
	conns := cp.getConns()

	select {
	case conn, ok := <-conns:
		if !ok {
			return *new(T), errors.New("connection already closed")
		}

		return conn, nil
	default:
		conn, err := cp.factory()
		if err != nil {
			return *new(T), err
		}

		return conn, nil
	}
}

// Returns connection to the pool.
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

// Closes all connections and pool.
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

func (cp *connectionPool[T]) getConns() chan T {
	cp.mutex.RLock()
	conns := cp.conns
	cp.mutex.RUnlock()

	return conns
}
