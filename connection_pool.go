package connectionpool

import (
	"errors"
	"sync"
	"time"
)

type Connectable interface {
	Ping() error
	Close() error
}

type ConnectionPool[T Connectable] interface {
	// Retrieves connection from pool if it exists or opens new connection.
	Connection() (T, error)
	// Returns connection to pool.
	Put(conn T) error
	// Closes all connections and pool.
	Close() error
}

type Config[T Connectable] struct {
	// Min number of connections that will be opened during New() function.
	MinConns uint64
	// Max number of opened connections.
	MaxConns uint64
	// IDLE timeout for every connection.
	IdleTimeout time.Duration
	// Function for creating new connections.
	Factory func() (T, error)
}

type connection[T Connectable] struct {
	conn      T
	timestamp time.Time
}

type connectionPool[T Connectable] struct {
	mutex sync.RWMutex
	conns chan connection[T]

	minConns    uint64
	maxConns    uint64
	idleTimeout time.Duration
	factory     func() (T, error)

	closed bool
}

// Opens new connection pool.
func New[T Connectable](cfg *Config[T]) (ConnectionPool[T], error) {
	if cfg.MaxConns == 0 {
		return nil, errors.New("max conns should be greater than 0")
	} else if cfg.MinConns > cfg.MaxConns {
		return nil, errors.New("min conns cant be greater than max conns")
	}
	if cfg.Factory == nil {
		return nil, errors.New("factory cant be nil")
	}

	pool := &connectionPool[T]{
		mutex:       sync.RWMutex{},
		maxConns:    cfg.MaxConns,
		minConns:    cfg.MinConns,
		idleTimeout: cfg.IdleTimeout,
		factory:     cfg.Factory,
		closed:      false,
	}

	conns := make(chan connection[T], cfg.MaxConns)
	for i := 0; i < int(cfg.MinConns); i++ {
		conn, err := pool.factory()
		if err != nil {
			pool.Close()
			return nil, err
		}

		conns <- connection[T]{
			conn:      conn,
			timestamp: time.Now(),
		}
	}
	pool.conns = conns

	return pool, nil
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool[T]) Connection() (T, error) {
	conns := cp.getConns()

	for {
		select {
		case conn, ok := <-conns:
			if !ok {
				return *new(T), errors.New("connection already closed")
			}
			// closes expired connection
			if cp.idleTimeout > 0 && conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) {
				conn.conn.Close()
				continue
			}
			// closes unhealthy connection
			if err := conn.conn.Ping(); err != nil {
				conn.conn.Close()
				continue
			}

			return conn.conn, nil
		default:
			// TODO: request for new connection if limit exceeded
			conn, err := cp.factory()
			if err != nil {
				return *new(T), err
			}

			return conn, nil
		}
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
	case cp.conns <- connection[T]{conn: conn, timestamp: time.Now()}:
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
		err = conn.conn.Close()
	}

	return err
}

func (cp *connectionPool[T]) getConns() chan connection[T] {
	cp.mutex.RLock()
	conns := cp.conns
	cp.mutex.RUnlock()

	return conns
}
