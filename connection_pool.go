package connectionpool

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	errMinConnsShouldNotBeNegative = errors.New("min conns should be greater than or equal to 0")
	errMaxConnsShouldBePositive    = errors.New("max conns should be greater than 0")
	errMinIsGreaterThanMax         = errors.New("min conns should be lower than or equal to max conns")
	errNilFactory                  = errors.New("factory cant be nil")
	errPoolAlreadyClosed           = errors.New("connection pool already closed")
	errMaxConnLimitExceeded        = errors.New("max active connections limit exceeded")
)

type Connectable interface {
	Ping() error
	Close() error
}

type ConnectionPool[T Connectable] interface {
	// Returns number of opened connections.
	Len() int
	// Retrieves connection from pool if it exists or opens new connection.
	Connection(ctx context.Context) (T, error)
	// Returns connection to pool.
	Put(conn T) error
	// Closes all connections and pool.
	Close() error
}

type Config[T Connectable] struct {
	// Min number of connections that will be opened during New() function.
	MinConns int
	// Max number of opened connections.
	MaxConns int
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
	connsMutex sync.RWMutex
	conns      chan connection[T]

	requestsMutex sync.RWMutex
	requests      []chan connection[T]

	minConns    int
	maxConns    int
	idleTimeout time.Duration
	factory     func() (T, error)

	closed bool
}

// Opens new connection pool.
func New[T Connectable](cfg *Config[T]) (ConnectionPool[T], error) {
	switch {
	case cfg.MinConns < 0:
		return nil, errMinConnsShouldNotBeNegative
	case cfg.MaxConns <= 0:
		return nil, errMaxConnsShouldBePositive
	case cfg.MinConns > cfg.MaxConns:
		return nil, errMinIsGreaterThanMax
	case cfg.Factory == nil:
		return nil, errNilFactory
	}

	pool := &connectionPool[T]{
		connsMutex:    sync.RWMutex{},
		requestsMutex: sync.RWMutex{},
		maxConns:      cfg.MaxConns,
		minConns:      cfg.MinConns,
		idleTimeout:   cfg.IdleTimeout,
		factory:       cfg.Factory,
		closed:        false,
		requests:      make([]chan connection[T], 0),
	}

	conns := make(chan connection[T], cfg.MaxConns)
	for i := 0; i < cfg.MinConns; i++ {
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
func (cp *connectionPool[T]) Connection(ctx context.Context) (T, error) {
	conns := cp.getConns()

	for {
		select {
		case <-ctx.Done():
			return *new(T), ctx.Err()
		case conn, ok := <-conns:
			if !ok {
				return *new(T), errPoolAlreadyClosed
			}

			// closing expired connection
			cp.connsMutex.RLock()
			connsLen := len(cp.conns)
			cp.connsMutex.RUnlock()
			if cp.idleTimeout > 0 &&
				conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) &&
				connsLen > cp.minConns {
				conn.conn.Close()

				continue
			}
			// closing unhealthy connection
			if err := conn.conn.Ping(); err != nil {
				conn.conn.Close()

				continue
			}

			return conn.conn, nil
		default:
			// max conns exceeded
			if len(cp.conns) >= cp.maxConns {
				// creating new request
				cp.requestsMutex.Lock()
				if cp.requests == nil {
					cp.requestsMutex.Unlock()

					return *new(T), errPoolAlreadyClosed
				}
				request := make(chan connection[T], 1)
				cp.requests = append(cp.requests, request)
				cp.requestsMutex.Unlock()

				// waiting connection from Put()
				var conn connection[T]
				select {
				case <-ctx.Done():
					return *new(T), ctx.Err()
				case c, ok := <-request:
					if !ok {
						return *new(T), errMaxConnLimitExceeded
					}
					conn = c
				}

				// closing expired connection
				cp.connsMutex.RLock()
				connsLen := len(cp.conns)
				cp.connsMutex.RUnlock()
				if cp.idleTimeout > 0 &&
					conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) &&
					connsLen > cp.minConns {
					conn.conn.Close()

					continue
				}
				// closing unhealthy connection
				if err := conn.conn.Ping(); err != nil {
					conn.conn.Close()

					continue
				}

				return conn.conn, nil
			}

			// creating new connection
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
	cp.connsMutex.RLock()
	defer cp.connsMutex.RUnlock()

	if cp.conns == nil {
		return conn.Close()
	}

	cp.requestsMutex.Lock()
	defer cp.requestsMutex.Unlock()
	if requestsLen := len(cp.requests); requestsLen > 0 {
		// FIFO, pushing connection to oldest request
		request := cp.requests[0]
		copy(cp.requests, cp.requests[1:])
		cp.requests = cp.requests[:requestsLen-1]

		request <- connection[T]{conn: conn, timestamp: time.Now()}

		return nil
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
	cp.connsMutex.Lock()
	if cp.closed {
		cp.connsMutex.Unlock()

		return nil
	}
	cp.closed = true
	conns := cp.conns
	cp.conns = nil
	cp.connsMutex.Unlock()

	if conns == nil {
		return nil
	}

	close(conns)
	var err error
	for conn := range conns {
		err = conn.conn.Close()
	}

	cp.requestsMutex.Lock()
	requests := cp.requests
	cp.requests = nil
	cp.requestsMutex.Unlock()

	for _, request := range requests {
		if request != nil {
			close(request)
		}
	}

	return err
}

// Returns number of opened connections.
func (cp *connectionPool[T]) Len() int {
	cp.connsMutex.RLock()
	conns := cp.conns
	cp.connsMutex.RUnlock()

	if conns == nil {
		return 0
	}

	return len(conns)
}

func (cp *connectionPool[T]) getConns() chan connection[T] {
	cp.connsMutex.RLock()
	conns := cp.conns
	cp.connsMutex.RUnlock()

	return conns
}
