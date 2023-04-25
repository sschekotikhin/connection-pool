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
	// Returns number of opened connections.
	Len() int
	// Retrieves connection from pool if it exists or opens new connection.
	Connection() (*T, error)
	// Returns connection to pool.
	Put(conn *T) error
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
	// Max lifetime for every connection (0 - connection never will be reopened)
	MaxLifeTime time.Duration
	// Function for creating new connections.
	Factory func() (T, error)
}

type connection[T Connectable] struct {
	conn      *T
	timestamp time.Time
}

type connectionPool[T Connectable] struct {
	connsMutex sync.RWMutex
	conns      chan connection[T]
	// TODO: is T is struct{}, when all connections have similar timestamp
	timestamps map[*T]time.Time

	requestsMutex sync.RWMutex
	requests      []chan connection[T]

	minConns    int
	maxConns    int
	idleTimeout time.Duration
	maxLifeTime time.Duration
	factory     func() (T, error)

	closed bool
}

// Opens new connection pool.
func New[T Connectable](cfg *Config[T]) (ConnectionPool[T], error) {
	if cfg.MinConns < 0 {
		return nil, errors.New("min conns should be greater than or equal to 0")
	} else if cfg.MaxConns <= 0 {
		return nil, errors.New("max conns should be greater than 0")
	} else if cfg.MinConns > cfg.MaxConns {
		return nil, errors.New("min conns cant be greater than max conns")
	}
	if cfg.Factory == nil {
		return nil, errors.New("factory cant be nil")
	}

	pool := &connectionPool[T]{
		connsMutex:    sync.RWMutex{},
		requestsMutex: sync.RWMutex{},
		maxConns:      cfg.MaxConns,
		minConns:      cfg.MinConns,
		idleTimeout:   cfg.IdleTimeout,
		maxLifeTime:   cfg.MaxLifeTime,
		factory:       cfg.Factory,
		closed:        false,
		requests:      make([]chan connection[T], 0),
	}

	conns := make(chan connection[T], cfg.MaxConns)
	timestamps := make(map[*T]time.Time, cfg.MaxConns)
	for i := 0; i < int(cfg.MinConns); i++ {
		conn, err := pool.factory()
		if err != nil {
			pool.Close()
			return nil, err
		}
		now := time.Now()

		conns <- connection[T]{conn: &conn, timestamp: now}
		timestamps[&conn] = now
	}
	pool.conns = conns
	pool.timestamps = timestamps

	return pool, nil
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool[T]) Connection() (*T, error) {
	conns, timestamps := cp.getConns()

	for {
		select {
		case conn, ok := <-conns:
			if !ok {
				return new(T), errors.New("connection already closed")
			}

			if (cp.maxLifeTime > 0 || cp.idleTimeout > 0) && len(conns) > cp.minConns {
				now := time.Now()

				// closing old connection
				if cp.maxLifeTime > 0 {
					if timestamp, ok := timestamps[conn.conn]; ok && timestamp.Add(cp.maxLifeTime).Before(now) {
						cp.freeConn(conn.conn)

						continue
					}
				}
				// closing expired connection
				if cp.idleTimeout > 0 && conn.timestamp.Add(cp.idleTimeout).Before(now) {
					cp.freeConn(conn.conn)

					continue
				}
			}
			// closing unhealthy connection
			if err := (*conn.conn).Ping(); err != nil {
				cp.freeConn(conn.conn)

				continue
			}

			return conn.conn, nil
		default:
			// max conns exceeded
			if len(cp.conns) >= int(cp.maxConns) {
				// creating new request
				cp.requestsMutex.Lock()
				if cp.requests == nil {
					cp.requestsMutex.Unlock()

					return new(T), errors.New("connection already closed")
				}
				request := make(chan connection[T], 1)
				cp.requests = append(cp.requests, request)
				cp.requestsMutex.Unlock()

				// waiting connection from Put()
				conn, ok := <-request
				if !ok {
					return new(T), errors.New("max active connections limit exceeded")
				}

				cp.connsMutex.RLock()
				connsLen := len(cp.conns)
				timestamps := cp.timestamps
				cp.connsMutex.RUnlock()

				if (cp.maxLifeTime > 0 || cp.idleTimeout > 0) && connsLen > cp.minConns {
					now := time.Now()

					// closing old connection
					if cp.maxLifeTime > 0 {
						if timestamp, ok := timestamps[conn.conn]; ok && timestamp.Add(cp.maxLifeTime).Before(now) {
							cp.freeConn(conn.conn)

							continue
						}
					}
					// closing expired connection
					if cp.idleTimeout > 0 && conn.timestamp.Add(cp.idleTimeout).Before(now) {
						cp.freeConn(conn.conn)

						continue
					}
				}
				// closing unhealthy connection
				if err := (*conn.conn).Ping(); err != nil {
					cp.freeConn(conn.conn)

					continue
				}

				return conn.conn, nil
			}

			// creating new connection
			cp.connsMutex.Lock()
			defer cp.connsMutex.Unlock()

			conn, err := cp.factory()
			if err != nil {
				return new(T), err
			}
			if cp.timestamps != nil {
				cp.timestamps[&conn] = time.Now()
			}

			return &conn, nil
		}
	}
}

// Returns connection to the pool.
func (cp *connectionPool[T]) Put(conn *T) error {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()

	if cp.conns == nil {
		delete(cp.timestamps, conn)

		return (*conn).Close()
	}

	cp.requestsMutex.Lock()
	defer cp.requestsMutex.Unlock()
	if requestsLen := len(cp.requests); requestsLen > 0 {
		// FIFO, pushing connection to oldest request
		request := cp.requests[0]
		copy(cp.requests, cp.requests[1:])
		cp.requests = cp.requests[:requestsLen-1]

		now := time.Now()
		request <- connection[T]{conn: conn, timestamp: now}
		if cp.timestamps != nil {
			cp.timestamps[conn] = now
		}

		return nil
	}

	now := time.Now()

	select {
	case cp.conns <- connection[T]{conn: conn, timestamp: now}:
		if cp.timestamps != nil {
			cp.timestamps[conn] = now
		}

		return nil
	default:
		delete(cp.timestamps, conn)

		return (*conn).Close()
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
	cp.timestamps = nil
	cp.connsMutex.Unlock()

	if conns == nil {
		return nil
	}

	close(conns)
	var err error
	for conn := range conns {
		err = (*conn.conn).Close()
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

func (cp *connectionPool[T]) getConns() (chan connection[T], map[*T]time.Time) {
	cp.connsMutex.RLock()
	conns := cp.conns
	timestamps := cp.timestamps
	cp.connsMutex.RUnlock()

	return conns, timestamps
}

func (cp *connectionPool[T]) freeConn(conn *T) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()

	(*conn).Close()
	delete(cp.timestamps, conn)
}
