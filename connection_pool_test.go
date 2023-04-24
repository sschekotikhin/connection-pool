package connectionpool

import (
	"math/rand"
	"testing"
	"time"
)

const (
	address = "127.0.0.1:12345"
)

func init() {
	rand.Seed(time.Now().UnixMilli())
}

type conn struct {
	a int
}

func (c conn) Ping() error {
	return nil
}

func (c conn) Close() error {
	return nil
}

var defaultConfig = &Config[conn]{
	MinConns:    1,
	MaxConns:    32,
	IdleTimeout: 10 * time.Second,
	MaxLifeTime: 30 * time.Second,
	Factory: func() (conn, error) {
		return conn{
			a: rand.Int(),
		}, nil
	},
}

func TestNew(t *testing.T) {
	t.Run("WithInvalidMinConns", func(t *testing.T) {
		_, err := New(&Config[conn]{MinConns: -1})
		if err.Error() != "min conns should be greater than or equal to 0" {
			t.Fail()
		}
	})
	t.Run("WithInvalidMaxConns", func(t *testing.T) {
		_, err := New(&Config[conn]{
			MinConns: 0,
			MaxConns: 0,
		})
		if err.Error() != "max conns should be greater than 0" {
			t.Fail()
		}
	})
	t.Run("InvalidWhenMinConnsGreaterThanMaxConns", func(t *testing.T) {
		_, err := New(&Config[conn]{
			MinConns: 2,
			MaxConns: 1,
		})
		if err.Error() != "min conns cant be greater than max conns" {
			t.Fail()
		}
	})
	t.Run("WithoutFactory", func(t *testing.T) {
		_, err := New(&Config[conn]{
			MinConns: 0,
			MaxConns: 1,
		})
		if err.Error() != "factory cant be nil" {
			t.Fail()
		}
	})
	t.Run("Ok", func(t *testing.T) {
		_, err := New(defaultConfig)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestConnection(t *testing.T) {
	t.Run("ReturnsConnectionFromPool", func(t *testing.T) {
		pool, err := newDefaultConnectionPool()
		if err != nil {
			t.Error(err)
		}
		defer pool.Close()

		poolSize := len(pool.timestamps)

		if _, err := pool.Connection(); err != nil {
			t.Error(err)
		} else if len(pool.timestamps) != poolSize {
			t.Error("connection returned not from the pool")
		}
	})
	t.Run("ReturnsNewConnection", func(t *testing.T) {
		pool, err := newDefaultConnectionPool()
		if err != nil {
			t.Error(err)
		}
		defer pool.Close()

		poolSize := len(pool.timestamps)
		if _, err := pool.Connection(); err != nil {
			t.Error(err)
		}

		if _, err := pool.Connection(); err != nil {
			t.Error(err)
		} else if len(pool.timestamps) != poolSize+1 {
			t.Error("expected new connection")
		}
	})
	// TODO: tests for requests queue
	// TODO: tests for idle timeout and max conn lifetime
}

func TestPut(t *testing.T) {
	t.Run("PutsConnectionIntoPool", func(t *testing.T) {
		pool, err := newDefaultConnectionPool()
		if err != nil {
			t.Error(err)
		}
		defer pool.Close()

		poolSize := len(pool.conns)
		conn, err := pool.Connection()
		if err != nil {
			t.Error(err)
		}
		if err := pool.Put(conn); err != nil {
			t.Error()
		} else if len(pool.conns) != poolSize {
			t.Errorf("expect len(pool.conns)=%d, got=%d", poolSize, len(pool.conns))
		}
	})
	t.Run("ClosingConnectionWhenPoolIsFilled", func(t *testing.T) {
		pool, err := newDefaultConnectionPool()
		if err != nil {
			t.Error(err)
		}
		defer pool.Close()

		conns := make([]*conn, 0, defaultConfig.MaxConns)
		for i := 0; i < defaultConfig.MaxConns+1; i++ {
			conn, err := pool.Connection()
			if err != nil {
				t.Error(err)
			}
			conns = append(conns, conn)
		}
		for i := 0; i < defaultConfig.MaxConns; i++ {
			if err := pool.Put(conns[i]); err != nil {
				t.Error(err)
			}
		}

		poolSize := len(pool.conns)
		if err := pool.Put(conns[len(conns)-1]); err != nil {
			t.Error()
		} else if len(pool.conns) != poolSize {
			t.Error("expect len(pool.conns) did not changed")
		}
	})
	// TODO: test for requests queue
}

func newDefaultConnectionPool() (*connectionPool[conn], error) {
	pool, err := New(defaultConfig)
	if err != nil {
		return nil, err
	}

	return pool.(*connectionPool[conn]), nil
}
