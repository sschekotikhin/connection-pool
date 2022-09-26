# connection-pool

---

Universal connection pool on generics.

#### Feature:

- More versatile, The connection type in the connection pool is generic, making it more versatile.
- More configurable, The connection supports setting the maximum idle time, the timeout connection will be closed and discarded, which can avoid the problem of automatic connection failure when idle.
- Support user setting ping method, used to check the connectivity of connection, invalid connection will be discarded.
- Support connection waiting, When the connection pool is full, support for connection waiting.

---

#### Install and usage:

```
go get github.com/sschekotikhin/connection-pool
```

#### Example:
```golang
package main

import (
	"log"
	"time"

	connectionpool "github.com/sschekotikhin/connection-pool"
)

type Conn struct{}

func (c Conn) Ping() error {
	return nil
}

func (c Conn) Close() error {
	return nil
}

func main() {
	pool, err := connectionpool.New(
		&connectionpool.Config[Conn]{
			MinConns:    10,
			MaxConns:    50,
			IdleTimeout: 10 * time.Second,
			Factory: func() (Conn, error) {
				return Conn{}, nil
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

  // get connection from the pool
	conn, err := pool.Connection()
	if err != nil {
		log.Fatal(err)
	}
	// do something with received connection
  // return connection to the pool
	pool.Put(conn)
}
```

---

#### Remarks:

The connection pool implementation refers to pool https://github.com/silenceper/pool, thanks.

---

The MIT License (MIT) - see LICENSE for more details
