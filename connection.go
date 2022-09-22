package connectionpool

type Connection interface {
	Close() error
}
