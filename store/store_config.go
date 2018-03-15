package store

const (
	DefaultStoreVer  = 1
	DefaultStoreSize = 100 * 1024 * 1024 * 1024 // 100GB
)

type StoreConfig struct {
	Version uint16
	MaxSize uint64
}

func NewDefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		Version: DefaultStoreVer,
		MaxSize: DefaultStoreSize,
	}
}
