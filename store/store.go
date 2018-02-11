package store

type Store interface {
	Put(key []byte, altKey []byte, blob []byte) error
	Get(key []byte, altKey []byte) ([]byte, error)
	Delete(key []byte, altKey []byte) error
}
