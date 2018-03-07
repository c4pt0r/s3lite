package store

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/juju/errors"
)

const (
	MetaBlobSize     = 2 + 8 + 4 + 64
	DefaultStoreSize = 4 * 1024 * 1024 * 1024 // 4GB
)

var (
	STORE_MAGIC = []byte{'\xc4', '\xc4'}
)

// fixed size meta
type MetaBlob struct {
	Version uint16
	MaxSize uint64
	Flags   uint32
	StoreID [64]byte
}

func (m *MetaBlob) Bytes() []byte {
	buf := make([]byte, MetaBlobSize)
	binary.LittleEndian.PutUint16(buf, m.Version)
	binary.LittleEndian.PutUint64(buf[2:10], m.MaxSize)
	binary.LittleEndian.PutUint32(buf[10:14], m.Flags)
	copy(buf[14:], m.StoreID[:])
	return buf
}

func (m *MetaBlob) FromBytes(buf []byte) error {
	if len(buf) != MetaBlobSize {
		return errors.New("invalid meta blob")
	}
	m.Version = binary.LittleEndian.Uint16(buf[0:2])
	m.MaxSize = binary.LittleEndian.Uint64(buf[2:10])
	m.Flags = binary.LittleEndian.Uint32(buf[10:14])
	copy(m.StoreID[:], buf[14:])
	return nil
}

func (m MetaBlob) ID() string {
	var buf []byte
	for _, c := range m.StoreID {
		if c == 0x0 {
			break
		}
		buf = append(buf, c)
	}
	return string(buf)
}

type Store struct {
	MetaBlob
	fp *os.File

	mu sync.Mutex
}

func (s *Store) Open(dataFile string, createIfNotExists bool) error {
	_, err := os.Stat(dataFile)
	if os.IsNotExist(err) {
		if createIfNotExists {
			// create new data store
			fp, err := s.createNewStoreFile(dataFile)
			if err != nil {
				return errors.Trace(err)
			}
			s.fp = fp
			return nil
		}
		return errors.New("store: open store error, no such file")
	}
	// open data file
	fp, err := os.OpenFile(dataFile, os.O_RDWR, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	// read meta blob
	buf := make([]byte, 2)
	_, err = io.ReadFull(fp, buf)
	if err != nil {
		return errors.Trace(err)
	}

	buf = make([]byte, MetaBlobSize)
	_, err = io.ReadFull(fp, buf)
	if err != nil {
		return errors.Trace(err)
	}

	s.MetaBlob.FromBytes(buf)
	s.fp = fp
	return nil
}

func (s *Store) createNewStoreFile(dataFile string) (*os.File, error) {
	fp, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// write magic
	_, err = fp.Write(STORE_MAGIC)
	if err != nil {
		fp.Close()
		return nil, errors.Trace(err)
	}
	// write meta blob
	_, err = fp.Write(s.MetaBlob.Bytes())
	if err != nil {
		fp.Close()
		return nil, errors.Trace(err)
	}
	fp.Sync()
	return fp, nil
}
