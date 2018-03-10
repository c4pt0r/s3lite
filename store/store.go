package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/c4pt0r/memberlist"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	MetaBlobSize     = 2 + 8 + 4 + 64
	Padding          = 8
	DefaultStoreSize = 4 * 1024 * 1024 * 1024 // 4GB
)

var (
	STORE_MAGIC = []byte{'\xc4', '\xc4'}
)

const (
	NEEDLE_FLAG_DELETE = 1 << iota
)

const (
	STORE_FLAG_READ_ONLY = 1 << iota
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

func (m *MetaBlob) SetFlag(flag int) {
	m.Flags |= uint32(flag)
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
	fp  *os.File
	idx *Index

	mu sync.Mutex
}

func (s *Store) Join(peerAddrs []string) error {
	cfg := memberlist.DefaultLocalConfig()
	cfg.Delegate = &StoreNodeDelegate{
		Meta: "type=storage",
	}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return errors.New("Failed to join: " + err.Error())
	}

	_, err = list.Join(peerAddrs)
	if err != nil {
		return errors.New("Failed to join cluster: " + err.Error())
	}

	// Ask for members of the cluster
	for _, member := range list.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}

	return nil
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
			s.idx = NewIndex()
			return nil
		}
		return errors.New("store: open store error, no such file")
	}
	// open data file
	fp, err := os.OpenFile(dataFile, os.O_RDWR, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	s.fp = fp
	if err := s.loadMetaBlob(); err != nil {
		s.fp.Close()
		return errors.Trace(err)
	}

	s.idx = NewIndex()
	log.Info("Load store successfully, ID: ", s.MetaBlob.ID())
	return nil
}

func (s *Store) loadMetaBlob() error {
	s.fp.Seek(0, 0)
	// read meta blob
	buf := make([]byte, 2)
	_, err := io.ReadFull(s.fp, buf)
	if err != nil {
		return errors.Trace(err)
	}
	if !bytes.Equal(STORE_MAGIC, buf) {
		return errors.New("store: magic not match, invalid store")
	}
	buf = make([]byte, MetaBlobSize)
	_, err = io.ReadFull(s.fp, buf)
	if err != nil {
		return errors.Trace(err)
	}
	return s.MetaBlob.FromBytes(buf)
}

func (s *Store) SetReadOnly() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.loadMetaBlob(); err != nil {
		return errors.Trace(err)
	}

	// set flag
	s.MetaBlob.SetFlag(STORE_FLAG_READ_ONLY)

	// skip magic
	_, err := s.fp.WriteAt(s.MetaBlob.Bytes(), 2)
	if err != nil {
		s.fp.Close()
		return errors.Trace(err)
	}
	s.fp.Sync()
	return nil
}

func (s *Store) IsReadOnly() bool {
	return s.MetaBlob.Flags&STORE_FLAG_READ_ONLY == 1
}

func (s *Store) createNewStoreFile(dataFile string) (*os.File, error) {
	log.Info("Creating store, ID: ", s.MetaBlob.ID())
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

func (s *Store) ReadNeedleAt(offset int64) (*Needle, error) {
	// | id | data size |
	b := make([]byte, 8+8)
	_, err := s.fp.ReadAt(b, offset)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// read data size
	dataSize := binary.LittleEndian.Uint32(b[12:16])
	b = make([]byte, 16+dataSize+4)
	_, err = s.fp.ReadAt(b, offset)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := &Needle{}
	err = n.FromPayload(b)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return n, nil
}

func (s *Store) ReadNeedleWithOffsetAndSize(offset int64, size uint32) (*Needle, error) {
	b := make([]byte, size)
	_, err := s.fp.ReadAt(b, offset)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := &Needle{}
	err = n.FromPayload(b)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return n, nil
}

func (s *Store) DeleteNeedle(n *Needle) {
	s.mu.Lock()
	defer s.mu.Unlock()
}

func (s *Store) WriteNeedle(n *Needle, needSync bool) (int64, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.IsReadOnly() {
		return 0, 0, errors.New("this store is read-only")
	}

	// seek to the end
	offset, err := s.fp.Seek(0, 2)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	// padding
	if offset%Padding != 0 {
		offset = offset + (Padding - offset%Padding)
		offset, err = s.fp.Seek(offset, 0)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	}

	buf := n.Bytes()
	_, err = s.fp.Write(buf)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	if needSync {
		s.fp.Sync()
	}
	// update index
	if payload, ok := s.idx.Get(n.ID); !ok || payload.offset < offset {
		s.idx.Put(n.ID, offset, uint32(len(buf)))
	}

	return offset, uint32(len(buf)), nil
}
