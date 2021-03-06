package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	"github.com/c4pt0r/memberlist"
	"github.com/c4pt0r/s3lite/meta"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	MetaBlobSize = 2 + 8 + 4 + 64
	Padding      = 8
)

var (
	STORE_MAGIC = []byte{'\xc4', '\xc4'}
)

const (
	NEEDLE_FLAG_DELETE = 1 << iota
)

type Store struct {
	MetaBlob
	conf       *StoreConfig
	fp         *os.File
	lastOffset int64
	idx        *Index

	// peer list (all nodes), available after calling Join
	memberList *memberlist.Memberlist
	nodeInfo   *StorageNodeInfo

	mu sync.Mutex
}

func NewStoreWithIDAndConfig(ID string, cfg *StoreConfig) *Store {
	ret := &Store{}
	ret.MetaBlob.SetID(ID)
	if cfg != nil {
		ret.MetaBlob.Version = cfg.Version
		ret.MetaBlob.MaxSize = cfg.MaxSize
	}
	return ret
}

func (s *Store) Join(nodeName string, nodeGossipAddr string, nodeGossipPort int, peerAddrs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: make it configurable
	cfg := memberlist.DefaultLocalConfig()
	if len(nodeName) > 0 {
		cfg.Name = nodeName
	}
	if len(nodeGossipAddr) > 0 {
		cfg.BindAddr = nodeGossipAddr
	}
	if nodeGossipPort > 0 {
		cfg.BindPort = nodeGossipPort
	}

	s.nodeInfo = &StorageNodeInfo{
		NodeInfo: meta.NodeInfo{
			Type: meta.NODE_TYPE_STORE,
		},
		IsReadOnly: s.IsReadOnly(),
	}

	cfg.Delegate = &StoreNodeDelegate{s.nodeInfo}
	list, err := memberlist.Create(cfg)
	if err != nil {
		return errors.New("Failed to join: " + err.Error())
	}

	_, err = list.Join(peerAddrs)
	if err != nil {
		return errors.New("Failed to join cluster: " + err.Error())
	}

	s.memberList = list
	return nil
}

func (s *Store) SetStoreID(ID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.MetaBlob.SetID(ID)
}

func (s *Store) Open(createIfNotExists bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.MetaBlob.ID()) == 0 {
		panic("missing store id")
	}

	dataFile := s.MetaBlob.ID() + ".dat"

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
	// load index
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

func (s *Store) setReadonly() error {
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

	// boardcast status change
	if s.memberList != nil {
		s.nodeInfo.IsReadOnly = true
		err = s.memberList.UpdateNode(10 * time.Second)
		if err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (s *Store) SetReadOnly() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setReadonly()
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

func (s *Store) ReadID(ID uint64) (*Needle, error) {
	if p, ok := s.idx.Get(ID); ok {
		n, err := s.ReadNeedleWithOffsetAndSize(p.offset, p.size)
		if err != nil {
			return nil, err
		}
		return n, nil
	}
	return nil, nil
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
	panic("todo")
}

func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// close file
	s.fp.Sync()
	s.fp.Close()
	// tell peers it's leaving
	s.memberList.Shutdown()
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
	if uint64(offset) > s.MetaBlob.MaxSize {
		s.setReadonly()
	}
	s.lastOffset = offset
	return offset, uint32(len(buf)), nil
}
