package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
)

var (
	HEADER_MAGIC = []byte{'\xc4', '\xc4'}
	FOOT_MAGIC   = []byte{'\x4c', '\x4c'}
)

// sizeof(needle header) = 8 + 4 + 4 + 4 = 20
const needleHeaderSz int = 20

type needleHeader struct {
	key    uint64 // :8
	altKey uint32 // :4
	flags  uint32 // :4
	size   uint32 // :4
}

func (h *needleHeader) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, h.key)
	buf.Write(bs)

	bs = make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, h.altKey)
	buf.Write(bs)

	binary.LittleEndian.PutUint32(bs, h.flags)
	buf.Write(bs)

	binary.LittleEndian.PutUint32(bs, h.size)
	buf.Write(bs)
	return buf.Bytes()
}

func (h *needleHeader) fromBytes(buf []byte) error {
	rdr := bytes.NewReader(buf)

	key := make([]byte, 8)
	_, err := rdr.Read(key)
	if err != nil {
		return errors.New("read key error")
	}

	altKey := make([]byte, 4)
	_, err = rdr.Read(altKey)
	if err != nil {
		return errors.New("read altKey error")
	}

	flags := make([]byte, 4)
	_, err = rdr.Read(flags)
	if err != nil {
		return errors.New("read flags error")
	}

	size := make([]byte, 4)
	_, err = rdr.Read(size)
	if err != nil {
		return errors.New("read size error")
	}

	h.key = binary.LittleEndian.Uint64(key)
	h.altKey = binary.LittleEndian.Uint32(altKey)
	h.flags = binary.LittleEndian.Uint32(flags)
	h.size = binary.LittleEndian.Uint32(size)

	return nil
}

type needle struct {
	hdr  *needleHeader
	data []byte
}

type BlobFile struct {
	path string
	fp   *os.File // always point to end of file
	rdr  *os.File
}

func NewBlobFile(path string) *BlobFile {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	rdr, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}

	return &BlobFile{
		path: path,
		fp:   fp,
		rdr:  rdr,
	}
}

func (f *BlobFile) Close() {
	f.fp.Sync()
	f.fp.Close()
	f.rdr.Close()
}

func (f *BlobFile) readNeedleAt(offset int64) (*needle, error) {
	_, err := f.rdr.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	hdrMagic := make([]byte, 2)
	_, err = f.rdr.Read(hdrMagic)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(hdrMagic, HEADER_MAGIC) != 0 {
		return nil, errors.New("header magic doesn't match, data file might corrupt")
	}

	hdr := make([]byte, needleHeaderSz)
	_, err = f.rdr.Read(hdr)
	if err != nil {
		return nil, err
	}

	needleHdr := &needleHeader{}
	err = needleHdr.fromBytes(hdr)
	if err != nil {
		return nil, err
	}

	data := make([]byte, needleHdr.size)
	_, err = f.rdr.Read(data)
	if err != nil {
		return nil, err
	}

	footMagic := make([]byte, 2)
	_, err = f.rdr.Read(footMagic)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(footMagic, FOOT_MAGIC) != 0 {
		return nil, errors.New("foot magic doesn't match, data file might corrupt")
	}
	// read checksum
	crc := make([]byte, 4)
	_, err = f.rdr.Read(crc)
	if err != nil {
		return nil, err
	}
	crcData := crc32.ChecksumIEEE(data)
	if crcData != binary.LittleEndian.Uint32(crc) {
		return nil, errors.New("checksum doesn't match")
	}

	return &needle{
		hdr:  needleHdr,
		data: data,
	}, nil
}

func (f *BlobFile) writeBlob(key uint64, altKey, flags uint32, data []byte) (int64, error) {
	hdr := &needleHeader{
		key:    key,
		altKey: altKey,
		flags:  flags,
		size:   uint32(len(data)),
	}
	f.fp.Write(HEADER_MAGIC)
	// write header
	_, err := f.fp.Write(hdr.toBytes())
	if err != nil {
		return -1, err
	}
	// write data
	_, err = f.fp.Write(data)
	if err != nil {
		return -1, err
	}
	f.fp.Write(FOOT_MAGIC)
	// write checksum
	crc := crc32.ChecksumIEEE(data)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, crc)
	f.fp.Write(bs)
	// TODO write padding
	// TODO add flag to control fsync
	err = f.fp.Sync()
	if err != nil {
		return -1, err
	}
	st, _ := f.fp.Stat()
	return st.Size(), nil
}
