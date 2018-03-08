package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaBlob(t *testing.T) {
	m := new(MetaBlob)
	copy(m.StoreID[0:], []byte("hello"))
	m.MaxSize = DefaultStoreSize
	m.Version = 1

	buf := m.Bytes()
	assert.Equal(t, len(buf), MetaBlobSize)

	fmt.Println(buf)

	m1 := new(MetaBlob)
	m1.FromBytes(buf)
	fmt.Println(*m1)
	assert.EqualValues(t, m1.Version, 1)
	assert.EqualValues(t, m1.ID(), "hello")
}

func TestCreateStore(t *testing.T) {
	s := new(Store)

	copy(s.MetaBlob.StoreID[0:], []byte("hello"))
	s.MetaBlob.MaxSize = DefaultStoreSize
	s.MetaBlob.Version = 1

	err := s.Open("hello.dat", true)
	assert.Nil(t, err)

	s1 := new(Store)
	err = s1.Open("hello.dat", false)
	assert.Nil(t, err)

	assert.EqualValues(t, s1.MetaBlob.ID(), "hello")
	assert.EqualValues(t, s1.MetaBlob.MaxSize, DefaultStoreSize)
	assert.EqualValues(t, s1.MetaBlob.Version, 1)
}

func TestWriteNeedle(t *testing.T) {
}
