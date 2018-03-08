package store

type Index struct {
	m map[uint64]int64
}

func (i *Index) Put(ID uint64, offset int64) {
	i.m[ID] = offset
}

func (i *Index) Get(ID uint64) (int64, bool) {
	v, ok := i.m[ID]
	return v, ok
}

func (i *Index) Delete(ID uint64) {
	delete(i.m, ID)
}

func NewIndex() *Index {
	return &Index{
		m: make(map[uint64]int64),
	}
}
