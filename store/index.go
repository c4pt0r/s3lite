package store

type MemIndex map[uint64]map[uint32]int64

type Index struct {
	m MemIndex
}

func NewIndex() *Index {
	m := make(MemIndex)
	return &Index{
		m: m,
	}
}

func (i *Index) Close() {

}

func (i *Index) Put(key uint64, altKey int32, offset int64) {
	if subDict, ok := i.m[key]; ok {
		subDict[altKey] = offset
	} else {
		i.m[key] = make(map[uint32]int64)
		i.m[key][altKey] = offset
	}
}
