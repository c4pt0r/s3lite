package meta

type Store struct {
	ID    string      `json:"id"` // leave this empty for unregistered store
	Addr  string      `json:"addr"`
	Desc  string      `json:"desc"`
	Group *WriteGroup `json:"group"`
}

type WriteGroup struct {
	ID     string   `json:"id"`
	Stores []*Store `json:"stores"`
}

type Bucket struct {
	Name string `json:"name"`
}

type MetaServer interface {
	AddStore(s *Store) error
	GetOnlineStoreList() ([]*Store, error)

	NewWriteGroup(ID string, stores []*Store) (*WriteGroup, error)
	GetAllWriteGroups() ([]*WriteGroup, error)
}
