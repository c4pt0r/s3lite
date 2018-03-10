package meta

type NodeType int

const (
	NODE_TYPE_STORE NodeType = iota + 1
	NODE_TYPE_METADATA_STORAGE
)

type NodeInfo struct {
	Type NodeType `json:"node_type"`
}
