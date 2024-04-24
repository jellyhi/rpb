package rpb

import (
	"fmt"
	//"github.com/gogo/protobuf/proto"
)

type KeyType string

const (
	KeyTypeNone   KeyType = "none"
	KeyTypeString KeyType = "string"
	KeyTypeHash   KeyType = "hash"
	KeyTypeSet    KeyType = "set"
	KeyTypeList   KeyType = "list"
	KeyTypeZSet   KeyType = "zset"
)

type KeyMeta struct {
	KeyType      KeyType
	KeyName      string
	HasPrefix    bool
	ProtoMessage string
}

var keyMetaNamespaces = make(map[string]map[string]*KeyMeta)

func RegisterKeyMeta(namespace string, meta *KeyMeta) {
	metas := keyMetaNamespaces[namespace]
	if metas == nil {
		metas = make(map[string]*KeyMeta)
		keyMetaNamespaces[namespace] = metas
	}
	if _, exist := metas[meta.KeyName]; exist {
		panic(fmt.Sprintf("namespace %s key name:%s repeated", namespace, meta.KeyName))
	}
	metas[meta.KeyName] = meta
}
