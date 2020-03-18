package storage

import (
	protocol "github.com/wangaoone/LambdaObjectstore/src/types"
)

type Backpack interface {
	func IsConsistent(*protocol.Meta) bool
}
