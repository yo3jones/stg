package objbinlog

import (
	"sync"
	"time"

	"github.com/yo3jones/stg/pkg/obj"
	"github.com/yo3jones/stg/pkg/stg"
)

type BinLogStorage interface {
	StartTranaction(objType string) Transaction
}

type Transaction interface {
	End()
	LogDelete(id any, from []byte) (err error)
	LogInsert(id any, to []byte) (err error)
	LogUpdate(id any, from, to []byte) (err error)
}

type binLogStorage[T comparable] struct {
	handle              stg.Handle
	idFactory           obj.IdFactory[T]
	lock                sync.Mutex
	marshalUnmarshaller obj.MarshalUnmarshaller[any]
	nower               obj.Nower
	writeLock           sync.Mutex
}

type transaction[T comparable] struct {
	ended         bool
	objType       string
	stg           *binLogStorage[T]
	timestamp     time.Time
	transactionId T
}

func New[T comparable](
	handle stg.Handle,
	idFactory obj.IdFactory[T],
	marshalUnmarshaller obj.MarshalUnmarshaller[any],
) BinLogStorage {
	return &binLogStorage[T]{
		handle:              handle,
		idFactory:           idFactory,
		marshalUnmarshaller: marshalUnmarshaller,
		nower:               obj.NewNower(),
	}
}

func (stg *binLogStorage[T]) StartTranaction(objType string) Transaction {
	stg.lock.Lock()
	return &transaction[T]{
		objType:       objType,
		stg:           stg,
		timestamp:     stg.nower.Now(),
		transactionId: stg.idFactory.New(),
	}
}
