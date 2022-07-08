package objbinlog

import (
	"sync"
	"time"

	"github.com/yo3jones/stg/pkg/stg"
)

type BinLogStorage interface {
	StartTransaction(objType string) Transaction
}

type Transaction interface {
	End()
	LogDelete(id any, from []byte) (err error)
	LogInsert(id any, to []byte) (err error)
	LogUpdate(id any, from, to []byte) (err error)
}

type binLogStorage[T comparable] struct {
	handle              stg.Handle
	idFactory           stg.IdFactory[T]
	lock                sync.Mutex
	marshalUnmarshaller stg.MarshalUnmarshaller[any]
	nower               stg.Nower
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
	idFactory stg.IdFactory[T],
	marshalUnmarshaller stg.MarshalUnmarshaller[any],
	opts ...OptBinLogStorage,
) BinLogStorage {
	stg := &binLogStorage[T]{
		handle:              handle,
		idFactory:           idFactory,
		marshalUnmarshaller: marshalUnmarshaller,
		nower:               stg.NewNower(),
	}

	for _, opt := range opts {
		opt.isBinLogStorageOpt()
		switch opt := opt.(type) {
		case OptNower:
			stg.nower = opt.Value
		}
	}

	return stg
}

func (stg *binLogStorage[T]) StartTransaction(objType string) Transaction {
	stg.lock.Lock()
	return &transaction[T]{
		objType:       objType,
		stg:           stg,
		timestamp:     stg.nower.Now(),
		transactionId: stg.idFactory.New(),
	}
}

type OptBinLogStorage interface {
	isBinLogStorageOpt() bool
}

type OptNower struct {
	Value stg.Nower
}

func (opt OptNower) isBinLogStorageOpt() bool {
	return true
}
