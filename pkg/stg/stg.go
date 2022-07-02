package stg

import (
	"io"
	"time"
)

type Handle interface {
	io.Reader
	io.Seeker
	io.WriterAt
	Truncate(size int64) error
}

type Spec[I comparable] interface {
	Ider[I]
}

type Ider[I comparable] interface {
	GetId() I
}

type MarshalUnmarshaller[I comparable, S Spec[I]] interface {
	Marshaller[I, S]
	Unmarshaller[I, S]
}

type Marshaller[I comparable, S Spec[I]] interface {
	Marshal(v S) ([]byte, error)
}

type Unmarshaller[I comparable, S Spec[I]] interface {
	Unmarshal(data []byte, v S) error
}

type Mutation[T comparable, I comparable] interface {
	Add(field string, from, to any)
	GetFrom() map[string]any
	GetId() I
	GetPartition() string
	GetTimestamp() time.Time
	GetTo() map[string]any
	GetTransactionId() T
	GetType() string
}
