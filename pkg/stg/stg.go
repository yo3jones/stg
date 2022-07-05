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

type MarshalUnmarshaller[S any] interface {
	Marshaller[S]
	Unmarshaller[S]
}

type Marshaller[S any] interface {
	Marshal(v S) ([]byte, error)
}

type Unmarshaller[S any] interface {
	Unmarshal(data []byte, v S) error
}

type Mutation[T comparable] interface {
	Add(field string, from, to any)
	GetFrom() map[string]any
	GetId() any
	GetPartition() string
	GetTimestamp() time.Time
	GetTo() map[string]any
	GetTransactionId() T
	GetType() string
}
