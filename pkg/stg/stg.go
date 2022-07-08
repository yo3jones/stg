package stg

import (
	"io"
	"time"

	"github.com/google/uuid"
)

type Handle interface {
	io.Reader
	io.Seeker
	io.WriterAt
	Truncate(size int64) error
}

type IdFactory[I comparable] interface {
	New() I
}

func NewUuidFactory() IdFactory[uuid.UUID] {
	return &uuidIdFactory{}
}

type uuidIdFactory struct{}

func (*uuidIdFactory) New() uuid.UUID {
	return uuid.New()
}

func NewStringUuidFactory() IdFactory[string] {
	return &stringUuidFactory{}
}

type stringUuidFactory struct{}

func (*stringUuidFactory) New() string {
	return uuid.NewString()
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

type Nower interface {
	Now() time.Time
}

type nower struct{}

func NewNower() Nower {
	return &nower{}
}

func (*nower) Now() time.Time {
	return time.Now()
}
