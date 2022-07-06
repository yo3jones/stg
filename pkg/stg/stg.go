package stg

import (
	"io"
)

type Handle interface {
	io.Reader
	io.Seeker
	io.WriterAt
	Truncate(size int64) error
}
