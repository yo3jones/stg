package stg

import "io"

type Handle interface {
	io.Reader
	io.Seeker
}
