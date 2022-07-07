package obj

import "github.com/yo3jones/stg/pkg/fstln"

type specMsg[S any] struct {
	op     op
	pos    fstln.Position
	source string
	spec   S
}

type op int

const (
	opNoop op = iota
	opDelete
	// opInsert
	opUpdate
	opDone
)

type optOp struct {
	value op
}

type optSource struct {
	value string
}
