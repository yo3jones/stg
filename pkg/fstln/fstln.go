package fstln

import (
	"sync"

	datastruc "github.com/yo3jones/datastruc/pkg"
	"github.com/yo3jones/stg/pkg/stg"
)

type Storage interface {
	Read(line []byte) (position Position, n int, isPrefix bool, err error)
	ResetScan() (err error)
}

type storage struct {
	buffer     []byte
	bufferCurr int
	bufferEof  bool
	bufferLen  int
	emptyLines datastruc.Heap[Position]
	handle     stg.Handle
	line       []byte
	lineCurr   int
	linePos    Position
	linePrefix bool
	readLock   sync.Mutex
	readPhase  phase
	scanCurr   int
	scanEof    bool
	scanEnd    int64
}

func New(handle stg.Handle, options ...Option) (stg Storage, err error) {
	return new(handle, options...)
}

func new(handle stg.Handle, options ...Option) (stg *storage, err error) {
	var (
		bufferSize = 1000
		lineSize   = 1000
	)

	for _, option := range options {
		switch option := option.(type) {
		case OptionBufferSize:
			bufferSize = option.value
		}
	}

	stg = &storage{
		buffer: make([]byte, bufferSize),
		handle: handle,
		line:   make([]byte, lineSize),
	}

	if err = stg.ResetScan(); err != nil {
		return nil, err
	}

	return stg, nil
}

type Position struct {
	Offset int
	Len    int
}

var PositionLesser = func(i, j Position) bool {
	if i.Len > j.Len {
		return true
	} else if i.Len < j.Len {
		return false
	}

	return i.Offset > j.Offset
}

type phase int

const (
	phaseRead phase = iota + 1
	phaseEmpty
)

type Option interface {
	isOption() bool
}

type OptionBufferSize struct {
	value int
}

func (option OptionBufferSize) isOption() bool {
	return true
}