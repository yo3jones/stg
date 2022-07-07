package obj

import (
	"io"
	"sync"

	"github.com/yo3jones/stg/pkg/fstln"
)

type readController[S any] struct {
	bufferLen           int
	ch                  chan specMsg[S]
	concurrency         int
	errCh               chan error
	factory             SpecFactory[S]
	filters             Matcher[S]
	lock                sync.Mutex
	op                  op
	source              string
	stg                 fstln.Storage
	marshalUnmarshaller MarshalUnmarshaller[S]
}

type readControllerOpt interface {
	isReadControllerOpt() bool
}

func (opt optBufferLen) isReadControllerOpt() bool {
	return true
}

func (opt optConcurrency) isReadControllerOpt() bool {
	return true
}

func (opt optOp) isReadControllerOpt() bool {
	return true
}

func (opt optSource) isReadControllerOpt() bool {
	return true
}

func newReadController[S any](
	ch chan specMsg[S],
	errCh chan error,
	factory SpecFactory[S],
	filters Matcher[S],
	stg fstln.Storage,
	marshalUnmarshaller MarshalUnmarshaller[S],
	opts ...readControllerOpt,
) *readController[S] {
	controller := &readController[S]{
		bufferLen:           1000,
		ch:                  ch,
		concurrency:         10,
		errCh:               errCh,
		factory:             factory,
		filters:             filters,
		op:                  opNoop,
		source:              "",
		stg:                 stg,
		marshalUnmarshaller: marshalUnmarshaller,
	}

	for _, opt := range opts {
		opt.isReadControllerOpt()
		switch opt := opt.(type) {
		case optBufferLen:
			controller.bufferLen = opt.value
		case optConcurrency:
			controller.concurrency = opt.value
		case optOp:
			controller.op = opt.value
		case optSource:
			controller.source = opt.value
		}
	}

	return controller
}

func (controller *readController[S]) Start() {
	var (
		err       error
		waitGroup sync.WaitGroup
	)

	if err = controller.stg.ResetScan(); err != nil {
		controller.errCh <- err
		return
	}

	for i := 0; i < controller.concurrency; i++ {
		proc := &readProcess[S]{
			buffer:     make([]byte, controller.bufferLen),
			controller: controller,
		}

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			proc.Start()
		}()
	}

	waitGroup.Wait()

	controller.ch <- specMsg[S]{
		op:     opDone,
		source: controller.source,
	}
}

type readProcess[S any] struct {
	buffer     []byte
	controller *readController[S]
}

func (proc *readProcess[S]) Start() {
	var (
		err error
		pos fstln.Position
		msg specMsg[S]
	)

	for {
		if pos, err = proc.read(); err != nil && err != io.EOF {
			proc.controller.errCh <- err
			break
		} else if pos == fstln.EOF {
			break
		}

		if msg, err = proc.unmarshal(pos); err != nil {
			proc.controller.errCh <- err
			break
		}

		if !proc.controller.filters.Match(msg.spec) {
			continue
		}

		proc.controller.ch <- msg
	}
}

func (proc *readProcess[S]) unmarshal(
	pos fstln.Position,
) (msg specMsg[S], err error) {
	data := proc.buffer[:pos.Len]
	s := proc.controller.factory.New()

	err = proc.controller.marshalUnmarshaller.Unmarshal(data, s)
	if err != nil {
		return msg, err
	}

	return specMsg[S]{
		op:     proc.controller.op,
		pos:    pos,
		source: proc.controller.source,
		spec:   s,
	}, nil
}

func (proc *readProcess[S]) read() (pos fstln.Position, err error) {
	proc.controller.lock.Lock()
	defer proc.controller.lock.Unlock()

	var (
		buffer    = proc.buffer
		bufferLen int
		isPrefix  bool
	)

	for {
		pos, _, isPrefix, err = proc.controller.stg.Read(buffer)
		if err != nil && err != io.EOF {
			return pos, err
		} else if pos == fstln.EOF {
			return pos, io.EOF
		}

		if !isPrefix {
			return pos, err
		}

		bufferLen = len(proc.buffer)
		proc.buffer = append(
			proc.buffer,
			make([]byte, proc.controller.bufferLen)...)
		buffer = proc.buffer[bufferLen:]
	}
}
