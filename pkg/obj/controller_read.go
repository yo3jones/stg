package obj

import (
	"io"
	"sync"

	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/stg"
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
	marshalUnmarshaller stg.MarshalUnmarshaller[S]
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

// func (opt optSource) isReadControllerOpt() bool {
// 	return true
// }

func newReadController[S any](
	ch chan specMsg[S],
	errCh chan error,
	factory SpecFactory[S],
	filters Matcher[S],
	stg fstln.Storage,
	marshalUnmarshaller stg.MarshalUnmarshaller[S],
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
			// case optSource:
			// 	controller.source = opt.value
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
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			controller.startProc()
		}()
	}

	waitGroup.Wait()

	controller.ch <- specMsg[S]{
		op:     opDone,
		source: controller.source,
	}
}

func (controller *readController[S]) startProc() {
	var (
		data []byte
		err  error
		pos  fstln.Position
		msg  specMsg[S]
	)

	for {
		if pos, data, err = controller.read(); err != nil && err != io.EOF {
			controller.errCh <- err
			break
		} else if pos == fstln.EOF {
			break
		}

		if msg, err = controller.unmarshal(pos, data); err != nil {
			controller.errCh <- err
			break
		}

		if !controller.filters.Match(msg.spec) {
			continue
		}

		controller.ch <- msg
	}
}

func (controller *readController[S]) unmarshal(
	pos fstln.Position,
	data []byte,
) (msg specMsg[S], err error) {
	s := controller.factory.New()

	err = controller.marshalUnmarshaller.Unmarshal(data, s)
	if err != nil {
		return msg, err
	}

	return specMsg[S]{
		op:     controller.op,
		pos:    pos,
		raw:    data,
		source: controller.source,
		spec:   s,
	}, nil
}

func (controller *readController[S]) read() (pos fstln.Position, data []byte, err error) {
	controller.lock.Lock()
	defer controller.lock.Unlock()

	data = make([]byte, controller.bufferLen)

	var (
		buffer   = data
		dataLen  int
		isPrefix bool
	)

	for {
		pos, _, isPrefix, err = controller.stg.Read(buffer)
		if err != nil && err != io.EOF {
			return pos, nil, err
		} else if pos == fstln.EOF {
			return pos, nil, io.EOF
		}

		if !isPrefix {
			return pos, data[:pos.Len-1], err
		}

		dataLen = len(data)
		data = append(
			data,
			make([]byte, controller.bufferLen)...)
		buffer = data[dataLen:]
	}
}
