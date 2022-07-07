package obj

import (
	"sync"

	"github.com/yo3jones/stg/pkg/fstln"
)

type writeController[S any] struct {
	concurrency         int
	errCh               chan error
	inCh                chan specMsg[S]
	outCh               chan specMsg[S]
	marshalUnmarshaller MarshalUnmarshaller[S]
	mutators            []Mutator[S]
	source              string
	stg                 fstln.Storage
}

func newWriteController[S any](
	inCh, outCh chan specMsg[S],
	errCh chan error,
	mutators []Mutator[S],
	stg fstln.Storage,
	marshalUnmarshaller MarshalUnmarshaller[S],
	opts ...writeControllerOpt,
) *writeController[S] {
	controller := &writeController[S]{
		concurrency:         10,
		inCh:                inCh,
		outCh:               outCh,
		errCh:               errCh,
		marshalUnmarshaller: marshalUnmarshaller,
		mutators:            mutators,
		source:              "",
		stg:                 stg,
	}

	for _, opt := range opts {
		opt.isWriteControllerOpt()
		switch opt := opt.(type) {
		case optConcurrency:
			controller.concurrency = opt.value
		case optSource:
			controller.source = opt.value
		}
	}

	return controller
}

type writeControllerOpt interface {
	isWriteControllerOpt() bool
}

func (opt optConcurrency) isWriteControllerOpt() bool {
	return true
}

func (opt optSource) isWriteControllerOpt() bool {
	return true
}

func (controller *writeController[S]) Start() {
	waitGroup := sync.WaitGroup{}

	for i := 0; i < controller.concurrency; i++ {
		proc := &writeProcess[S]{
			errCh:               controller.errCh,
			inCh:                controller.inCh,
			outCh:               controller.outCh,
			marshalUnmarshaller: controller.marshalUnmarshaller,
			stg:                 controller.stg,
		}

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			proc.Start()
		}()
	}

	waitGroup.Wait()

	controller.outCh <- specMsg[S]{
		op:     opDone,
		source: controller.source,
	}
}

type writeProcess[S any] struct {
	errCh               chan error
	inCh                chan specMsg[S]
	outCh               chan specMsg[S]
	marshalUnmarshaller MarshalUnmarshaller[S]
	stg                 fstln.Storage
}

func (proc *writeProcess[S]) Start() {
	for {
		if done := proc.processMsg(); done {
			break
		}
	}
}

func (proc *writeProcess[S]) processMsg() (done bool) {
	var (
		ok  bool
		msg specMsg[S]
	)

	if msg, ok = <-proc.inCh; !ok {
		return true
	}

	switch msg.op {
	case opDone:
		close(proc.inCh)
		return true
	case opDelete:
		proc.processDeleteMsg(msg)
	}

	return false
}

func (proc *writeProcess[S]) processDeleteMsg(msg specMsg[S]) {
	if err := proc.stg.Delete(msg.pos); err != nil {
		proc.errCh <- err
		return
	}

	proc.outCh <- msg
}
