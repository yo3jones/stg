package obj

import (
	"sync"
	"time"

	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/objbinlog"
	"github.com/yo3jones/stg/pkg/stg"
)

type writeController[I comparable, S any] struct {
	binLogTrans         objbinlog.Transaction
	concurrency         int
	errCh               chan error
	idAccessor          Accessor[S, I]
	inCh                chan specMsg[S]
	outCh               chan specMsg[S]
	marshalUnmarshaller stg.MarshalUnmarshaller[S]
	mutators            []Mutator[S]
	now                 time.Time
	source              string
	stg                 fstln.Storage
	updatedAtAccessor   Accessor[S, time.Time]
}

func newWriteController[I comparable, S any](
	inCh, outCh chan specMsg[S],
	errCh chan error,
	mutators []Mutator[S],
	stg fstln.Storage,
	binLogTrans objbinlog.Transaction,
	marshalUnmarshaller stg.MarshalUnmarshaller[S],
	idAccessor Accessor[S, I],
	updatedAtAccessor Accessor[S, time.Time],
	now time.Time,
	opts ...writeControllerOpt,
) *writeController[I, S] {
	controller := &writeController[I, S]{
		binLogTrans:         binLogTrans,
		concurrency:         10,
		idAccessor:          idAccessor,
		inCh:                inCh,
		outCh:               outCh,
		errCh:               errCh,
		marshalUnmarshaller: marshalUnmarshaller,
		mutators:            mutators,
		now:                 now,
		source:              "",
		stg:                 stg,
		updatedAtAccessor:   updatedAtAccessor,
	}

	for _, opt := range opts {
		opt.isWriteControllerOpt()
		switch opt := opt.(type) {
		case optConcurrency:
			controller.concurrency = opt.value
			// case optSource:
			// 	controller.source = opt.value
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

// func (opt optSource) isWriteControllerOpt() bool {
// 	return true
// }

func (controller *writeController[I, S]) Start() {
	waitGroup := sync.WaitGroup{}

	for i := 0; i < controller.concurrency; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			controller.startProc()
		}()
	}

	waitGroup.Wait()

	controller.outCh <- specMsg[S]{
		op:     opDone,
		source: controller.source,
	}
}

func (controller *writeController[I, S]) startProc() {
	for {
		if done := controller.processMsg(); done {
			break
		}
	}
}

func (controller *writeController[I, S]) processMsg() (done bool) {
	var (
		ok  bool
		msg specMsg[S]
	)

	if msg, ok = <-controller.inCh; !ok {
		return true
	}

	switch msg.op {
	case opDone:
		close(controller.inCh)
		return true
	case opDelete:
		controller.processDeleteMsg(msg)
	case opUpdate:
		controller.processUpdateMsg(msg)
	}

	return false
}

func (controller *writeController[I, S]) processDeleteMsg(msg specMsg[S]) {
	var err error

	err = controller.binLogTrans.LogDelete(
		controller.idAccessor.Get(msg.spec),
		msg.raw,
	)
	if err != nil {
		controller.errCh <- err
		return
	}

	if err = controller.stg.Delete(msg.pos); err != nil {
		controller.errCh <- err
		return
	}

	controller.outCh <- msg
}

func (controller *writeController[I, S]) processUpdateMsg(msg specMsg[S]) {
	var (
		afterPos fstln.Position
		data     []byte
		err      error
		mutators = make([]Mutator[S], 0, len(controller.mutators)+1)
	)

	mutators = append(
		mutators,
		NewMutator(controller.updatedAtAccessor, controller.now),
	)
	mutators = append(mutators, controller.mutators...)

	for _, mutator := range mutators {
		mutator.Mutate(msg.spec)
	}

	if data, err = controller.marshalUnmarshaller.Marshal(msg.spec); err != nil {
		controller.errCh <- err
		return
	}

	err = controller.binLogTrans.LogUpdate(
		controller.idAccessor.Get(msg.spec),
		msg.raw,
		data,
	)
	if err != nil {
		controller.errCh <- err
		return
	}

	if afterPos, err = controller.stg.Update(msg.pos, data); err != nil {
		controller.errCh <- err
		return
	}

	controller.outCh <- specMsg[S]{
		op:     msg.op,
		pos:    afterPos,
		source: msg.source,
		spec:   msg.spec,
	}
}
