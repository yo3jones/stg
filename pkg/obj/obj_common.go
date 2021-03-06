package obj

import (
	"time"

	"github.com/yo3jones/stg/pkg/objbinlog"
)

func (stg *storage[I, S]) newReadController(
	ch chan specMsg[S],
	errCh chan error,
	filters Matcher[S],
	op op,
) *readController[S] {
	return newReadController(
		ch,
		errCh,
		stg.factory,
		filters,
		stg.stg,
		stg.marshalUnmarshaller,
		optBufferLen{stg.bufferLen},
		optConcurrency{stg.concurrency},
		optOp{op},
	)
}

func (stg *storage[I, S]) newWriteController(
	inCh chan specMsg[S],
	outCh chan specMsg[S],
	errCh chan error,
	binLogTrans objbinlog.Transaction,
	mutators []Mutator[S],
	now time.Time,
) *writeController[I, S] {
	return newWriteController(
		inCh,
		outCh,
		errCh,
		mutators,
		stg.stg,
		binLogTrans,
		stg.marshalUnmarshaller,
		stg.idAccessor,
		stg.updatedAtAccessor,
		now,
		optConcurrency{stg.concurrency},
	)
}

func (stg *storage[I, S]) runReadWrite(
	op op,
	filters Matcher[S],
	mutators []Mutator[S],
	orderBys ...Lesser[S],
) (result []S, err error) {
	var (
		binLogTrans objbinlog.Transaction
		inCh        = make(chan specMsg[S], stg.concurrency)
		outCh       = make(chan specMsg[S], stg.concurrency)
		errCh       = make(chan error, stg.concurrency)
		now         = stg.nower.Now()
	)

	binLogTrans = stg.binLogStg.StartTransaction(stg.objType)
	defer binLogTrans.End()

	readController := stg.newReadController(inCh, errCh, filters, op)
	writeController := stg.newWriteController(
		inCh,
		outCh,
		errCh,
		binLogTrans,
		mutators,
		now,
	)

	go readController.Start()
	go writeController.Start()

	if result, err = stg.gatherResults(outCh, errCh, orderBys...); err != nil {
		return nil, err
	}

	return result, nil
}

func (stg *storage[I, S]) gatherResults(
	ch chan specMsg[S],
	errCh chan error,
	orderBys ...Lesser[S],
) (results []S, err error) {
	var (
		done bool
		s    S
	)
	results = make([]S, 0, 100)

	for {
		if s, done, err = stg.gatherResult(ch, errCh); err != nil {
			return nil, err
		} else if done {
			break
		} else {
			results = append(results, s)
		}
	}

	if len(orderBys) > 0 {
		Sort(results, orderBys...)
	}

	return results, nil
}

func (*storage[I, S]) gatherResult(
	ch chan specMsg[S],
	errCh chan error,
) (s S, done bool, err error) {
	select {
	case msg := <-ch:
		if msg.op == opDone {
			return s, true, nil
		}
		return msg.spec, false, nil
	case err := <-errCh:
		return s, false, err
	}
}
