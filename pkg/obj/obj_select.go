package obj

func (stg *storage[T, S]) Select(
	filters Matcher[S],
	orderBys ...Lesser[S],
) (results []S, err error) {
	var (
		ch    = make(chan specMsg[S], stg.concurrency)
		errCh = make(chan error, stg.concurrency)
	)

	controller := newReadController(
		ch,
		errCh,
		stg.factory,
		filters,
		stg.stg,
		stg.unmarshaller,
		optBufferLen{stg.bufferLen},
		optConcurrency{stg.concurrency},
	)

	go controller.Start()

	if results, err = stg.gatherResults(ch, errCh, orderBys); err != nil {
		return nil, err
	}

	return results, nil
}

func (stg *storage[T, S]) gatherResults(
	ch chan specMsg[S],
	errCh chan error,
	orderBys []Lesser[S],
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

	Sort(results, orderBys...)

	return results, nil
}

func (*storage[T, S]) gatherResult(
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
