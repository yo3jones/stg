package obj

func (stg *storage[I, T, S]) Select(
	filters Matcher[S],
	orderBys []Lesser[S],
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
		stg.marshalUnmarshaller,
		optBufferLen{stg.bufferLen},
		optConcurrency{stg.concurrency},
	)

	go controller.Start()

	if results, err = stg.gatherResults(ch, errCh, orderBys); err != nil {
		return nil, err
	}

	return results, nil
}

func (stg *storage[I, T, S]) gatherResults(
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

func (*storage[I, T, S]) gatherResult(
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

func (stg *storage[I, T, S]) NewSelectBuilder() SelectBuilder[S] {
	return &selectBuilder[T, S]{stg: stg}
}

type SelectBuilder[S any] interface {
	Where(filters ...Matcher[S]) SelectBuilder[S]
	OrderBy(orderBys ...Lesser[S]) SelectBuilder[S]
	Run() (results []S, err error)
}

type selectBuilder[T comparable, S any] struct {
	where    Matcher[S]
	orderBys []Lesser[S]
	stg      Storage[S]
}

func (builder *selectBuilder[T, S]) Where(
	filters ...Matcher[S],
) SelectBuilder[S] {
	builder.where = And(filters...)
	return builder
}

func (builder *selectBuilder[T, S]) OrderBy(
	orderBys ...Lesser[S],
) SelectBuilder[S] {
	builder.orderBys = orderBys
	return builder
}

func (builder *selectBuilder[T, S]) Run() (results []S, err error) {
	return builder.stg.Select(builder.where, builder.orderBys)
}
