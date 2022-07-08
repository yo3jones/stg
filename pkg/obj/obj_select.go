package obj

func (stg *storage[I, S]) Select(
	filters Matcher[S],
	orderBys []Lesser[S],
) (results []S, err error) {
	stg.lock.Lock()
	defer stg.lock.Unlock()
	var (
		ch    = make(chan specMsg[S], stg.concurrency)
		errCh = make(chan error, stg.concurrency)
	)

	controller := stg.newReadController(ch, errCh, filters, opNoop)

	go controller.Start()

	if results, err = stg.gatherResults(ch, errCh, orderBys...); err != nil {
		return nil, err
	}

	return results, nil
}

func (stg *storage[I, S]) NewSelectBuilder() SelectBuilder[S] {
	return &selectBuilder[S]{stg: stg}
}

type SelectBuilder[S any] interface {
	Where(filters ...Matcher[S]) SelectBuilder[S]
	OrderBy(orderBys ...Lesser[S]) SelectBuilder[S]
	Run() (results []S, err error)
}

type selectBuilder[S any] struct {
	where    Matcher[S]
	orderBys []Lesser[S]
	stg      Storage[S]
}

func (builder *selectBuilder[S]) Where(
	filters ...Matcher[S],
) SelectBuilder[S] {
	builder.where = And(filters...)
	return builder
}

func (builder *selectBuilder[S]) OrderBy(
	orderBys ...Lesser[S],
) SelectBuilder[S] {
	builder.orderBys = orderBys
	return builder
}

func (builder *selectBuilder[S]) Run() (results []S, err error) {
	return builder.stg.Select(builder.where, builder.orderBys)
}
