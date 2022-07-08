package obj

func (stg *storage[I, S]) Delete(
	filters Matcher[S],
) (deleted []S, err error) {
	return stg.runReadWrite(opDelete, filters, []Mutator[S]{})
}

func (stg *storage[I, S]) NewDeleteBuilder() DeleteBuilder[S] {
	return &deleteBuilder[S]{
		stg: stg,
	}
}

type DeleteBuilder[S any] interface {
	Where(filters ...Matcher[S]) DeleteBuilder[S]
	Run() (deleted []S, err error)
}

type deleteBuilder[S any] struct {
	stg     Storage[S]
	filters Matcher[S]
}

func (builder *deleteBuilder[S]) Where(filters ...Matcher[S]) DeleteBuilder[S] {
	builder.filters = And(filters...)
	return builder
}

func (builder *deleteBuilder[S]) Run() (deleted []S, err error) {
	return builder.stg.Delete(builder.filters)
}
