package obj

func (stg *storage[I, S]) Update(
	filters Matcher[S],
	mutators []Mutator[S],
	orderBys []Lesser[S],
) (updated []S, err error) {
	return stg.runReadWrite(opUpdate, filters, mutators, orderBys...)
}

func (stg *storage[I, S]) NewUpdateBuilder() UpdateBuilder[S] {
	return &updateBuilder[S]{
		stg: stg,
	}
}

type UpdateBuilder[S any] interface {
	OrderBy(orderBys ...Lesser[S]) UpdateBuilder[S]
	Run() (updated []S, err error)
	Set(mutators ...Mutator[S]) UpdateBuilder[S]
	Where(filters ...Matcher[S]) UpdateBuilder[S]
}

type updateBuilder[S any] struct {
	filters  Matcher[S]
	orderBys []Lesser[S]
	mutators []Mutator[S]
	stg      Storage[S]
}

func (builder *updateBuilder[S]) OrderBy(
	orderBys ...Lesser[S],
) UpdateBuilder[S] {
	builder.orderBys = orderBys
	return builder
}

func (builder *updateBuilder[S]) Run() (updated []S, err error) {
	return builder.stg.Update(
		builder.filters,
		builder.mutators,
		builder.orderBys,
	)
}

func (builder *updateBuilder[S]) Set(mutators ...Mutator[S]) UpdateBuilder[S] {
	builder.mutators = mutators
	return builder
}

func (builder *updateBuilder[S]) Where(filters ...Matcher[S]) UpdateBuilder[S] {
	builder.filters = And(filters...)
	return builder
}
