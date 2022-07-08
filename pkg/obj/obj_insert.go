package obj

func (stg *storage[I, S]) Insert(
	mutators []Mutator[S],
) (inserted S, err error) {
	stg.lock.Lock()
	defer stg.lock.Unlock()
	var (
		data  []byte
		trans = stg.binLogStg.StartTransaction(stg.objType)
	)
	defer trans.End()

	inserted = stg.factory.New()

	now := stg.nower.Now()
	stg.idAccessor.Set(inserted, stg.idFactory.New())
	stg.updatedAtAccessor.Set(inserted, now)
	stg.createdAtAccessor.Set(inserted, now)

	for _, mutator := range mutators {
		mutator.Mutate(inserted)
	}

	if data, err = stg.marshalUnmarshaller.Marshal(inserted); err != nil {
		return inserted, err
	}

	if err = trans.LogInsert(stg.idAccessor.Get(inserted), data); err != nil {
		return inserted, err
	}

	if _, err = stg.stg.Insert(data); err != nil {
		return inserted, err
	}

	// TODO handle mutation log

	return inserted, nil
}

func (stg *storage[I, S]) NewInsertBuilder() InsertBuilder[S] {
	return &insertBuilder[S]{stg: stg}
}

type InsertBuilder[S any] interface {
	Set(mutators ...Mutator[S]) InsertBuilder[S]
	Run() (inserted S, err error)
}

type insertBuilder[S any] struct {
	mutators []Mutator[S]
	stg      Storage[S]
}

func (builder *insertBuilder[S]) Set(mutators ...Mutator[S]) InsertBuilder[S] {
	builder.mutators = mutators
	return builder
}

func (builder *insertBuilder[S]) Run() (inserted S, err error) {
	return builder.stg.Insert(builder.mutators)
}
