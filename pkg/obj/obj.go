package obj

import (
	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/stg"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type Storage[T comparable, S any] interface {
	Select(
		filters Matcher[S],
		orderBys []Lesser[S],
	) (results []S, err error)
	NewSelectBuilder() SelectBuilder[S]
	// Delete(filters Matcher[S]) (deleted int, err error)
	// Insert(mutators Mutator[T, S]) (inserted S, err error)
	// Update(
	// 	filters Matcher[S],
	// 	mutators Mutator[T, S],
	// 	orderBys ...Lesser[S],
	// ) (updated []S, err error)
}

type storage[I comparable, T comparable, S any] struct {
	bufferLen    int
	concurrency  int
	factory      SpecFactory[S]
	idAccessor   Accessor[S, I]
	idFactory    IdFactory[I]
	stg          fstln.Storage
	unmarshaller stg.Unmarshaller[S]
}

type SpecFactory[S any] interface {
	New() S
}

type IdFactory[I comparable] interface {
	New() I
}

type Accessor[S any, T any] interface {
	Get(s S) T
	Name() string
	Set(s S, v T)
}

type Mutator[T comparable, S any] interface {
	Mutate(s S, mutation stg.Mutation[T])
}

type mutator[T comparable, S any, V comparable] struct {
	accessor Accessor[S, V]
	value    V
}

func (mutator *mutator[T, S, V]) Mutate(s S, mutation stg.Mutation[T]) {
	from := mutator.accessor.Get(s)
	mutator.accessor.Set(s, mutator.value)
	mutation.Add(mutator.accessor.Name(), from, mutator.value)
}

func NewMutator[T comparable, S any, V comparable](
	accessor Accessor[S, V],
	value V,
) Mutator[T, S] {
	return &mutator[T, S, V]{accessor, value}
}

type Matcher[S any] interface {
	Match(s S) bool
}

type noopMatcher[S any] struct{}

func (matcher *noopMatcher[S]) Match(_ S) bool {
	return true
}

func Noop[S any]() Matcher[S] {
	return &noopMatcher[S]{}
}

type and[S any] struct {
	matchers []Matcher[S]
}

func (matcher *and[S]) Match(s S) bool {
	for _, m := range matcher.matchers {
		if !m.Match(s) {
			return false
		}
	}
	return true
}

func And[S any](matchers ...Matcher[S]) Matcher[S] {
	return &and[S]{matchers}
}

type or[S any] struct {
	matchers []Matcher[S]
}

func (matcher *or[S]) Match(s S) bool {
	for _, m := range matcher.matchers {
		if m.Match(s) {
			return true
		}
	}
	return false
}

func Or[S any](matchers ...Matcher[S]) Matcher[S] {
	return &or[S]{matchers}
}

type equals[S any, T comparable] struct {
	accessor Accessor[S, T]
	value    T
}

func (matcher *equals[S, T]) Match(s S) bool {
	return matcher.accessor.Get(s) == matcher.value
}

func Equals[S any, T comparable](
	accessor Accessor[S, T],
	value T,
) Matcher[S] {
	return &equals[S, T]{accessor, value}
}

type Lesser[S any] interface {
	Less(i, j S) int
}

type orderBy[S any, T constraints.Ordered] struct {
	accessor Accessor[S, T]
	desc     bool
}

func (lesser *orderBy[S, T]) Less(i, j S) int {
	iVal := lesser.accessor.Get(i)
	jVal := lesser.accessor.Get(j)

	var result int

	if iVal < jVal {
		result = -1
	} else if iVal > jVal {
		result = 1
	} else {
		result = 0
	}

	if lesser.desc {
		result *= -1
	}

	return result
}

func OrderBy[S any, T constraints.Ordered](
	accessor Accessor[S, T],
) Lesser[S] {
	return &orderBy[S, T]{accessor, false}
}

func OrderByDesc[S any, T constraints.Ordered](
	accessor Accessor[S, T],
) Lesser[S] {
	return &orderBy[S, T]{accessor, true}
}

func Sort[S any](
	specs []S,
	lessers ...Lesser[S],
) {
	slices.SortFunc(specs, func(a, b S) bool {
		for _, lesser := range lessers {
			var res int
			if res = lesser.Less(a, b); res < 0 {
				return true
			} else if res > 0 {
				return false
			}
		}
		return false
	})
}
