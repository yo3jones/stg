package obj

import (
	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/stg"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type Storage[T comparable, I comparable, S stg.Spec[I]] interface {
	Delete(filters Matcher[I, S]) (deleted int, err error)
	Insert(mutators Mutator[T, I, S]) (inserted S, err error)
	Select(
		filters Matcher[I, S],
		orderBys ...Lesser[I, S],
	) (results []S, err error)
	Update(
		filters Matcher[I, S],
		mutators Mutator[T, I, S],
		orderBys ...Lesser[I, S],
	) (updated []S, err error)
}

type storage[T comparable, I comparable, S stg.Spec[I]] struct {
	stg          fstln.Storage
	factory      SpecFactory[I, S]
	unmarshaller stg.Unmarshaller[I, S]
	concurrency  int
	bufferLen    int
}

type SpecFactory[I comparable, S stg.Spec[I]] interface {
	New() S
}

type Accessor[I comparable, S stg.Spec[I], T any] interface {
	Get(s S) T
	Name() string
	Set(s S, v T)
}

type Mutator[T comparable, I comparable, S stg.Spec[I]] interface {
	Mutate(s S, mutation stg.Mutation[T, I])
}

type mutator[T comparable, I comparable, S stg.Spec[I], V comparable] struct {
	accessor Accessor[I, S, V]
	value    V
}

func (mutator *mutator[T, I, S, V]) Mutate(s S, mutation stg.Mutation[T, I]) {
	from := mutator.accessor.Get(s)
	mutator.accessor.Set(s, mutator.value)
	mutation.Add(mutator.accessor.Name(), from, mutator.value)
}

func NewMutator[T comparable, I comparable, S stg.Spec[I], V comparable](
	accessor Accessor[I, S, V],
	value V,
) Mutator[T, I, S] {
	return &mutator[T, I, S, V]{accessor, value}
}

type Matcher[I comparable, S stg.Spec[I]] interface {
	Match(s S) bool
}

type noopMatcher[I comparable, S stg.Spec[I]] struct{}

func (matcher *noopMatcher[I, S]) Match(_ S) bool {
	return true
}

func Noop[I comparable, S stg.Spec[I]]() Matcher[I, S] {
	return &noopMatcher[I, S]{}
}

type and[I comparable, S stg.Spec[I]] struct {
	matchers []Matcher[I, S]
}

func (matcher *and[I, S]) Match(s S) bool {
	for _, m := range matcher.matchers {
		if !m.Match(s) {
			return false
		}
	}
	return true
}

func And[I comparable, S stg.Spec[I]](matchers ...Matcher[I, S]) Matcher[I, S] {
	return &and[I, S]{matchers}
}

type or[I comparable, S stg.Spec[I]] struct {
	matchers []Matcher[I, S]
}

func (matcher *or[I, S]) Match(s S) bool {
	for _, m := range matcher.matchers {
		if m.Match(s) {
			return true
		}
	}
	return false
}

func Or[I comparable, S stg.Spec[I]](matchers ...Matcher[I, S]) Matcher[I, S] {
	return &or[I, S]{matchers}
}

type equals[I comparable, S stg.Spec[I], T comparable] struct {
	accessor Accessor[I, S, T]
	value    T
}

func (matcher *equals[I, S, T]) Match(s S) bool {
	return matcher.accessor.Get(s) == matcher.value
}

func Equals[I comparable, S stg.Spec[I], T comparable](
	accessor Accessor[I, S, T],
	value T,
) Matcher[I, S] {
	return &equals[I, S, T]{accessor, value}
}

type Lesser[I comparable, S stg.Spec[I]] interface {
	Less(i, j S) int
}

type orderBy[I comparable, S stg.Spec[I], T constraints.Ordered] struct {
	accessor Accessor[I, S, T]
	desc     bool
}

func (lesser *orderBy[I, S, T]) Less(i, j S) int {
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

func OrderBy[I comparable, S stg.Spec[I], T constraints.Ordered](
	accessor Accessor[I, S, T],
) Lesser[I, S] {
	return &orderBy[I, S, T]{accessor, false}
}

func OrderByDesc[I comparable, S stg.Spec[I], T constraints.Ordered](
	accessor Accessor[I, S, T],
) Lesser[I, S] {
	return &orderBy[I, S, T]{accessor, true}
}

func Sort[I comparable, S stg.Spec[I]](
	specs []S,
	lessers ...Lesser[I, S],
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
