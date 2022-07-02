package obj

import (
	"github.com/yo3jones/stg/pkg/stg"
	"golang.org/x/exp/constraints"
)

type Storage[T comparable, I comparable, S Spec[I]] interface {
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

type Spec[I comparable] interface {
	Ider[I]
}

type Ider[I comparable] interface {
	GetId() I
}

type Accessor[I comparable, S Spec[I], T any] interface {
	Get(s S) T
	Name() string
	Set(s S, v T)
}

type Mutator[T comparable, I comparable, S Spec[I]] interface {
	Mutate(s S, mutation stg.Mutation[T, I])
}

type mutator[T comparable, I comparable, S Spec[I], V comparable] struct {
	accessor Accessor[I, S, V]
	value    V
}

func (mutator *mutator[T, I, S, V]) Mutate(s S, mutation stg.Mutation[T, I]) {
	from := mutator.accessor.Get(s)
	mutator.accessor.Set(s, mutator.value)
	mutation.Add(mutator.accessor.Name(), from, mutator.value)
}

func NewMutator[T comparable, I comparable, S Spec[I], V comparable](
	accessor Accessor[I, S, V],
	value V,
) Mutator[T, I, S] {
	return &mutator[T, I, S, V]{accessor, value}
}

type Matcher[I comparable, S Spec[I]] interface {
	Match(s S) bool
}

type and[I comparable, S Spec[I]] struct {
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

func And[I comparable, S Spec[I]](matchers ...Matcher[I, S]) Matcher[I, S] {
	return &and[I, S]{matchers}
}

type or[I comparable, S Spec[I]] struct {
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

func Or[I comparable, S Spec[I]](matchers ...Matcher[I, S]) Matcher[I, S] {
	return &or[I, S]{matchers}
}

type equals[I comparable, S Spec[I], T comparable] struct {
	accessor Accessor[I, S, T]
	value    T
}

func (matcher *equals[I, S, T]) Match(s S) bool {
	return matcher.accessor.Get(s) == matcher.value
}

func Equals[I comparable, S Spec[I], T comparable](
	accessor Accessor[I, S, T],
	value T,
) Matcher[I, S] {
	return &equals[I, S, T]{accessor, value}
}

type Lesser[I comparable, S Spec[I]] interface {
	Less(i, j S) int
}

type orderBy[I comparable, S Spec[I], T constraints.Ordered] struct {
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

func OrderBy[I comparable, S Spec[I], T constraints.Ordered](
	accessor Accessor[I, S, T],
) Lesser[I, S] {
	return &orderBy[I, S, T]{accessor, false}
}

func OrderByDesc[I comparable, S Spec[I], T constraints.Ordered](
	accessor Accessor[I, S, T],
) Lesser[I, S] {
	return &orderBy[I, S, T]{accessor, true}
}
