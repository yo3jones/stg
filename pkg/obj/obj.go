package obj

import (
	"time"

	"github.com/yo3jones/stg/pkg/fstln"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type Storage[S any] interface {
	Delete(filters Matcher[S]) (deleted []S, err error)
	Insert(mutators []Mutator[S]) (inserted S, err error)
	Select(
		filters Matcher[S],
		orderBys []Lesser[S],
	) (results []S, err error)
	NewDeleteBuilder() DeleteBuilder[S]
	NewInsertBuilder() InsertBuilder[S]
	NewSelectBuilder() SelectBuilder[S]
	Update(
		filters Matcher[S],
		mutators []Mutator[S],
		orderBys []Lesser[S],
	) (updated []S, err error)
}

type storage[I comparable, T comparable, S any] struct {
	bufferLen           int
	concurrency         int
	createdAtAccessor   Accessor[S, time.Time]
	factory             SpecFactory[S]
	idAccessor          Accessor[S, I]
	idFactory           IdFactory[I]
	nower               Nower
	stg                 fstln.Storage
	marshalUnmarshaller MarshalUnmarshaller[S]
	updatedAtAccessor   Accessor[S, time.Time]
}

type MarshalUnmarshaller[S any] interface {
	Marshaller[S]
	MutationMarshaller
	MutationUnmarshaller
	Unmarshaller[S]
}

type Marshaller[S any] interface {
	Marshal(v S) ([]byte, error)
}

type Unmarshaller[S any] interface {
	Unmarshal(data []byte, v S) error
}

type MutationMarshaller interface {
	MarshalMutation(mutation *Mutation) ([]byte, error)
}

type MutationUnmarshaller interface {
	UnmarshalMutation(data []byte, v *Mutation) error
}

type MutationAdder interface {
	Add(field string, from, to any)
}

type Mutation struct {
	TransactionId string         `json:"transactionId"`
	Timestamp     time.Time      `json:"timestamp"`
	Type          string         `json:"type"`
	Id            any            `json:"id"`
	Partition     string         `json:"partition"`
	From          map[string]any `json:"from"`
	To            map[string]any `json:"to"`
}

func newMutation() *Mutation {
	return &Mutation{
		From: map[string]any{},
		To:   map[string]any{},
	}
}

func (mutation *Mutation) Add(field string, from, to any) {
	if _, exists := mutation.From[field]; !exists {
		mutation.From[field] = from
	}
	mutation.To[field] = to

	// TODO check if the from and to values are equal, if so then remove
	// this may be tough for non comparable types, might be able to do specific
	// logic for set, slice and map?
	//
	// also need to worry about type safety as we don't have compile time
	// checking that the from and to types match
}

type Nower interface {
	Now() time.Time
}

type nower struct{}

func (*nower) Now() time.Time {
	return time.Now()
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

type Mutator[S any] interface {
	Mutate(s S, mutation MutationAdder)
}

type mutator[S any, V comparable] struct {
	accessor Accessor[S, V]
	value    V
}

func (mutator *mutator[S, V]) Mutate(s S, mutation MutationAdder) {
	from := mutator.accessor.Get(s)
	mutator.accessor.Set(s, mutator.value)
	mutation.Add(mutator.accessor.Name(), from, mutator.value)
}

func NewMutator[S any, V comparable](
	accessor Accessor[S, V],
	value V,
) Mutator[S] {
	return &mutator[S, V]{accessor, value}
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

type optBufferLen struct {
	value int
}

type optConcurrency struct {
	value int
}
