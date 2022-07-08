package obj

import (
	"encoding/json"
	"fmt"
	"time"
)

type TestSpec struct {
	Id        int       `json:"id"`
	Type      string    `json:"type"`
	Foo       string    `json:"foo"`
	Bar       string    `json:"bar"`
	UpdatedAt time.Time `json:"updatedAt"`
	CreatedAt time.Time `json:"createdAt"`
}

func (spec *TestSpec) GetId() int {
	return spec.Id
}

func (spec *TestSpec) String() string {
	var (
		b   []byte
		err error
	)

	if b, err = json.MarshalIndent(spec, "", "  "); err != nil {
		return "woops"
	}

	return string(b)
}

type TestSpecFactory struct{}

func (factory *TestSpecFactory) New() *TestSpec {
	return &TestSpec{}
}

type TestNower struct{}

func (*TestNower) Now() time.Time {
	return GetTestNow()
}

func GetTestNow() time.Time {
	now, err := time.Parse(time.RFC3339, "2022-07-06T16:18:00-04:00")
	if err != nil {
		fmt.Println(err)
	}
	return now
}

var (
	IdAccessor        = &idAccessor{}
	FooAccessor       = &fooAccessor{}
	BarAccessor       = &barAccessor{}
	UpdatedAtAccessor = &updatedAtAccessor{}
	CreatedAtAccessor = &createdAtAccessor{}
)

var (
	OrderById      = OrderBy[*TestSpec, int](IdAccessor)
	OrderByFoo     = OrderBy[*TestSpec, string](FooAccessor)
	OrderByFooDesc = OrderByDesc[*TestSpec, string](FooAccessor)
	OrderByBar     = OrderBy[*TestSpec, string](BarAccessor)
	OrderByBarDesc = OrderByDesc[*TestSpec, string](BarAccessor)
)

type idAccessor struct{}

func (*idAccessor) Get(s *TestSpec) int {
	return s.Id
}

func (*idAccessor) Name() string {
	return "id"
}

func (*idAccessor) Set(s *TestSpec, v int) {
	s.Id = v
}

type updatedAtAccessor struct{}

func (*updatedAtAccessor) Get(s *TestSpec) time.Time {
	return s.UpdatedAt
}

func (*updatedAtAccessor) Name() string {
	return "updatedAt"
}

func (*updatedAtAccessor) Set(s *TestSpec, v time.Time) {
	s.UpdatedAt = v
}

type createdAtAccessor struct{}

func (*createdAtAccessor) Get(s *TestSpec) time.Time {
	return s.CreatedAt
}

func (*createdAtAccessor) Name() string {
	return "createdAt"
}

func (*createdAtAccessor) Set(s *TestSpec, v time.Time) {
	s.CreatedAt = v
}

type fooAccessor struct{}

func (*fooAccessor) Get(s *TestSpec) string {
	return s.Foo
}

func (*fooAccessor) Name() string {
	return "foo"
}

func (*fooAccessor) Set(s *TestSpec, v string) {
	s.Foo = v
}

func FooEquals(v string) Matcher[*TestSpec] {
	return Equals[*TestSpec, string](FooAccessor, v)
}

func MutateFoo(v string) Mutator[*TestSpec] {
	return NewMutator[*TestSpec, string](FooAccessor, v)
}

type barAccessor struct{}

func (*barAccessor) Get(s *TestSpec) string {
	return s.Bar
}

func (*barAccessor) Name() string {
	return "bar"
}

func (*barAccessor) Set(s *TestSpec, v string) {
	s.Bar = v
}

func BarEquals(v string) Matcher[*TestSpec] {
	return Equals[*TestSpec, string](BarAccessor, v)
}

func MutateBar(v string) Mutator[*TestSpec] {
	return NewMutator[*TestSpec, string](BarAccessor, v)
}

type testIdFactory struct {
	value int
}

func (factory *testIdFactory) New() int {
	defer func() { factory.value++ }()
	return factory.value
}

type testMarshalUnmarshaller[S any] struct {
	mockErr          *mockErr
	marshalCallCount int
}

func (marshalUnmarshaller *testMarshalUnmarshaller[S]) Marshal(
	v S,
) ([]byte, error) {
	defer func() { marshalUnmarshaller.marshalCallCount++ }()
	mockErr := marshalUnmarshaller.mockErr
	if mockErr != nil &&
		mockErr.mockErrType == mockErrTypeMarshal &&
		marshalUnmarshaller.marshalCallCount == mockErr.errorOn {
		return nil, fmt.Errorf("%s", mockErr.msg)
	}
	return json.Marshal(v)
}

func (*testMarshalUnmarshaller[S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}
