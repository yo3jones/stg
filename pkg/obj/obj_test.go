package obj

import (
	"testing"

	"github.com/yo3jones/stg/pkg/jsonl"
)

type TestSpec struct {
	Id  int    `json:"id"`
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

func (spec *TestSpec) GetId() int {
	return spec.Id
}

var (
	FooAccessor = &fooAccessor{}
	BarAccessor = &barAccessor{}
)

var (
	OrderByFoo     = OrderBy[int, *TestSpec, string](FooAccessor)
	OrderByFooDesc = OrderByDesc[int, *TestSpec, string](FooAccessor)
	OrderByBar     = OrderBy[int, *TestSpec, string](BarAccessor)
	OrderByBarDesc = OrderByDesc[int, *TestSpec, string](BarAccessor)
)

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

func FooEquals(v string) Matcher[int, *TestSpec] {
	return Equals[int, *TestSpec, string](FooAccessor, v)
}

func MutateFoo(v string) Mutator[string, int, *TestSpec] {
	return NewMutator[string, int, *TestSpec, string](FooAccessor, v)
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

func BarEquals(v string) Matcher[int, *TestSpec] {
	return Equals[int, *TestSpec, string](BarAccessor, v)
}

func TestAnd(t *testing.T) {
	type test struct {
		name   string
		spec   *TestSpec
		and    Matcher[int, *TestSpec]
		expect bool
	}

	tests := []test{
		{
			name:   "with all match",
			spec:   &TestSpec{1, "foo", "bar"},
			and:    And(FooEquals("foo"), BarEquals("bar")),
			expect: true,
		},
		{
			name:   "with one not matched",
			spec:   &TestSpec{1, "foo", "bar"},
			and:    And(FooEquals("foo"), BarEquals("baz")),
			expect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.and.Match(tc.spec)

			if got != tc.expect {
				t.Errorf("expected and to be %t but got %t", tc.expect, got)
			}
		})
	}
}

func TestOr(t *testing.T) {
	type test struct {
		name   string
		spec   *TestSpec
		or     Matcher[int, *TestSpec]
		expect bool
	}

	tests := []test{
		{
			name:   "with one match",
			spec:   &TestSpec{1, "foo", "bar"},
			or:     Or(FooEquals("fiz"), BarEquals("bar")),
			expect: true,
		},
		{
			name:   "with all not matching",
			spec:   &TestSpec{1, "foo", "bar"},
			or:     Or(FooEquals("fiz"), BarEquals("buz")),
			expect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.or.Match(tc.spec)

			if got != tc.expect {
				t.Errorf("expected or to be %t but got %t", tc.expect, got)
			}
		})
	}
}

func TestOrderBy(t *testing.T) {
	type test struct {
		name   string
		i      *TestSpec
		j      *TestSpec
		lesser Lesser[int, *TestSpec]
		expect int
	}

	tests := []test{
		{
			name:   "with less",
			i:      &TestSpec{1, "fiz", "bar"},
			j:      &TestSpec{1, "foo", "bar"},
			lesser: OrderByFoo,
			expect: -1,
		},
		{
			name:   "with not less",
			i:      &TestSpec{1, "foo", "bar"},
			j:      &TestSpec{1, "fiz", "bar"},
			lesser: OrderByFoo,
			expect: 1,
		},
		{
			name:   "with equal",
			i:      &TestSpec{1, "foo", "bar"},
			j:      &TestSpec{1, "foo", "bar"},
			lesser: OrderByFoo,
			expect: 0,
		},
		{
			name:   "with less desc",
			i:      &TestSpec{1, "fiz", "bar"},
			j:      &TestSpec{1, "foo", "bar"},
			lesser: OrderByFooDesc,
			expect: 1,
		},
		{
			name:   "with not less desc",
			i:      &TestSpec{1, "foo", "bar"},
			j:      &TestSpec{1, "fiz", "bar"},
			lesser: OrderByFooDesc,
			expect: -1,
		},
		{
			name:   "with equal desc",
			i:      &TestSpec{1, "foo", "bar"},
			j:      &TestSpec{1, "foo", "bar"},
			lesser: OrderByFooDesc,
			expect: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.lesser.Less(tc.i, tc.j)

			if got != tc.expect {
				t.Errorf(
					"expected lesser to have returned %d but got %d",
					tc.expect,
					got,
				)
			}
		})
	}
}

func TestMutator(t *testing.T) {
	mutation := &jsonl.JsonlMutation[int]{
		From: map[string]any{},
		To:   map[string]any{},
	}

	mutator := MutateFoo("fiz")

	spec := &TestSpec{
		Foo: "foo",
	}

	mutator.Mutate(spec, mutation)

	if spec.Foo != "fiz" {
		t.Errorf(
			"expected the mutator to change Foo from foo to fiz but got %s",
			spec.Foo,
		)
	}

	if mutation.From["foo"] != "foo" {
		t.Errorf(
			"expected mutation from value for Foo to be foo but got %s",
			mutation.From["foo"],
		)
	}

	if mutation.To["foo"] != "fiz" {
		t.Errorf(
			"expected mutation to value for Foo to be fiz but got %s",
			mutation.To["foo"],
		)
	}
}
