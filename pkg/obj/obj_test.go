package obj

import (
	"reflect"
	"testing"
)

func TestAnd(t *testing.T) {
	type test struct {
		name   string
		spec   *TestSpec
		and    Matcher[*TestSpec]
		expect bool
	}

	tests := []test{
		{
			name:   "with all match",
			spec:   &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			and:    And(FooEquals("foo"), BarEquals("bar")),
			expect: true,
		},
		{
			name:   "with one not matched",
			spec:   &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
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
		or     Matcher[*TestSpec]
		expect bool
	}

	tests := []test{
		{
			name:   "with one match",
			spec:   &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			or:     Or(FooEquals("fiz"), BarEquals("bar")),
			expect: true,
		},
		{
			name:   "with all not matching",
			spec:   &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
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
		lesser Lesser[*TestSpec]
		expect int
	}

	tests := []test{
		{
			name:   "with less",
			i:      &TestSpec{Id: 1, Foo: "fiz", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			lesser: OrderByFoo,
			expect: -1,
		},
		{
			name:   "with not less",
			i:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "fiz", Bar: "bar"},
			lesser: OrderByFoo,
			expect: 1,
		},
		{
			name:   "with equal",
			i:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			lesser: OrderByFoo,
			expect: 0,
		},
		{
			name:   "with less desc",
			i:      &TestSpec{Id: 1, Foo: "fiz", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			lesser: OrderByFooDesc,
			expect: 1,
		},
		{
			name:   "with not less desc",
			i:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "fiz", Bar: "bar"},
			lesser: OrderByFooDesc,
			expect: -1,
		},
		{
			name:   "with equal desc",
			i:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
			j:      &TestSpec{Id: 1, Foo: "foo", Bar: "bar"},
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
	mutator := MutateFoo("fiz")

	spec := &TestSpec{
		Foo: "foo",
	}

	mutator.Mutate(spec)

	if spec.Foo != "fiz" {
		t.Errorf(
			"expected the mutator to change Foo from foo to fiz but got %s",
			spec.Foo,
		)
	}
}

func TestSort(t *testing.T) {
	type test struct {
		name    string
		specs   []*TestSpec
		lessers []Lesser[*TestSpec]
		expect  []*TestSpec
	}

	tests := []test{
		{
			name: "with less",
			specs: []*TestSpec{
				{Id: 1, Foo: "a", Bar: "b"},
				{Id: 2, Foo: "a", Bar: "a"},
			},
			lessers: []Lesser[*TestSpec]{
				OrderByFoo,
				OrderByBar,
			},
			expect: []*TestSpec{
				{Id: 2, Foo: "a", Bar: "a"},
				{Id: 1, Foo: "a", Bar: "b"},
			},
		},
		{
			name: "without less",
			specs: []*TestSpec{
				{Id: 1, Foo: "a", Bar: "a"},
				{Id: 2, Foo: "a", Bar: "b"},
			},
			lessers: []Lesser[*TestSpec]{
				OrderByFoo,
				OrderByBar,
			},
			expect: []*TestSpec{
				{Id: 1, Foo: "a", Bar: "a"},
				{Id: 2, Foo: "a", Bar: "b"},
			},
		},
		{
			name: "with equal",
			specs: []*TestSpec{
				{Id: 1, Foo: "a", Bar: "a"},
				{Id: 2, Foo: "a", Bar: "a"},
			},
			lessers: []Lesser[*TestSpec]{
				OrderByFoo,
				OrderByBar,
			},
			expect: []*TestSpec{
				{Id: 1, Foo: "a", Bar: "a"},
				{Id: 2, Foo: "a", Bar: "a"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			Sort(tc.specs, tc.lessers...)

			if !reflect.DeepEqual(tc.specs, tc.expect) {
				t.Errorf(
					"expected \n%s\n to be sorted to \n%s\n",
					testSpecSliceString(tc.specs),
					testSpecSliceString(tc.expect),
				)
			}
		})
	}
}
