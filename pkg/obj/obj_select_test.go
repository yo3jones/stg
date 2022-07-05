package obj

import (
	"testing"
)

func TestSelect(t *testing.T) {
	type test struct {
		name        string
		lines       []string
		filters     Matcher[*TestSpec]
		orderBys    []Lesser[*TestSpec]
		bufferLen   int
		mockError   *mockErr
		expectError string
		expect      []*TestSpec
	}

	tests := []test{
		{
			name: "without filters",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			filters: Noop[*TestSpec](),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			expect: []*TestSpec{
				{Id: 2, Foo: "fiz", Bar: "buz"},
				{Id: 1, Foo: "foo", Bar: "bar"},
			},
		},
		{
			name: "with filters",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"foo","bar":"buz"}`,
				`{"id":3,"foo":"fiz","bar":"bar"}`,
				`{"id":4,"foo":"fiz","bar":"buz"}`,
			},
			filters: BarEquals("bar"),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			expect: []*TestSpec{
				{Id: 3, Foo: "fiz", Bar: "bar"},
				{Id: 1, Foo: "foo", Bar: "bar"},
			},
		},
		{
			name: "with small buffer len",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			filters: Noop[*TestSpec](),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			bufferLen: 2,
			expect: []*TestSpec{
				{Id: 2, Foo: "fiz", Bar: "buz"},
				{Id: 1, Foo: "foo", Bar: "bar"},
			},
		},
		{
			name: "with reset scan error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			filters: Noop[*TestSpec](),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			mockError: &mockErr{
				mockErrType: mockErrTypeResetScan,
				errorOn:     0,
				msg:         "with reset scan error",
			},
			expectError: "with reset scan error",
		},
		{
			name: "with read error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			filters: Noop[*TestSpec](),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			mockError: &mockErr{
				mockErrType: mockErrTypeRead,
				errorOn:     0,
				msg:         "with read error",
			},
			expectError: "with read error",
		},
		{
			name: "with unmarshal error",
			lines: []string{
				`{"id":1,"foo":"foo",bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			filters: Noop[*TestSpec](),
			orderBys: []Lesser[*TestSpec]{
				OrderByFoo,
			},
			expectError: "invalid character 'b' looking for beginning of object key string",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			util := &testUtil{
				test:        t,
				lines:       tc.lines,
				filters:     tc.filters,
				orderBys:    tc.orderBys,
				bufferLen:   tc.bufferLen,
				mockError:   tc.mockError,
				expectError: tc.expectError,
				expect:      tc.expect,
			}

			err = util.setup()
			defer util.teardown()

			if err != nil {
				t.Fatal(err)
			}

			util.expectSelect()
		})
	}
}
