package obj

import "testing"

func TestUpdate(t *testing.T) {
	type test struct {
		name         string
		lines        []string
		filters      Matcher[*TestSpec]
		mutators     []Mutator[*TestSpec]
		orderBys     []Lesser[*TestSpec]
		mockErr      *mockErr
		expectError  string
		expect       []*TestSpec
		expectLines  [][]string
		expectBinLog [][]string
	}

	tests := []test{
		{
			name: "with success",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mutators: []Mutator[*TestSpec]{
				MutateFoo("FOO"),
			},
			orderBys: []Lesser[*TestSpec]{
				OrderById,
			},
			expect: []*TestSpec{
				{Id: 1, Foo: "FOO", Bar: "bar", UpdatedAt: GetTestNow()},
				{Id: 2, Foo: "FOO", Bar: "bar", UpdatedAt: GetTestNow()},
			},
			expectLines: [][]string{
				{
					`                                `,
					`                                `,
					`{"id":3,"foo":"fam","bar":"baz"}`,
					`{"id":1,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}`,
					`{"id":2,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}`,
				},
				{
					`                                `,
					`                                `,
					`{"id":3,"foo":"fam","bar":"baz"}`,
					`{"id":2,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}`,
					`{"id":1,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}`,
				},
			},
			expectBinLog: [][]string{
				{
					`{"transaction":200,"type":"test","id":1,"ts":"2022-07-06T16:18:00-04:00","from":{"id":1,"foo":"foo","bar":"bar"},"to":{"id":1,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}}`,
					`{"transaction":200,"type":"test","id":2,"ts":"2022-07-06T16:18:00-04:00","from":{"id":2,"foo":"fiz","bar":"bar"},"to":{"id":2,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}}`,
				},
				{
					`{"transaction":200,"type":"test","id":2,"ts":"2022-07-06T16:18:00-04:00","from":{"id":2,"foo":"fiz","bar":"bar"},"to":{"id":2,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}}`,
					`{"transaction":200,"type":"test","id":1,"ts":"2022-07-06T16:18:00-04:00","from":{"id":1,"foo":"foo","bar":"bar"},"to":{"id":1,"type":"","foo":"FOO","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"0001-01-01T00:00:00Z"}}`,
				},
			},
		},
		{
			name: "with marshal error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mutators: []Mutator[*TestSpec]{
				MutateFoo("FOO"),
			},
			orderBys: []Lesser[*TestSpec]{
				OrderById,
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeMarshal,
				errorOn:     0,
				msg:         "with marshal error",
			},
			expectError: "with marshal error",
		},
		{
			name: "with update error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mutators: []Mutator[*TestSpec]{
				MutateFoo("FOO"),
			},
			orderBys: []Lesser[*TestSpec]{
				OrderById,
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeUpdate,
				errorOn:     0,
				msg:         "with update error",
			},
			expectError: "with update error",
		},
		{
			name: "with log update error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mutators: []Mutator[*TestSpec]{
				MutateFoo("FOO"),
			},
			orderBys: []Lesser[*TestSpec]{
				OrderById,
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeBinLog,
				errorOn:     0,
				msg:         "with log update error",
			},
			expectError: "with log update error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			util := &testUtil{
				test:         t,
				lines:        tc.lines,
				filters:      tc.filters,
				mutators:     tc.mutators,
				orderBys:     tc.orderBys,
				mockError:    tc.mockErr,
				expectError:  tc.expectError,
				expect:       tc.expect,
				expectLines:  tc.expectLines,
				expectBinLog: tc.expectBinLog,
			}

			err = util.setup()
			defer util.teardown()

			if err != nil {
				t.Fatal(err)
			}

			util.expectUpdate()
		})
	}
}
