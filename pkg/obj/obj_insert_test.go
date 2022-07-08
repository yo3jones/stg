package obj

import (
	"testing"
)

func TestInsert(t *testing.T) {
	type test struct {
		name         string
		lines        []string
		mutators     []Mutator[*TestSpec]
		mockErr      *mockErr
		expectError  string
		expect       *TestSpec
		expectLines  [][]string
		expectBinLog [][]string
	}

	tests := []test{
		{
			name: "with success",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			mutators: []Mutator[*TestSpec]{
				MutateFoo("foo"),
				MutateBar("bar"),
			},
			expect: &TestSpec{
				Id:        100,
				Foo:       "foo",
				Bar:       "bar",
				UpdatedAt: GetTestNow(),
				CreatedAt: GetTestNow(),
			},
			expectLines: [][]string{
				{
					`{"id":1,"foo":"foo","bar":"bar"}`,
					`{"id":2,"foo":"fiz","bar":"buz"}`,
					`{"id":100,"type":"","foo":"foo","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"2022-07-06T16:18:00-04:00"}`,
				},
			},
			expectBinLog: [][]string{
				{
					`{"transaction":200,"type":"test","id":100,"ts":"2022-07-06T16:18:00-04:00","from":null,"to":{"id":100,"type":"","foo":"foo","bar":"bar","updatedAt":"2022-07-06T16:18:00-04:00","createdAt":"2022-07-06T16:18:00-04:00"}}`,
				},
			},
		},
		{
			name: "with marshal error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			mutators: []Mutator[*TestSpec]{
				MutateFoo("foo"),
				MutateBar("bar"),
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeMarshal,
				errorOn:     0,
				msg:         "with marshal error",
			},
			expectError: "with marshal error",
		},
		{
			name: "with insert error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			mutators: []Mutator[*TestSpec]{
				MutateFoo("foo"),
				MutateBar("bar"),
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeInsert,
				errorOn:     0,
				msg:         "with insert error",
			},
			expectError: "with insert error",
		},
		{
			name: "with log insert error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"buz"}`,
			},
			mutators: []Mutator[*TestSpec]{
				MutateFoo("foo"),
				MutateBar("bar"),
			},
			mockErr: &mockErr{
				mockErrType: mockErrTypeBinLog,
				errorOn:     0,
				msg:         "with log insert error",
			},
			expectError: "with log insert error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			util := &testUtil{
				test:         t,
				lines:        tc.lines,
				mutators:     tc.mutators,
				mockError:    tc.mockErr,
				expectError:  tc.expectError,
				expect:       []*TestSpec{tc.expect},
				expectLines:  tc.expectLines,
				expectBinLog: tc.expectBinLog,
			}

			err = util.setup()
			defer util.teardown()

			if err != nil {
				t.Fatal(err)
			}

			util.expectInsert()
		})
	}
}
