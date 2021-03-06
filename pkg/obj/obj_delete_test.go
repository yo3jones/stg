package obj

import "testing"

func TestDelete(t *testing.T) {
	type test struct {
		name         string
		lines        []string
		filters      Matcher[*TestSpec]
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
			expect: []*TestSpec{
				{Id: 1, Foo: "foo", Bar: "bar"},
				{Id: 2, Foo: "fiz", Bar: "bar"},
			},
			expectLines: [][]string{
				{
					`                                `,
					`                                `,
					`{"id":3,"foo":"fam","bar":"baz"}`,
				},
			},
			expectBinLog: [][]string{
				{
					`{"transaction":200,"type":"test","id":1,"ts":"2022-07-06T16:18:00-04:00","from":{"id":1,"foo":"foo","bar":"bar"},"to":null}`,
					`{"transaction":200,"type":"test","id":2,"ts":"2022-07-06T16:18:00-04:00","from":{"id":2,"foo":"fiz","bar":"bar"},"to":null}`,
				},
				{
					`{"transaction":200,"type":"test","id":2,"ts":"2022-07-06T16:18:00-04:00","from":{"id":2,"foo":"fiz","bar":"bar"},"to":null}`,
					`{"transaction":200,"type":"test","id":1,"ts":"2022-07-06T16:18:00-04:00","from":{"id":1,"foo":"foo","bar":"bar"},"to":null}`,
				},
			},
		},
		{
			name: "with delete error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mockErr: &mockErr{
				mockErrType: mockErrTypeDelete,
				errorOn:     0,
				msg:         "with delete error",
			},
			expectError: "with delete error",
		},
		{
			name: "with bin log error",
			lines: []string{
				`{"id":1,"foo":"foo","bar":"bar"}`,
				`{"id":2,"foo":"fiz","bar":"bar"}`,
				`{"id":3,"foo":"fam","bar":"baz"}`,
			},
			filters: BarEquals("bar"),
			mockErr: &mockErr{
				mockErrType: mockErrTypeBinLog,
				errorOn:     0,
				msg:         "with bin log error",
			},
			expectError: "with bin log error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			util := &testUtil{
				test:         t,
				lines:        tc.lines,
				filters:      tc.filters,
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

			util.expectDelete()
		})
	}
}
