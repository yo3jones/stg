package obj

import "testing"

func TestDelete(t *testing.T) {
	type test struct {
		name        string
		lines       []string
		filters     Matcher[*TestSpec]
		mockErr     *mockErr
		expectError string
		expect      []*TestSpec
		expectLines []string
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
			expectLines: []string{
				`                                `,
				`                                `,
				`{"id":3,"foo":"fam","bar":"baz"}`,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			util := &testUtil{
				test:        t,
				lines:       tc.lines,
				filters:     tc.filters,
				mockError:   tc.mockErr,
				expectError: tc.expectError,
				expect:      tc.expect,
				expectLines: tc.expectLines,
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
