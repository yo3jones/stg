package fstln

import (
	"testing"
)

func TestMaintenance(t *testing.T) {
	type test struct {
		name        string
		lines       []string
		options     []Option
		error       *mockError
		expectFreed int
		expect      []string
	}

	tests := []test{
		{
			name: "with no empty",
			lines: []string{
				"one",
				"two",
			},
			expectFreed: 0,
			expect: []string{
				"one",
				"two",
			},
		},
		{
			name: "with empty lines",
			lines: []string{
				"   ",
				"one",
				"   ",
				"",
				"two",
				"   ",
			},
			expectFreed: 13,
			expect: []string{
				"one",
				"two",
			},
		},
		{
			name: "with seek error",
			lines: []string{
				"one",
				"two",
			},
			error: &mockError{
				errorType: mockErrorTypeSeek,
				errorOn:   2,
				msg:       "with seek error",
			},
		},
		{
			name: "with read error 0",
			lines: []string{
				"foo",
				"bar",
				"fiz",
			},
			options: []Option{OptionBufferSize{4}},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   0,
				msg:       "with read error 0",
			},
		},
		{
			name: "with read error 1",
			lines: []string{
				"foo",
				"bar",
				"fiz",
			},
			options: []Option{OptionBufferSize{2}},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   1,
				msg:       "with read error 1",
			},
		},
		{
			name: "with write error",
			lines: []string{
				"   ",
				"foo",
				"bar",
				"fiz",
			},
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   0,
				msg:       "with write error",
			},
		},
		{
			name: "with truncate error",
			lines: []string{
				"   ",
				"foo",
				"bar",
				"fiz",
			},
			error: &mockError{
				errorType: mockErrorTypeTruncate,
				errorOn:   0,
				msg:       "with truncate error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			util, _, err := NewTestUtil().
				SetTest(t).
				SetName("test.jsonl").
				SetLines(tc.lines...).
				SetOptions(tc.options...).
				SetMockError(tc.error).
				Setup()
			defer util.Teardown()
			if err != nil {
				t.Fatal(err)
			}

			var gotFreed int

			gotFreed, err = util.Stg.Maintenance()

			if tc.error == nil && err != nil {
				t.Fatal(err)
			}

			if tc.error != nil && err == nil {
				t.Fatalf("expected an error but got nil")
			}

			if tc.error != nil && err.Error() != tc.error.msg {
				t.Fatalf(
					"expected error to be %s but got %s",
					tc.error.msg,
					err.Error(),
				)
			}

			if gotFreed != tc.expectFreed {
				t.Errorf(
					"expected maintenance to have freed %d bytes but got %d",
					tc.expectFreed,
					gotFreed,
				)
			}

			got := util.ReadOutput()
			expect := util.Join(tc.expect...)

			if tc.error == nil && got != expect {
				t.Errorf(
					"expected output to be \n%s\n but got \n%s\n",
					expect,
					got,
				)
			}
		})
	}
}
