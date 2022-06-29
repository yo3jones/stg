package fstln

import (
	"testing"
)

func TestResetScan(t *testing.T) {
	type test struct {
		name  string
		error *mockError
	}

	tests := []test{
		{
			name: "with first scan error",
			error: &mockError{
				errorType: mockErrorTypeSeek,
				errorOn:   0,
				msg:       "first mock error",
			},
		},
		{
			name: "with second scan error",
			error: &mockError{
				errorType: mockErrorTypeSeek,
				errorOn:   1,
				msg:       "second mock error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			util, _, err := NewTestUtil().SetTest(t).
				SetName("test.jsonl").
				SetLines("one", "two").
				SetMockError(tc.error).
				Setup()
			defer util.Teardown()

			if err == nil {
				t.Fatalf("expected an error but got nil")
			}

			if err.Error() != tc.error.msg {
				t.Fatalf(
					"expected an error with msg \n%s\n but got \n%s\n",
					tc.error.msg,
					err.Error(),
				)
			}
		})
	}
}

func TestRead(t *testing.T) {
	type test struct {
		name           string
		lines          []string
		readBufferSize int
		options        []Option
		expect         []string
		error          *mockError
	}

	tests := []test{
		{
			name: "without empty lines",
			lines: []string{
				"one",
				"two",
			},
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
				"   ",
				"two",
				"   ",
			},
			expect: []string{
				"one",
				"two",
			},
		},
		{
			name: "with small read buffer size",
			lines: []string{
				"one",
				"two",
			},
			readBufferSize: 2,
			expect: []string{
				"one",
				"two",
			},
		},
		{
			name: "with small buffer size",
			lines: []string{
				"one",
				"two",
			},
			options: []Option{OptionBufferSize{2}},
			expect: []string{
				"one",
				"two",
			},
		},
		{
			name: "with read error",
			lines: []string{
				"one",
				"two",
			},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   0,
				msg:       "mock read error",
			},
		},
		{
			name: "with fill line error",
			lines: []string{
				"one",
				"two",
			},
			options: []Option{OptionBufferSize{2}},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   1,
				msg:       "mock read error",
			},
		},
		{
			name: "with empty line error",
			lines: []string{
				"    ",
				"one",
				"two",
			},
			options: []Option{OptionBufferSize{2}},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   1,
				msg:       "mock read error",
			},
		},
		{
			name: "with read line error",
			lines: []string{
				" ",
				"a",
				" ",
			},
			options: []Option{OptionBufferSize{2}},
			error: &mockError{
				errorType: mockErrorTypeRead,
				errorOn:   2,
				msg:       "mock read error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			util, _, err := NewTestUtil().
				SetTest(t).
				SetName("test.jsonl").
				SetLines(tc.lines...).
				SetReadBufferSize(tc.readBufferSize).
				SetOptions(tc.options...).
				SetMockError(tc.error).
				Setup()
			defer util.Teardown()
			if err != nil {
				t.Fatal(err)
			}

			expect := util.Join(tc.expect...)
			got, gotErr := util.RealAll()

			if gotErr != nil && tc.error == nil {
				t.Fatal(gotErr)
			}

			if tc.error != nil && gotErr == nil {
				t.Fatalf("expected an error but got nil")
			}

			if tc.error != nil && gotErr.Error() != tc.error.msg {
				t.Fatalf(
					"expected and error with message \n%s\n but got \n%s\n",
					tc.error.msg,
					gotErr.Error(),
				)
			}

			if got != expect {
				t.Errorf(
					"expected output to be \n%s\n but got \n%s\n",
					expect,
					got,
				)
			}
		})
	}
}
