package fstln

import (
	"strings"
	"testing"
)

func TestInsert(t *testing.T) {
	type test struct {
		name         string
		lines        []string
		stripNewLine bool
		expect       []string
		error        *mockError
	}

	tests := []test{
		{
			name: "with append",
			lines: []string{
				"one",
				"two",
			},
			expect: []string{
				"one",
				"two",
				"ONE",
				"TWO",
			},
		},
		{
			name: "with empty lines",
			lines: []string{
				"   ",
				"one",
				"   ",
				"two",
				"   ",
			},
			expect: []string{
				"ONE",
				"one",
				"TWO",
				"two",
				"   ",
			},
		},
		{
			name: "with large empty line",
			lines: []string{
				"       ",
				"one",
				"two",
			},
			expect: []string{
				"ONE",
				"TWO",
				"one",
				"two",
			},
		},
		{
			name: "with combine small empty lines",
			lines: []string{
				" ",
				" ",
				"one",
				"two",
			},
			expect: []string{
				"ONE",
				"one",
				"two",
				"TWO",
			},
		},
		{
			name: "with extra empty space",
			lines: []string{
				"     ",
				"one",
				"two",
			},
			expect: []string{
				"ONE",
				" ",
				"one",
				"two",
				"TWO",
			},
		},
		{
			name: "without new line",
			lines: []string{
				"   ",
				"one",
				"two",
			},
			stripNewLine: true,
			expect: []string{
				"ONE",
				"one",
				"two",
				"TWO",
			},
		},
		{
			name: "with empty line",
			lines: []string{
				"before",
				"",
				"after",
			},
			stripNewLine: true,
			expect: []string{
				"before",
				"",
				"after",
				"BEFORE",
				"",
				"AFTER",
			},
		},
		{
			name: "with write error",
			lines: []string{
				"one",
				"two",
			},
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   0,
				msg:       "mock write error",
			},
		},
		{
			name: "with write error no new line",
			lines: []string{
				"one",
				"two",
			},
			stripNewLine: true,
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   1,
				msg:       "mock write error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				err  error
				util *TestUtil
			)

			util, _, err = NewTestUtil().
				SetTest(t).
				SetName("test.jsonl").
				SetLines(tc.lines...).
				SetMockError(tc.error).
				Setup()
			defer util.Teardown()
			if err != nil {
				t.Fatal(err)
			}

			err = util.ReadAllCallback(
				func(_ Position, line string, stg *storage) (err error) {
					if tc.stripNewLine {
						line = line[:len(line)-1]
					}
					_, err = stg.Insert([]byte(strings.ToUpper(line)))
					return err
				},
			)
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
