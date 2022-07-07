package fstln

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestDelete(t *testing.T) {
	type test struct {
		name   string
		lines  []string
		expect []string
		error  *mockError
	}

	tests := []test{
		{
			name: "with delete",
			lines: []string{
				"one",
				"two",
			},
			expect: []string{
				"   ",
				"   ",
			},
		},
		{
			name: "with delete empty lines",
			lines: []string{
				"one",
				"",
				"two",
				"    ",
			},
			expect: []string{
				"   ",
				"",
				"   ",
				"    ",
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
				func(pos Position, line string, stg *storage) error {
					return stg.Delete(pos)
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

func TestInsert(t *testing.T) {
	type test struct {
		name         string
		lines        []string
		stripNewLine bool
		options      []Option
		insertCount  int
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
				" ",
				"after",
			},
			stripNewLine: true,
			expect: []string{
				"before",
				"",
				" ",
				"after",
				"BEFORE",
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
			name: "with write error blank",
			lines: []string{
				"   ",
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
				SetOptions(tc.options...).
				SetMockError(tc.error).
				Setup()
			defer util.Teardown()
			if err != nil {
				t.Fatal(err)
			}

			insertCount := 0
			err = util.ReadAllCallback(
				func(_ Position, line string, stg *storage) (err error) {
					if tc.stripNewLine {
						line = line[:len(line)-1]
					}
					if tc.insertCount == 0 || insertCount < tc.insertCount {
						_, err = stg.Insert([]byte(strings.ToUpper(line)))
						insertCount++
					}
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

	t.Run("with empty write", func(t *testing.T) {
		var (
			err    error
			expect = Position{Offset: 0, Len: 1}
			got    Position
			util   *TestUtil
		)

		util, _, err = NewTestUtil().
			SetTest(t).
			SetName("test.jsonl").
			SetLines().
			Setup()
		defer util.Teardown()

		if err != nil {
			t.Fatal(err)
		}

		if got, err = util.Stg.Insert([]byte{}); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, expect) {
			t.Errorf(
				"expected to write at position \n%v\n but got \n%v\n",
				expect,
				got,
			)
		}
	})
}

func TestUpdate(t *testing.T) {
	var (
		longer = func(pos Position, line string, stg *storage) (err error) {
			part := strings.ToUpper(line[:len(line)-1])
			_, err = stg.Update(
				pos,
				[]byte(
					fmt.Sprintf("%s%s\n", part, part),
				),
			)
			return err
		}
		same = func(pos Position, line string, stg *storage) (err error) {
			part := strings.ToUpper(line[:len(line)-1])
			_, err = stg.Update(pos, []byte(fmt.Sprintf("%s\n", part)))
			return err
		}
		shorter = func(pos Position, line string, stg *storage) (err error) {
			part := strings.ToUpper(line[:len(line)/2])
			_, err = stg.Update(pos, []byte(fmt.Sprintf("%s\n", part)))
			return err
		}
	)
	type test struct {
		name    string
		lines   []string
		updater func(pos Position, line string, stg *storage) (err error)
		expect  []string
		error   *mockError
	}

	tests := []test{
		{
			name: "with same length",
			lines: []string{
				"one",
				"two",
			},
			updater: same,
			expect: []string{
				"ONE",
				"TWO",
			},
		},
		{
			name: "with longer length",
			lines: []string{
				"one",
				"two",
			},
			updater: longer,
			expect: []string{
				"   ",
				"   ",
				"ONEONE",
				"TWOTWO",
			},
		},
		{
			name: "with sorter length",
			lines: []string{
				"fooo",
				"baar",
			},
			updater: shorter,
			expect: []string{
				"FO",
				" ",
				"BA",
				" ",
			},
		},
		{
			name: "with empty lines",
			lines: []string{
				"     ",
				"",
				"foo",
				"bar",
			},
			updater: longer,
			expect: []string{
				"FOOFOO",
				"   ",
				"   ",
				"BARBAR",
			},
		},
		{
			name: "with inplace write error",
			lines: []string{
				"one",
				"two",
			},
			updater: same,
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   0,
				msg:       "with inplace write error",
			},
		},
		{
			name: "with inplace smaller write error",
			lines: []string{
				"oneone",
				"twotwo",
			},
			updater: shorter,
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   1,
				msg:       "with inplace smaller write error",
			},
		},
		{
			name: "with out of place write error",
			lines: []string{
				"one",
				"two",
			},
			updater: longer,
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   0,
				msg:       "with out of place write error",
			},
		},
		{
			name: "with out of place clear write error",
			lines: []string{
				"one",
				"two",
			},
			updater: longer,
			error: &mockError{
				errorType: mockErrorTypeWriteAt,
				errorOn:   1,
				msg:       "with out of place clear write error",
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

			err = util.ReadAllCallback(tc.updater)
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
