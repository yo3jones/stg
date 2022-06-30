package fstln

import (
	"os"
	"testing"
)

func TestNewHappyPath(t *testing.T) {
	var (
		err  error
		file *os.File
		stg  Storage
	)

	os.Remove("test.jsonl")
	if err = os.WriteFile("test.jsonl", []byte("foo\n"), 0666); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test.jsonl")

	if file, err = os.OpenFile("test.jsonl", os.O_RDWR, 0666); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	if stg, err = New(file); err != nil {
		t.Fatal(err)
	}

	if stg == nil {
		t.Errorf("expected a storage interface but got nil")
	}
}

func TestIsOption(t *testing.T) {
	type test struct {
		name   string
		option Option
	}

	tests := []test{
		{
			name:   "OptionBufferSize",
			option: &OptionBufferSize{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.option.isOption()
			if !got {
				t.Errorf("expected option but got %t", got)
			}
		})
	}
}

func TestPositionLesser(t *testing.T) {
	type test struct {
		name   string
		i      Position
		j      Position
		expect bool
	}

	tests := []test{
		{
			name:   "with equal",
			i:      Position{Offset: 1, Len: 1},
			j:      Position{Offset: 1, Len: 1},
			expect: false,
		},
		{
			name:   "with len greater",
			i:      Position{Offset: 1, Len: 2},
			j:      Position{Offset: 1, Len: 1},
			expect: true,
		},
		{
			name:   "with len less",
			i:      Position{Offset: 1, Len: 1},
			j:      Position{Offset: 1, Len: 2},
			expect: false,
		},
		{
			name:   "with offset greater",
			i:      Position{Offset: 2, Len: 1},
			j:      Position{Offset: 1, Len: 1},
			expect: false,
		},
		{
			name:   "with offset less",
			i:      Position{Offset: 1, Len: 1},
			j:      Position{Offset: 2, Len: 1},
			expect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PositionLesser(tc.i, tc.j)

			if got != tc.expect {
				t.Errorf(
					"expected lesser to return %t but got %t",
					tc.expect,
					got,
				)
			}
		})
	}
}
