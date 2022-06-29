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
