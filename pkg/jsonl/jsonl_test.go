package jsonl

import (
	"reflect"
	"testing"
	"time"
)

type TestSpec struct {
	Id  int    `json:"id"`
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

func (spec *TestSpec) GetId() int {
	return spec.Id
}

func TestMarshal(t *testing.T) {
	jsonlMarshalUnmarshaller := &JsonlMarshalUnmarshaller[*TestSpec]{}
	spec := &TestSpec{1, "foo", "bar"}

	got, err := jsonlMarshalUnmarshaller.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}

	gotString := string(got)
	expect := `{"id":1,"foo":"foo","bar":"bar"}`

	if gotString != expect {
		t.Errorf(
			"expected the unmarshal value to be %s but got %s",
			expect,
			gotString,
		)
	}
}

func TestUnmarshal(t *testing.T) {
	jsonlMarshalUnmarshaller := &JsonlMarshalUnmarshaller[*TestSpec]{}
	data := []byte(`{"id":1,"foo":"foo","bar":"bar"}`)
	got := &TestSpec{}

	err := jsonlMarshalUnmarshaller.Unmarshal([]byte(data), got)
	expect := &TestSpec{1, "foo", "bar"}

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expect) {
		t.Errorf("expected unmarshalled value to be %v but got %v", expect, got)
	}
}

func TestMutator(t *testing.T) {
	mutator := &JsonlMutation{
		TransactionId: "foo",
		Timestamp:     time.Now(),
		Type:          "bar",
		Id:            1,
		Partition:     "fiz",
		From:          map[string]any{"buz": "baz"},
		To:            map[string]any{"buz": "quz"},
	}

	var got any

	if got = mutator.GetFrom(); !reflect.DeepEqual(got, mutator.From) {
		t.Errorf("expected from to be %v but got %v", mutator.From, got)
	}

	if got = mutator.GetId(); got != mutator.Id {
		t.Errorf("expected id to be %d but got %d", mutator.Id, got)
	}

	if got = mutator.GetPartition(); got != mutator.Partition {
		t.Errorf(
			"expected partition to be %s but got %s",
			mutator.Partition,
			got,
		)
	}

	if got = mutator.GetTimestamp(); !reflect.DeepEqual(
		got,
		mutator.Timestamp,
	) {
		t.Errorf(
			"expected timestamp to be %v but got %v",
			mutator.Timestamp,
			got,
		)
	}

	if got = mutator.GetTo(); !reflect.DeepEqual(got, mutator.To) {
		t.Errorf("expected to to be %v but got %v", mutator.To, got)
	}

	if got = mutator.GetTransactionId(); got != mutator.TransactionId {
		t.Errorf(
			"expected TransactionId to be %s but got %s",
			mutator.TransactionId,
			got,
		)
	}

	if got = mutator.GetType(); got != mutator.Type {
		t.Errorf("expected type to be %s but got %s", mutator.Type, got)
	}
}

func TestMutatorAdd(t *testing.T) {
	mutator := &JsonlMutation{
		From: map[string]any{},
		To:   map[string]any{},
	}

	mutator.Add("foo", "bar", "baz")

	var (
		exists bool
		got    any
	)

	got, exists = mutator.From["foo"]
	if !exists {
		t.Errorf("expected from to contain foo but it did not")
	}
	if got != "bar" {
		t.Errorf("expected from to contain value bar but did not")
	}

	got, exists = mutator.To["foo"]
	if !exists {
		t.Errorf("expected to to contain foo but it did not")
	}
	if got != "baz" {
		t.Errorf("expected to to contain value baz but did not")
	}

	mutator.Add("foo", "baz", "fuz")

	got, exists = mutator.From["foo"]
	if !exists {
		t.Errorf("expected from to contain foo but it did not")
	}
	if got != "bar" {
		t.Errorf("expected from to contain value bar but did not")
	}

	got, exists = mutator.To["foo"]
	if !exists {
		t.Errorf("expected to to contain foo but it did not")
	}
	if got != "fuz" {
		t.Errorf("expected to to contain value fuz but did not")
	}
}
