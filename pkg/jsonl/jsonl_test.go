package jsonl

import (
	"reflect"
	"testing"

	"github.com/yo3jones/stg/pkg/obj"
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

func TestMarshalMutation(t *testing.T) {
	jsonlMarshalUnmarshaller := &JsonlMarshalUnmarshaller[*TestSpec]{}
	mutation := &obj.Mutation{Id: "foo"}

	got, err := jsonlMarshalUnmarshaller.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}

	gotString := string(got)
	expect := `{"transactionId":"","timestamp":"0001-01-01T00:00:00Z","type":"","id":"foo","partition":"","from":null,"to":null}`

	if gotString != expect {
		t.Errorf(
			"expected the mutation unmarshal value to be \n%s\n but got \n%s\n",
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

func TestMutationUnmarshal(t *testing.T) {
	jsonlMarshalUnmarshaller := &JsonlMarshalUnmarshaller[*TestSpec]{}
	data := []byte(
		`{"transactionId":"","timestamp":"0001-01-01T00:00:00Z","type":"","id":"foo","partition":"","from":null,"to":null}`,
	)
	got := &obj.Mutation{}

	err := jsonlMarshalUnmarshaller.UnmarshalMutation([]byte(data), got)
	expect := &obj.Mutation{Id: "foo"}

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expect) {
		t.Errorf(
			"expected unmarshalled mutation value to be %v but got %v",
			expect,
			got,
		)
	}
}
