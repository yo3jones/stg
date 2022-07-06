package jsonl

import (
	"encoding/json"

	"github.com/yo3jones/stg/pkg/obj"
)

type JsonlMarshalUnmarshaller[S any] struct{}

func (*JsonlMarshalUnmarshaller[S]) Marshal(v S) ([]byte, error) {
	return json.Marshal(v)
}

func (*JsonlMarshalUnmarshaller[S]) MarshalMutation(
	mutation *obj.Mutation,
) ([]byte, error) {
	return json.Marshal(mutation)
}

func (*JsonlMarshalUnmarshaller[S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}

func (*JsonlMarshalUnmarshaller[S]) UnmarshalMutation(
	data []byte,
	v *obj.Mutation,
) error {
	return json.Unmarshal(data, v)
}
