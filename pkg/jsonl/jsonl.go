package jsonl

import (
	"encoding/json"
)

type JsonlMarshalUnmarshaller[S any] struct{}

func (*JsonlMarshalUnmarshaller[S]) Marshal(v S) ([]byte, error) {
	return json.Marshal(v)
}

func (*JsonlMarshalUnmarshaller[S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}
