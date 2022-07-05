package jsonl

import (
	"encoding/json"
	"time"
)

type JsonlMarshalUnmarshaller[S any] struct{}

func (*JsonlMarshalUnmarshaller[S]) Marshal(v S) ([]byte, error) {
	return json.Marshal(v)
}

func (*JsonlMarshalUnmarshaller[S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}

type JsonlMutation struct {
	TransactionId string         `json:"transactionId"`
	Timestamp     time.Time      `json:"timestamp"`
	Type          string         `json:"type"`
	Id            any            `json:"id"`
	Partition     string         `json:"partition"`
	From          map[string]any `json:"from"`
	To            map[string]any `json:"to"`
}

func (mutation *JsonlMutation) Add(field string, from, to any) {
	if _, exists := mutation.From[field]; !exists {
		mutation.From[field] = from
	}
	mutation.To[field] = to

	// TODO check if the from and to values are equal, if so then remove
	// this may be tough for non comparable types, might be able to do specific
	// logic for set, slice and map?
	//
	// also need to worry about type safety as we don't have compile time
	// checking that the from and to types match
}

func (mutation *JsonlMutation) GetFrom() map[string]any {
	return mutation.From
}

func (mutation *JsonlMutation) GetId() any {
	return mutation.Id
}

func (mutation *JsonlMutation) GetPartition() string {
	return mutation.Partition
}

func (mutation *JsonlMutation) GetTimestamp() time.Time {
	return mutation.Timestamp
}

func (mutation *JsonlMutation) GetTo() map[string]any {
	return mutation.To
}

func (mutation *JsonlMutation) GetTransactionId() string {
	return mutation.TransactionId
}

func (mutation *JsonlMutation) GetType() string {
	return mutation.Type
}
