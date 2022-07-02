package jsonl

import (
	"encoding/json"
	"time"

	"github.com/yo3jones/stg/pkg/stg"
)

type JsonlMarshalUnmarshaller[I comparable, S stg.Spec[I]] struct{}

func (*JsonlMarshalUnmarshaller[I, S]) Marshal(v S) ([]byte, error) {
	return json.Marshal(v)
}

func (*JsonlMarshalUnmarshaller[I, S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}

type JsonlMutation[I comparable] struct {
	TransactionId string         `json:"transactionId"`
	Timestamp     time.Time      `json:"timestamp"`
	Type          string         `json:"type"`
	Id            I              `json:"id"`
	Partition     string         `json:"partition"`
	From          map[string]any `json:"from"`
	To            map[string]any `json:"to"`
}

func (mutation *JsonlMutation[I]) Add(field string, from, to any) {
	if _, exists := mutation.From[field]; !exists {
		mutation.From[field] = from
	}
	mutation.To[field] = to
}

func (mutation *JsonlMutation[I]) GetFrom() map[string]any {
	return mutation.From
}

func (mutation *JsonlMutation[I]) GetId() I {
	return mutation.Id
}

func (mutation *JsonlMutation[I]) GetPartition() string {
	return mutation.Partition
}

func (mutation *JsonlMutation[I]) GetTimestamp() time.Time {
	return mutation.Timestamp
}

func (mutation *JsonlMutation[I]) GetTo() map[string]any {
	return mutation.To
}

func (mutation *JsonlMutation[I]) GetTransactionId() string {
	return mutation.TransactionId
}

func (mutation *JsonlMutation[I]) GetType() string {
	return mutation.Type
}
