package objbinlog

import (
	"fmt"
	"io"
	"time"
)

func (trans *transaction[T]) End() {
	trans.ended = true
	trans.stg.lock.Unlock()
}

func (trans *transaction[T]) LogDelete(id any, from []byte) (err error) {
	return trans.writeLog(id, from, nil)
}

func (trans *transaction[T]) LogInsert(id any, to []byte) (err error) {
	return trans.writeLog(id, nil, to)
}

func (trans *transaction[T]) LogUpdate(
	id any,
	from, to []byte,
) (err error) {
	return trans.writeLog(id, from, to)
}

func (trans *transaction[T]) writeLog(id any, from, to []byte) (err error) {
	var (
		data                []byte
		handle              = trans.stg.handle
		log                 *Log[T]
		marshalUnmarshaller = trans.stg.marshalUnmarshaller
		n                   int
		offset              int64
		timestamp           = trans.timestamp
	)

	trans.stg.writeLock.Lock()
	defer trans.stg.writeLock.Unlock()

	if trans.ended {
		return fmt.Errorf("%w", endedError)
	}

	log = &Log[T]{
		TransactionId: trans.transactionId,
		Type:          trans.objType,
		Id:            id,
		Timestamp:     timestamp,
		From:          DataWrapper{from},
		To:            DataWrapper{to},
	}

	if data, err = marshalUnmarshaller.Marshal(log); err != nil {
		return err
	}

	if offset, err = handle.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	if n, err = handle.WriteAt(data, offset); err != nil {
		return err
	}

	if _, err = handle.WriteAt([]byte{'\n'}, offset+int64(n)); err != nil {
		return err
	}

	return nil
}

type Log[T comparable] struct {
	TransactionId T           `json:"transaction"`
	Type          string      `json:"type"`
	Id            any         `json:"id"`
	Timestamp     time.Time   `json:"ts"`
	From          DataWrapper `json:"from"`
	To            DataWrapper `json:"to"`
}

type DataWrapper struct {
	data []byte
}

func (wrapper *DataWrapper) MarshalJSON() ([]byte, error) {
	if wrapper.data == nil {
		return []byte("null"), nil
	}
	return wrapper.data, nil
}

var endedError = fmt.Errorf("illegal state error, transaction has ended")
