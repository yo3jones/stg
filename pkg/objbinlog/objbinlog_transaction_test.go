package objbinlog

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yo3jones/stg/pkg/stg"
)

type testHelper struct {
	t       *testing.T
	file    *os.File
	stg     BinLogStorage
	mockErr *mockErr
	expect  []string
}

func (helper *testHelper) setup() BinLogStorage {
	var err error

	os.Remove("test.jsonl")

	if helper.file, err = os.Create("test.jsonl"); err != nil {
		helper.t.Fatal(err)
	}

	// helper.stg = &binLogStorage[int]{
	// 	handle: &mockHandle{
	// 		handle:  helper.file,
	// 		mockErr: helper.mockErr,
	// 	},
	// 	idFactory:           &testIdFactory{},
	// 	marshalUnmarshaller: &testMarshalUnmarshaller{mockErr: helper.mockErr},
	// 	nower:               &testNower{},
	// }

	helper.stg = New[int](
		&mockHandle{handle: helper.file, mockErr: helper.mockErr},
		&testIdFactory{},
		&testMarshalUnmarshaller{mockErr: helper.mockErr},
		OptNower{&testNower{}},
	)

	return helper.stg
}

func (helper *testHelper) doExpect() {
	var (
		err          error
		expectSb     = &strings.Builder{}
		expectString string
		gotBytes     []byte
		gotString    string
	)

	if gotBytes, err = ioutil.ReadFile("test.jsonl"); err != nil {
		helper.t.Fatal(err)
	}

	gotString = string(gotBytes)

	for _, line := range helper.expect {
		fmt.Fprintf(expectSb, "%s\n", line)
	}
	expectString = expectSb.String()

	if gotString != expectString {
		helper.t.Errorf(
			"expected to have lines \n%s\n but got \n%s\n",
			expectString,
			gotString,
		)
	}
}

func (helper *testHelper) teardown() {
	if helper.file != nil {
		helper.file.Close()
	}
	os.Remove("test.jsonl")
}

func TestLogDelete(t *testing.T) {
	type test struct {
		name      string
		froms     [][]string
		mockErr   *mockErr
		expectErr string
		expect    []string
	}

	tests := []test{
		{
			name: "with success",
			froms: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			expect: []string{
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":0,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"foo"},`,
					`"to":null`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":1,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"bar"},`,
					`"to":null`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":2,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"fiz"},`,
					`"to":null`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":3,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"buz"},`,
					`"to":null`,
					`}`,
				}, ""),
			},
		},
		{
			name: "with marshal error",
			froms: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			mockErr: &mockErr{
				errType: mockErrTypeMarshal,
				on:      0,
				msg:     "with marshal error",
			},
			expectErr: "with marshal error",
		},
		{
			name: "with seek error",
			froms: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			mockErr: &mockErr{
				errType: mockErrTypeSeek,
				on:      0,
				msg:     "with seek error",
			},
			expectErr: "with seek error",
		},
		{
			name: "with writeAt error 0",
			froms: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			mockErr: &mockErr{
				errType: mockErrTypeWriteAt,
				on:      0,
				msg:     "with writeAt error 0",
			},
			expectErr: "with writeAt error 0",
		},
		{
			name: "with writeAt error 1",
			froms: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			mockErr: &mockErr{
				errType: mockErrTypeWriteAt,
				on:      1,
				msg:     "with writeAt error 1",
			},
			expectErr: "with writeAt error 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				err         error
				stg         BinLogStorage
				transaction Transaction
			)

			helper := &testHelper{
				t:       t,
				mockErr: tc.mockErr,
				expect:  tc.expect,
			}

			stg = helper.setup()
			defer helper.teardown()

			id := 0
			for _, froms := range tc.froms {
				transaction = stg.StartTransaction("test")
				for _, from := range froms {
					if err != nil {
						continue
					}
					err = transaction.LogDelete(id, []byte(from))
					id++
				}
				transaction.End()
			}

			if err != nil && tc.expectErr == "" {
				t.Fatal(err)
			}

			if tc.expectErr != "" && err == nil {
				t.Fatalf("expected an error but got nil")
			}

			if tc.expectErr != "" && err != nil && err.Error() != tc.expectErr {
				t.Fatalf(
					"expected and error with msg \n%s\n but got \n%s\n",
					tc.expectErr,
					err.Error(),
				)
			}

			if tc.expectErr != "" {
				return
			}

			helper.doExpect()

			err = transaction.LogDelete(99, []byte(`{"foo":"foo"}`))

			if err == nil {
				t.Fatalf(
					"expected an error logging after transaction is complete but got nil",
				)
			}

			if err.Error() != "illegal state error, transaction has ended" {
				t.Fatalf(
					"expected error with msg \n%s\n but got \n%s\n",
					"illegal state error, transaction has ended",
					err.Error(),
				)
			}
		})
	}
}

func TestLogInsert(t *testing.T) {
	type test struct {
		name   string
		tos    [][]string
		expect []string
	}

	tests := []test{
		{
			name: "with success",
			tos: [][]string{
				{
					`{"foo":"foo"}`,
					`{"foo":"bar"}`,
				},
				{
					`{"foo":"fiz"}`,
					`{"foo":"buz"}`,
				},
			},
			expect: []string{
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":0,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":null,`,
					`"to":{"foo":"foo"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":1,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":null,`,
					`"to":{"foo":"bar"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":2,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":null,`,
					`"to":{"foo":"fiz"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":3,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":null,`,
					`"to":{"foo":"buz"}`,
					`}`,
				}, ""),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				err         error
				stg         BinLogStorage
				transaction Transaction
			)

			helper := &testHelper{
				t:      t,
				expect: tc.expect,
			}

			stg = helper.setup()
			defer helper.teardown()

			id := 0
			for _, tos := range tc.tos {
				transaction = stg.StartTransaction("test")
				for _, to := range tos {
					if err != nil {
						continue
					}
					err = transaction.LogInsert(id, []byte(to))
					id++
				}
				transaction.End()
			}

			if err != nil {
				t.Fatal(err)
			}

			helper.doExpect()
		})
	}
}

func TestLogUpdate(t *testing.T) {
	type test struct {
		name    string
		updates [][][]string
		expect  []string
	}

	tests := []test{
		{
			name: "with success",
			updates: [][][]string{
				{
					{`{"foo":"foo"}`, `{"foo":"FOO"}`},
					{`{"foo":"bar"}`, `{"foo":"BAR"}`},
				},
				{
					{`{"foo":"fiz"}`, `{"foo":"FIZ"}`},
					{`{"foo":"buz"}`, `{"foo":"BUZ"}`},
				},
			},
			expect: []string{
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":0,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"foo"},`,
					`"to":{"foo":"FOO"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":0,`,
					`"type":"test",`,
					`"id":1,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"bar"},`,
					`"to":{"foo":"BAR"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":2,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"fiz"},`,
					`"to":{"foo":"FIZ"}`,
					`}`,
				}, ""),
				strings.Join([]string{
					`{`,
					`"transaction":1,`,
					`"type":"test",`,
					`"id":3,`,
					`"ts":"2022-07-07T13:51:57-04:00",`,
					`"from":{"foo":"buz"},`,
					`"to":{"foo":"BUZ"}`,
					`}`,
				}, ""),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				err         error
				stg         BinLogStorage
				transaction Transaction
			)

			helper := &testHelper{
				t:      t,
				expect: tc.expect,
			}

			stg = helper.setup()
			defer helper.teardown()

			id := 0
			for _, updates := range tc.updates {
				transaction = stg.StartTransaction("test")
				for _, update := range updates {
					if err != nil {
						continue
					}
					err = transaction.LogUpdate(
						id,
						[]byte(update[0]),
						[]byte(update[1]),
					)
					id++
				}
				transaction.End()
			}

			if err != nil {
				t.Fatal(err)
			}

			helper.doExpect()
		})
	}
}

type testIdFactory struct {
	value int
}

func (factory *testIdFactory) New() int {
	defer func() { factory.value++ }()
	return factory.value
}

type testMarshalUnmarshaller struct {
	mockErr          *mockErr
	marshalCallCount int
}

func (marshalUnmarshaller *testMarshalUnmarshaller) Marshal(
	v any,
) ([]byte, error) {
	defer func() { marshalUnmarshaller.marshalCallCount++ }()
	if marshalUnmarshaller.mockErr != nil &&
		marshalUnmarshaller.mockErr.errType == mockErrTypeMarshal &&
		marshalUnmarshaller.marshalCallCount == marshalUnmarshaller.mockErr.on {
		return nil, fmt.Errorf("%s", marshalUnmarshaller.mockErr.msg)
	}
	return json.Marshal(v)
}

func (marshalUnmarshaller *testMarshalUnmarshaller) Unmarshal(
	data []byte,
	v any,
) error {
	return json.Unmarshal(data, v)
}

func GetTestNow() time.Time {
	now, err := time.Parse(time.RFC3339, "2022-07-07T13:51:57-04:00")
	if err != nil {
		fmt.Println(err)
	}
	return now
}

type testNower struct{}

type mockHandle struct {
	handle           stg.Handle
	mockErr          *mockErr
	seekCallCount    int
	writeAtCallCount int
}

func (mock *mockHandle) Read(p []byte) (int, error) {
	return mock.handle.Read(p)
}

func (mock *mockHandle) Seek(offset int64, whence int) (int64, error) {
	defer func() { mock.seekCallCount++ }()
	if mock.mockErr != nil &&
		mock.mockErr.errType == mockErrTypeSeek &&
		mock.seekCallCount == mock.mockErr.on {
		return 0, fmt.Errorf("%s", mock.mockErr.msg)
	}
	return mock.handle.Seek(offset, whence)
}

func (mock *mockHandle) WriteAt(p []byte, off int64) (n int, err error) {
	defer func() { mock.writeAtCallCount++ }()
	if mock.mockErr != nil &&
		mock.mockErr.errType == mockErrTypeWriteAt &&
		mock.writeAtCallCount == mock.mockErr.on {
		return 0, fmt.Errorf("%s", mock.mockErr.msg)
	}
	return mock.handle.WriteAt(p, off)
}

func (mock *mockHandle) Truncate(size int64) error {
	return mock.handle.Truncate(size)
}

func (nower *testNower) Now() time.Time {
	return GetTestNow()
}

type mockErrType int

const (
	mockErrTypeMarshal mockErrType = iota
	mockErrTypeSeek
	mockErrTypeWriteAt
)

type mockErr struct {
	errType mockErrType
	on      int
	msg     string
}
