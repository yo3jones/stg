package obj

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/jsonl"
)

type TestSpec struct {
	Id   int    `json:"id"`
	Type string `json:"type"`
	Foo  string `json:"foo"`
	Bar  string `json:"bar"`
}

func (spec *TestSpec) GetId() int {
	return spec.Id
}

func (spec *TestSpec) String() string {
	var (
		b   []byte
		err error
	)

	if b, err = json.MarshalIndent(spec, "", "  "); err != nil {
		return "woops"
	}

	return string(b)
}

type TestSpecFactory struct{}

func (factory *TestSpecFactory) New() *TestSpec {
	return &TestSpec{}
}

var (
	FooAccessor = &fooAccessor{}
	BarAccessor = &barAccessor{}
)

var (
	OrderByFoo     = OrderBy[int, *TestSpec, string](FooAccessor)
	OrderByFooDesc = OrderByDesc[int, *TestSpec, string](FooAccessor)
	OrderByBar     = OrderBy[int, *TestSpec, string](BarAccessor)
	OrderByBarDesc = OrderByDesc[int, *TestSpec, string](BarAccessor)
)

type fooAccessor struct{}

func (*fooAccessor) Get(s *TestSpec) string {
	return s.Foo
}

func (*fooAccessor) Name() string {
	return "foo"
}

func (*fooAccessor) Set(s *TestSpec, v string) {
	s.Foo = v
}

func FooEquals(v string) Matcher[int, *TestSpec] {
	return Equals[int, *TestSpec, string](FooAccessor, v)
}

func MutateFoo(v string) Mutator[string, int, *TestSpec] {
	return NewMutator[string, int, *TestSpec, string](FooAccessor, v)
}

type barAccessor struct{}

func (*barAccessor) Get(s *TestSpec) string {
	return s.Bar
}

func (*barAccessor) Name() string {
	return "bar"
}

func (*barAccessor) Set(s *TestSpec, v string) {
	s.Bar = v
}

func BarEquals(v string) Matcher[int, *TestSpec] {
	return Equals[int, *TestSpec, string](BarAccessor, v)
}

type testUtil struct {
	file        *os.File
	fstlnstg    fstln.Storage
	stg         *storage[string, int, *TestSpec]
	test        *testing.T
	lines       []string
	filters     Matcher[int, *TestSpec]
	orderBys    []Lesser[int, *TestSpec]
	bufferLen   int
	mockError   *mockErr
	expectError string
	expect      []*TestSpec
}

func (util *testUtil) setup() (err error) {
	os.Remove("test.jsonl")

	if util.file, err = os.Create("test.jsonl"); err != nil {
		return err
	}

	var fstlnstg fstln.Storage
	if fstlnstg, err = fstln.New(util.file); err != nil {
		return err
	}
	util.fstlnstg = &mockStg{
		stg:     fstlnstg,
		mockErr: util.mockError,
	}

	bufferLen := 1000
	if util.bufferLen != 0 {
		bufferLen = util.bufferLen
	}

	util.stg = &storage[string, int, *TestSpec]{
		stg:          util.fstlnstg,
		factory:      &TestSpecFactory{},
		unmarshaller: &jsonl.JsonlMarshalUnmarshaller[int, *TestSpec]{},
		concurrency:  2,
		bufferLen:    bufferLen,
	}

	if err = util.writeLines(util.lines); err != nil {
		return err
	}

	return nil
}

func (util *testUtil) teardown() {
	if util.file != nil {
		util.file.Close()
	}
	os.Remove("test.jsonl")
}

func (util *testUtil) writeLines(lines []string) (err error) {
	for _, line := range lines {
		if _, err = fmt.Fprintf(util.file, "%s\n", line); err != nil {
			return err
		}
	}
	return nil
}

func (util *testUtil) expectSelect() {
	var (
		err    error
		result []*TestSpec
	)

	result, err = util.stg.Select(util.filters, util.orderBys...)

	if util.expectError != "" && err == nil {
		util.test.Errorf("expected an error but got nil")
		return
	}

	if util.expectError != "" && err.Error() != util.expectError {
		util.test.Errorf(
			"expected an error with message \n%s\n but got \n%s\n",
			util.expectError,
			err.Error(),
		)
		return
	}

	if util.expectError != "" {
		return
	}

	if !reflect.DeepEqual(result, util.expect) {
		util.test.Errorf(
			"expected select result to be \n%s\n but got \n%s\n",
			testSpecSliceString(util.expect),
			testSpecSliceString(result),
		)
	}
}

func testSpecSliceString(specs []*TestSpec) string {
	var (
		b   []byte
		err error
	)

	if b, err = json.MarshalIndent(specs, "", "  "); err != nil {
		return "woops"
	}

	return string(b)
}

type mockStg struct {
	callCounts map[mockErrType]int
	mockErr    *mockErr
	stg        fstln.Storage
}

func (mock *mockStg) Delete(
	pos fstln.Position,
) (err error) {
	return mock.stg.Delete(pos)
}

func (mock *mockStg) Insert(
	line []byte,
) (pos fstln.Position, err error) {
	return mock.stg.Insert(line)
}

func (mock *mockStg) Maintenance() (freed int, err error) {
	return mock.stg.Maintenance()
}

func (mock *mockStg) Read(
	line []byte,
) (pos fstln.Position, n int, isPrefix bool, err error) {
	if err = mock.handleMockError(mockErrTypeRead); err != nil {
		return pos, n, isPrefix, err
	}
	return mock.stg.Read(line)
}

func (mock *mockStg) ResetScan() (err error) {
	if err = mock.handleMockError(mockErrTypeResetScan); err != nil {
		return err
	}
	return mock.stg.ResetScan()
}

func (mock *mockStg) Update(
	pos fstln.Position,
	line []byte,
) (afterPos fstln.Position, err error) {
	return mock.stg.Update(pos, line)
}

func (mock *mockStg) getCallCount(t mockErrType) int {
	if mock.callCounts == nil {
		mock.callCounts = map[mockErrType]int{}
	}

	var (
		callCounts int
		exists     bool
	)

	if callCounts, exists = mock.callCounts[t]; !exists {
		return 0
	} else {
		return callCounts
	}
}

func (mock *mockStg) incrementCallCount(t mockErrType) {
	callCount := mock.getCallCount(t)
	mock.callCounts[t] = callCount + 1
}

func (mock *mockStg) handleMockError(t mockErrType) error {
	defer mock.incrementCallCount(t)

	if mock.mockErr == nil {
		return nil
	}

	if mock.mockErr.mockErrType != t {
		return nil
	}

	callCount := mock.getCallCount(t)

	if callCount == mock.mockErr.errorOn {
		return fmt.Errorf("%s", mock.mockErr.msg)
	}

	return nil
}

type mockErr struct {
	mockErrType mockErrType
	errorOn     int
	msg         string
}

type mockErrType int

const (
	mockErrTypeResetScan mockErrType = iota
	mockErrTypeRead
)
