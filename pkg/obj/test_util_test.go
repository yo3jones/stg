package obj

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yo3jones/stg/pkg/fstln"
)

type TestSpec struct {
	Id        int       `json:"id"`
	Type      string    `json:"type"`
	Foo       string    `json:"foo"`
	Bar       string    `json:"bar"`
	UpdatedAt time.Time `json:"updatedAt"`
	CreatedAt time.Time `json:"createdAt"`
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

type TestNower struct{}

func (*TestNower) Now() time.Time {
	return GetTestNow()
}

func GetTestNow() time.Time {
	now, err := time.Parse(time.RFC3339, "2022-07-06T16:18:00-04:00")
	if err != nil {
		fmt.Println(err)
	}
	return now
}

var (
	IdAccessor        = &idAccessor{}
	FooAccessor       = &fooAccessor{}
	BarAccessor       = &barAccessor{}
	UpdatedAtAccessor = &updatedAtAccessor{}
	CreatedAtAccessor = &createdAtAccessor{}
)

var (
	OrderById      = OrderBy[*TestSpec, int](IdAccessor)
	OrderByFoo     = OrderBy[*TestSpec, string](FooAccessor)
	OrderByFooDesc = OrderByDesc[*TestSpec, string](FooAccessor)
	OrderByBar     = OrderBy[*TestSpec, string](BarAccessor)
	OrderByBarDesc = OrderByDesc[*TestSpec, string](BarAccessor)
)

type idAccessor struct{}

func (*idAccessor) Get(s *TestSpec) int {
	return s.Id
}

func (*idAccessor) Name() string {
	return "id"
}

func (*idAccessor) Set(s *TestSpec, v int) {
	s.Id = v
}

type updatedAtAccessor struct{}

func (*updatedAtAccessor) Get(s *TestSpec) time.Time {
	return s.UpdatedAt
}

func (*updatedAtAccessor) Name() string {
	return "updatedAt"
}

func (*updatedAtAccessor) Set(s *TestSpec, v time.Time) {
	s.UpdatedAt = v
}

type createdAtAccessor struct{}

func (*createdAtAccessor) Get(s *TestSpec) time.Time {
	return s.CreatedAt
}

func (*createdAtAccessor) Name() string {
	return "createdAt"
}

func (*createdAtAccessor) Set(s *TestSpec, v time.Time) {
	s.CreatedAt = v
}

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

func FooEquals(v string) Matcher[*TestSpec] {
	return Equals[*TestSpec, string](FooAccessor, v)
}

func MutateFoo(v string) Mutator[*TestSpec] {
	return NewMutator[*TestSpec, string](FooAccessor, v)
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

func BarEquals(v string) Matcher[*TestSpec] {
	return Equals[*TestSpec, string](BarAccessor, v)
}

func MutateBar(v string) Mutator[*TestSpec] {
	return NewMutator[*TestSpec, string](BarAccessor, v)
}

type idFactory struct{}

func (*idFactory) New() int {
	return 99
}

type testMarshalUnmarshaller[S any] struct {
	mockErr          *mockErr
	marshalCallCount int
}

func (marshalUnmarshaller *testMarshalUnmarshaller[S]) Marshal(
	v S,
) ([]byte, error) {
	defer func() { marshalUnmarshaller.marshalCallCount++ }()
	mockErr := marshalUnmarshaller.mockErr
	if mockErr != nil &&
		mockErr.mockErrType == mockErrTypeMarshal &&
		marshalUnmarshaller.marshalCallCount == mockErr.errorOn {
		return nil, fmt.Errorf("%s", mockErr.msg)
	}
	return json.Marshal(v)
}

func (*testMarshalUnmarshaller[S]) Unmarshal(data []byte, v S) error {
	return json.Unmarshal(data, v)
}

func (*testMarshalUnmarshaller[S]) MarshalMutation(
	mutation *Mutation,
) ([]byte, error) {
	return json.Marshal(mutation)
}

func (*testMarshalUnmarshaller[S]) UnmarshalMutation(
	data []byte,
	v *Mutation,
) error {
	return json.Unmarshal(data, v)
}

type testUtil struct {
	file        *os.File
	fstlnstg    fstln.Storage
	stg         *storage[int, string, *TestSpec]
	test        *testing.T
	lines       []string
	filters     Matcher[*TestSpec]
	orderBys    []Lesser[*TestSpec]
	mutators    []Mutator[*TestSpec]
	bufferLen   int
	mockError   *mockErr
	expectError string
	expect      []*TestSpec
	expectLines []string
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

	util.stg = &storage[int, string, *TestSpec]{
		bufferLen:         bufferLen,
		concurrency:       2,
		createdAtAccessor: CreatedAtAccessor,
		factory:           &TestSpecFactory{},
		idAccessor:        IdAccessor,
		idFactory:         &idFactory{},
		nower:             &TestNower{},
		stg:               util.fstlnstg,
		marshalUnmarshaller: &testMarshalUnmarshaller[*TestSpec]{
			mockErr: util.mockError,
		},
		updatedAtAccessor: UpdatedAtAccessor,
	}

	if err = util.writeLines(util.lines); err != nil {
		return err
	}

	if err = fstlnstg.ResetScan(); err != nil {
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

func (util *testUtil) handleExpectError(err error) (done bool) {
	if util.expectError == "" && err != nil {
		util.test.Fatal(err)
		return true
	}

	if util.expectError == "" {
		return false
	}

	if err == nil {
		util.test.Errorf("expected an error but got nil")
	}

	if err.Error() != util.expectError {
		util.test.Errorf(
			"expected an error with message \n%s\n but got \n%s\n",
			util.expectError,
			err.Error(),
		)
	}

	return true
}

func (util *testUtil) expectSelect() {
	var (
		err    error
		result []*TestSpec
	)

	result, err = util.stg.NewSelectBuilder().
		Where(util.filters).
		OrderBy(util.orderBys...).
		Run()

	if done := util.handleExpectError(err); done {
		return
	}

	util.expectSpecs(result...)
}

func (util *testUtil) expectInsert() {
	var (
		err    error
		result *TestSpec
	)

	result, err = util.stg.NewInsertBuilder().
		Set(util.mutators...).
		Run()

	if done := util.handleExpectError(err); done {
		return
	}

	util.expectSpecs(result)

	util.handleExpectLines()
}

func (util *testUtil) expectDelete() {
	var (
		err    error
		result []*TestSpec
	)

	result, err = util.stg.NewDeleteBuilder().
		Where(util.filters).
		Run()

	Sort(result, OrderById)

	if done := util.handleExpectError(err); done {
		return
	}

	util.expectSpecs(result...)

	util.handleExpectLines()
}

func (util *testUtil) expectSpecs(got ...*TestSpec) {
	if !reflect.DeepEqual(got, util.expect) {
		util.test.Errorf(
			"expected select result to be \n%s\n but got \n%s\n",
			testSpecSliceString(util.expect),
			testSpecSliceString(got),
		)
	}
}

func (util *testUtil) handleExpectLines() {
	var (
		err          error
		gotBytes     []byte
		gotString    string
		expectString string
	)

	if gotBytes, err = ioutil.ReadFile("test.jsonl"); err != nil {
		util.test.Fatal(err)
	}

	gotString = string(gotBytes)
	expectString = fmt.Sprintf("%s\n", strings.Join(util.expectLines, "\n"))

	if gotString != expectString {
		util.test.Errorf(
			"expected file contents to be \n%s\n but got \n%s\n",
			expectString,
			gotString,
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
	if err = mock.handleMockError(mockErrTypeDelete); err != nil {
		return err
	}
	return mock.stg.Delete(pos)
}

func (mock *mockStg) Insert(
	line []byte,
) (pos fstln.Position, err error) {
	if err = mock.handleMockError(mockErrTypeInsert); err != nil {
		return pos, err
	}
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
	mockErrTypeMarshal
	mockErrTypeInsert
	mockErrTypeDelete
)
