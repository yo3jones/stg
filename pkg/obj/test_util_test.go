package obj

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yo3jones/stg/pkg/fstln"
	"github.com/yo3jones/stg/pkg/objbinlog"
)

type testUtil struct {
	file         *os.File
	logFile      *os.File
	fstlnstg     fstln.Storage
	binLogStg    objbinlog.BinLogStorage
	stg          *storage[int, *TestSpec]
	test         *testing.T
	lines        []string
	filters      Matcher[*TestSpec]
	orderBys     []Lesser[*TestSpec]
	mutators     []Mutator[*TestSpec]
	bufferLen    int
	mockError    *mockErr
	expectError  string
	expect       []*TestSpec
	expectLines  [][]string
	expectBinLog [][]string
}

func (util *testUtil) setup() (err error) {
	os.Remove("test.jsonl")
	os.Remove("test_log.jsonl")

	if util.file, err = os.Create("test.jsonl"); err != nil {
		return err
	}

	if util.logFile, err = os.Create("test_log.jsonl"); err != nil {
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

	util.binLogStg = objbinlog.New[int](
		util.logFile,
		&testIdFactory{200},
		&testMarshalUnmarshaller[any]{},
		objbinlog.OptNower{Value: &TestNower{}},
	)

	util.stg = &storage[int, *TestSpec]{
		binLogStg:         &mockBinLogStroage{util.binLogStg, util.mockError},
		bufferLen:         bufferLen,
		concurrency:       2,
		createdAtAccessor: CreatedAtAccessor,
		factory:           &TestSpecFactory{},
		idAccessor:        IdAccessor,
		idFactory:         &testIdFactory{100},
		nower:             &TestNower{},
		objType:           "test",
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
	if util.logFile != nil {
		util.logFile.Close()
	}
	os.Remove("test.jsonl")
	os.Remove("test_log.jsonl")
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
		return true
	}

	if err.Error() != util.expectError {
		util.test.Errorf(
			"expected an error with message \n%s\n but got \n%s\n",
			util.expectError,
			err.Error(),
		)
		return true
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

	util.handleExpectBinLog()
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

	util.handleExpectBinLog()
}

func (util *testUtil) expectUpdate() {
	var (
		err    error
		result []*TestSpec
	)

	result, err = util.stg.NewUpdateBuilder().
		Where(util.filters).
		Set(util.mutators...).
		OrderBy(util.orderBys...).
		Run()

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
		found        bool
		gotBytes     []byte
		gotString    string
		expectString string
	)

	if gotBytes, err = ioutil.ReadFile("test.jsonl"); err != nil {
		util.test.Fatal(err)
	}

	gotString = string(gotBytes)
	for _, expectLines := range util.expectLines {
		expectString = fmt.Sprintf("%s\n", strings.Join(expectLines, "\n"))
		if gotString == expectString {
			found = true
			break
		}
	}

	if !found {
		expectString = fmt.Sprintf(
			"%s\n",
			strings.Join(util.expectLines[0], "\n"),
		)
		util.test.Errorf(
			"expected file contents to be \n%s\n but got \n%s\n",
			expectString,
			gotString,
		)
	}
}

func (util *testUtil) handleExpectBinLog() {
	var (
		err      error
		expect   string
		found    bool
		got      string
		gotBytes []byte
	)

	if gotBytes, err = ioutil.ReadFile("test_log.jsonl"); err != nil {
		util.test.Fatal(err)
	}

	got = string(gotBytes)
	for _, expectLines := range util.expectBinLog {
		expect = fmt.Sprintf("%s\n", strings.Join(expectLines, "\n"))
		if got == expect {
			found = true
			break
		}
	}

	if !found {
		expect = fmt.Sprintf("%s\n", strings.Join(util.expectBinLog[0], "\n"))
		util.test.Errorf(
			"expected log file to contain \n%s\n but got \n%s\n",
			expect,
			got,
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
	if err = mock.handleMockError(mockErrTypeUpdate); err != nil {
		return afterPos, err
	}
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

type mockBinLogStroage struct {
	stg     objbinlog.BinLogStorage
	mockErr *mockErr
}

func (mock *mockBinLogStroage) StartTransaction(
	objType string,
) objbinlog.Transaction {
	return &mockTransaction{
		transaction: mock.stg.StartTransaction(objType),
		mockErr:     mock.mockErr,
	}
}

type mockTransaction struct {
	transaction objbinlog.Transaction
	mockErr     *mockErr
	callCount   int
}

func (mock *mockTransaction) End() {
	mock.transaction.End()
}

func (mock *mockTransaction) LogDelete(id any, from []byte) (err error) {
	defer func() { mock.callCount++ }()
	if mock.mockErr != nil &&
		mock.mockErr.mockErrType == mockErrTypeBinLog &&
		mock.mockErr.errorOn == mock.callCount {
		return fmt.Errorf("%s", mock.mockErr.msg)
	}
	return mock.transaction.LogDelete(id, from)
}

func (mock *mockTransaction) LogInsert(id any, to []byte) (err error) {
	defer func() { mock.callCount++ }()
	if mock.mockErr != nil &&
		mock.mockErr.mockErrType == mockErrTypeBinLog &&
		mock.mockErr.errorOn == mock.callCount {
		return fmt.Errorf("%s", mock.mockErr.msg)
	}
	return mock.transaction.LogInsert(id, to)
}

func (mock *mockTransaction) LogUpdate(id any, from, to []byte) (err error) {
	defer func() { mock.callCount++ }()
	if mock.mockErr != nil &&
		mock.mockErr.mockErrType == mockErrTypeBinLog &&
		mock.mockErr.errorOn == mock.callCount {
		return fmt.Errorf("%s", mock.mockErr.msg)
	}
	return mock.transaction.LogUpdate(id, from, to)
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
	mockErrTypeUpdate
	mockErrTypeBinLog
)
