package fstln

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/yo3jones/stg/pkg/stg"
)

type TestUtil struct {
	Test           *testing.T
	Name           string
	Lines          []string
	File           *os.File
	Options        []Option
	MockHandle     *mockHandle
	MockError      *mockError
	Stg            *storage
	ReadBufferSize int
}

func NewTestUtil() *TestUtil {
	return &TestUtil{}
}

func (util *TestUtil) SetTest(test *testing.T) *TestUtil {
	util.Test = test
	return util
}

func (util *TestUtil) SetName(name string) *TestUtil {
	util.Name = name
	return util
}

func (util *TestUtil) SetLines(lines ...string) *TestUtil {
	util.Lines = lines
	return util
}

func (util *TestUtil) SetReadBufferSize(readBuffersize int) *TestUtil {
	util.ReadBufferSize = readBuffersize
	return util
}

func (util *TestUtil) SetOptions(options ...Option) *TestUtil {
	util.Options = options
	return util
}

func (util *TestUtil) SetMockError(mockError *mockError) *TestUtil {
	util.MockError = mockError
	return util
}

func (util *TestUtil) Setup() (*TestUtil, *storage, error) {
	var err error
	os.Remove(util.Name)

	err = os.WriteFile(util.Name, []byte(util.Join(util.Lines...)), 0666)
	if err != nil {
		util.Test.Fatal(err)
		return nil, nil, err
	}

	if util.File, err = os.OpenFile(util.Name, os.O_RDWR, 0666); err != nil {
		util.Test.Fatal(err)
		return nil, nil, err
	}

	util.MockHandle = &mockHandle{
		handle:    util.File,
		mockError: util.MockError,
	}

	if util.Stg, err = new(util.MockHandle, util.Options...); err != nil {
		return util, nil, err
	}

	if util.ReadBufferSize <= 0 {
		util.ReadBufferSize = 100
	}

	return util, util.Stg, nil
}

func (util *TestUtil) Teardown() {
	if util.File != nil {
		util.File.Close()
	}
	os.Remove(util.Name)
}

func (util *TestUtil) RealAll() (string, error) {
	var (
		lines []string
		err   error
	)
	if lines, err = util.ReadAllLines(); err != nil {
		return "", err
	}
	return strings.Join(lines, ""), nil
}

func (util *TestUtil) ReadAllLines() (lines []string, err error) {
	var line string

	lines = []string{}

	for err != io.EOF {
		if _, line, err = util.ReadLine(); err != nil && err != io.EOF {
			return nil, err
		}

		lines = append(lines, line)
	}

	return lines, nil
}

func (util *TestUtil) ReadAllCallback(
	callback func(pos Position, line string, stg *storage) error,
) (err error) {
	var (
		line string
		pos  Position
	)

	for err != io.EOF {
		if pos, line, err = util.ReadLine(); err != nil && err != io.EOF {
			return err
		}

		if err := callback(pos, line, util.Stg); err != nil {
			return err
		}
	}

	return nil
}

func (util *TestUtil) ReadLine() (pos Position, line string, err error) {
	var (
		buffer   = make([]byte, util.ReadBufferSize)
		isPrefix = true
		n        int
	)

	for isPrefix {
		pos, n, isPrefix, err = util.Stg.Read(buffer)
		if err != nil && err != io.EOF {
			return pos, "", err
		}

		line = fmt.Sprintf("%s%s", line, string(buffer[:n]))
	}

	return pos, line, err
}

func (util *TestUtil) ReadOutput() string {
	var (
		data []byte
		err  error
	)

	if data, err = ioutil.ReadFile(util.Name); err != nil {
		util.Test.Fatal(err)
	}

	return string(data)
}

func (*TestUtil) Join(lines ...string) string {
	return strings.Join(append(lines, ""), "\n")
}

type mockHandle struct {
	handle           stg.Handle
	mockError        *mockError
	readCallCount    int
	seekCallCount    int
	writeAtCallCount int
}

func (mock *mockHandle) Read(p []byte) (n int, err error) {
	if mock.shouldError(mockErrorTypeRead, mock.readCallCount) {
		return 0, fmt.Errorf(mock.mockError.msg)
	}
	mock.readCallCount++
	return mock.handle.Read(p)
}

func (mock *mockHandle) Seek(offset int64, whence int) (int64, error) {
	if mock.shouldError(mockErrorTypeSeek, mock.seekCallCount) {
		return 0, fmt.Errorf(mock.mockError.msg)
	}
	mock.seekCallCount++
	return mock.handle.Seek(offset, whence)
}

func (mock *mockHandle) WriteAt(p []byte, off int64) (n int, err error) {
	if mock.shouldError(mockErrorTypeWriteAt, mock.writeAtCallCount) {
		return 0, fmt.Errorf(mock.mockError.msg)
	}
	mock.writeAtCallCount++
	return mock.handle.WriteAt(p, off)
}

func (mock *mockHandle) shouldError(
	mockErrorType mockErrorType,
	callCallCount int,
) bool {
	if mock.mockError == nil {
		return false
	}
	if mock.mockError.errorType != mockErrorType {
		return false
	}
	if mock.mockError.errorOn != callCallCount {
		return false
	}
	return true
}

type mockError struct {
	errorType mockErrorType
	errorOn   int
	msg       string
}

type mockErrorType int

const (
	mockErrorTypeRead mockErrorType = iota + 1
	mockErrorTypeSeek
	mockErrorTypeWriteAt
)
