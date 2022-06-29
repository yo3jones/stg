package fstln

import (
	"io"

	datastruc "github.com/yo3jones/datastruc/pkg"
)

func (stg *storage) Read(
	line []byte,
) (position Position, n int, isPrefix bool, err error) {
	stg.readLock.Lock()
	defer stg.readLock.Unlock()

	if stg.linePrefix {
		return stg.fillInputBuffer(line)
	}

	if err = stg.readLine(); err != nil {
		return position, 0, false, err
	}

	return stg.fillInputBuffer(line)
}

func (stg *storage) ResetScan() (err error) {
	var endOffset int64

	if endOffset, err = stg.handle.Seek(int64(0), io.SeekEnd); err != nil {
		return err
	}

	if _, err = stg.handle.Seek(int64(0), io.SeekStart); err != nil {
		return err
	}

	stg.bufferCurr = 0
	stg.bufferEof = false
	stg.bufferLen = 0
	stg.emptyLines = datastruc.NewHeapSize(PositionLesser, 100)
	stg.lineCurr = 0
	stg.linePrefix = false
	stg.scanEof = false
	stg.scanCurr = 0
	stg.scanEnd = endOffset

	return nil
}

func (stg *storage) fillBuffer() (err error) {
	var n int

	if n, err = stg.handle.Read(stg.buffer); err != nil && err != io.EOF {
		return err
	}

	stg.bufferCurr = 0
	stg.bufferEof = err == io.EOF
	stg.bufferLen = n

	return nil
}

func (stg *storage) fillInputBuffer(
	line []byte,
) (position Position, n int, isPrefix bool, err error) {
	n = 0
	for i := 0; stg.lineCurr < len(stg.line) && i < len(line); i++ {
		line[i] = stg.line[stg.lineCurr]
		stg.lineCurr++
		n++
	}

	isPrefix = stg.lineCurr < len(stg.line)
	if !isPrefix {
		stg.lineCurr = 0
	}
	stg.linePrefix = isPrefix
	err = nil

	if stg.scanEof {
		err = io.EOF
	}

	return stg.linePos, n, isPrefix, err
}

func (stg *storage) fillLine() (pos Position, err error) {
	shouldUpdateState := stg.readPhase != phaseEmpty
	pos = Position{
		Offset: stg.scanCurr,
		Len:    0,
	}

	if shouldUpdateState {
		stg.line = stg.line[:0]
	}

	for {
		if stg.bufferCurr >= stg.bufferLen && !stg.bufferEof {
			if err = stg.fillBuffer(); err != nil {
				return pos, err
			}
		}
		// should never get here
		// else if stg.bufferCurr >= stg.bufferLen {
		// 	break
		// }

		b := stg.buffer[stg.bufferCurr]
		if shouldUpdateState {
			stg.line = append(stg.line, b)
		}

		stg.bufferCurr++
		stg.scanCurr++
		pos.Len++

		if stg.scanCurr >= int(stg.scanEnd) {
			stg.scanEof = true
			break
		}

		if b == '\n' {
			break
		}
	}

	if shouldUpdateState {
		stg.linePos = pos
	}

	return pos, nil
}

func (stg *storage) handleEmptyLines() (err error) {
	var (
		b          byte
		groupedPos *Position
		peaked     bool
		pos        Position
	)
	for {
		if b, peaked, err = stg.peak(); err != nil {
			return err
		} else if !peaked {
			break
		}

		if b != ' ' {
			break
		}

		if pos, err = stg.fillLine(); err != nil {
			return err
		}

		if groupedPos == nil {
			groupedPos = &pos
		} else {
			groupedPos.Len += pos.Len
		}
	}

	if groupedPos == nil {
		return nil
	}

	stg.emptyLines.Push(*groupedPos)

	return nil
}

func (stg *storage) peak() (b byte, peaked bool, err error) {
	if stg.scanEof {
		return b, false, nil
	}

	if stg.bufferCurr >= stg.bufferLen && stg.bufferEof {
		// TODO coverage need to wait for write path
		return b, false, nil
	}

	if stg.bufferCurr >= stg.bufferLen {
		if err = stg.fillBuffer(); err != nil {
			return b, false, err
		}
	}

	return stg.buffer[stg.bufferCurr], true, nil
}

func (stg *storage) readLine() (err error) {
	stg.readPhase = phaseEmpty
	if err = stg.handleEmptyLines(); err != nil {
		return err
	}

	stg.readPhase = phaseRead
	if _, err = stg.fillLine(); err != nil {
		return err
	}

	stg.readPhase = phaseEmpty
	if err = stg.handleEmptyLines(); err != nil {
		return err
	}

	return nil
}