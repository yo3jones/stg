package fstln

import "io"

func (stg *storage) Insert(line []byte) (position Position, err error) {
	stg.writeLock.Lock()
	defer stg.writeLock.Unlock()

	context := newWriteLineContext(line)

	var (
		availablePosition Position
		n                 int
		off               int64
		positionAvailable bool
	)

	availablePosition, positionAvailable = stg.emptyLines.PopIf(
		func(v Position) bool {
			return v.Len >= context.effectiveLen
		},
	)

	if positionAvailable {
		off = int64(availablePosition.Offset)
	} else {
		off = stg.offsetEnd
	}

	if n, err = context.writeAt(stg.handle, off); err != nil {
		return position, err
	}
	position = Position{
		Offset: int(off),
		Len:    n,
	}
	if !positionAvailable {
		stg.offsetEnd += int64(n)
	}

	if positionAvailable && availablePosition.Len > n {
		stg.emptyLines.Push(Position{
			Offset: availablePosition.Offset + n,
			Len:    availablePosition.Len - n,
		})
	}

	return position, nil
}

type writeLineContext struct {
	effectiveLen       int
	hasTrailingNewLine bool
	line               []byte
}

func newWriteLineContext(line []byte) *writeLineContext {
	var (
		effectiveLen       int
		hasTrailingNewLine bool
		lineLen            int
	)

	lineLen = len(line)
	if lineLen <= 0 {
		effectiveLen = lineLen + 1
		hasTrailingNewLine = false
	} else if line[lineLen-1] != '\n' {
		effectiveLen = lineLen + 1
		hasTrailingNewLine = false
	} else {
		effectiveLen = lineLen
		hasTrailingNewLine = true
	}

	return &writeLineContext{
		effectiveLen:       effectiveLen,
		hasTrailingNewLine: hasTrailingNewLine,
		line:               line,
	}
}

func (context *writeLineContext) writeAt(
	writer io.WriterAt,
	off int64,
) (n int, err error) {
	if n, err = writer.WriteAt(context.line, off); err != nil {
		return 0, err
	}

	if context.hasTrailingNewLine {
		return n, nil
	}

	if _, err = writer.WriteAt([]byte{'\n'}, off+int64(n)); err != nil {
		return n, err
	}

	return n + 1, nil
}
