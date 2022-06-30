package fstln

import (
	"io"
)

func (stg *storage) Delete(pos Position) (err error) {
	stg.writeLock.Lock()
	defer stg.writeLock.Unlock()

	return stg.delete(pos)
}

func (stg *storage) Insert(line []byte) (position Position, err error) {
	stg.writeLock.Lock()
	defer stg.writeLock.Unlock()

	context := newWriteLineContext(line)

	return stg.insertUnsafe(context)
}

func (stg *storage) Update(
	pos Position,
	line []byte,
) (afterPos Position, err error) {
	stg.writeLock.Lock()
	defer stg.writeLock.Unlock()

	context := newWriteLineContext(line)

	if context.effectiveLen <= pos.Len {
		return stg.updateInplace(pos, context)
	} else {
		return stg.updateOutOfPlace(pos, context)
	}
}

func (stg *storage) append(
	context *writeLineContext,
) (afterPos Position, err error) {
	var n int

	if n, err = context.writeAt(stg.handle, stg.offsetEnd); err != nil {
		return afterPos, err
	}

	afterPos = Position{
		Offset: int(stg.offsetEnd),
		Len:    n,
	}

	stg.offsetEnd += int64(n)

	return afterPos, nil
}

func (stg *storage) delete(pos Position) (err error) {
	deleteBuffer := make([]byte, pos.Len)

	for i := 0; i < pos.Len-1; i++ {
		deleteBuffer[i] = ' '
	}
	deleteBuffer[pos.Len-1] = '\n'

	_, err = stg.handle.WriteAt(deleteBuffer, int64(pos.Offset))
	if err != nil {
		return err
	}

	return nil
}

func (stg *storage) insertOnBlank(
	pos Position,
	context *writeLineContext,
) (afterPos Position, err error) {
	var n int

	if n, err = context.writeAt(stg.handle, int64(pos.Offset)); err != nil {
		return afterPos, err
	}

	if pos.Len > n {
		stg.emptyLines.Push(Position{
			Offset: pos.Offset + n,
			Len:    pos.Len - n,
		})
	}

	afterPos = Position{
		Offset: pos.Offset,
		Len:    n,
	}

	return afterPos, nil
}

func (stg *storage) insertUnsafe(
	context *writeLineContext,
) (position Position, err error) {
	var (
		availablePosition Position
		positionAvailable bool
	)

	availablePosition, positionAvailable = stg.emptyLines.PopIf(
		func(v Position) bool {
			return v.Len >= context.effectiveLen
		},
	)

	if positionAvailable {
		return stg.insertOnBlank(availablePosition, context)
	} else {
		return stg.append(context)
	}
}

func (stg *storage) updateInplace(
	pos Position,
	context *writeLineContext,
) (afterPos Position, err error) {
	var n int

	if n, err = context.writeAt(stg.handle, int64(pos.Offset)); err != nil {
		return afterPos, err
	}

	afterPos = Position{
		Offset: pos.Offset,
		Len:    n,
	}

	if afterPos.Len >= pos.Len {
		return afterPos, nil
	}

	err = stg.delete(Position{
		Offset: pos.Offset + n,
		Len:    pos.Len - n,
	})
	if err != nil {
		return afterPos, err
	}

	return afterPos, nil
}

func (stg *storage) updateOutOfPlace(
	pos Position,
	context *writeLineContext,
) (afterPos Position, err error) {
	if afterPos, err = stg.insertUnsafe(context); err != nil {
		return afterPos, err
	}

	if err = stg.delete(pos); err != nil {
		return afterPos, err
	}

	return afterPos, nil
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
