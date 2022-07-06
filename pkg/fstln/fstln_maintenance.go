package fstln

func (stg *storage) Maintenance() (freed int, err error) {
	stg.readLock.Lock()
	defer stg.readLock.Unlock()
	stg.writeLock.Lock()
	defer stg.writeLock.Unlock()

	var (
		emptyLines     *Position
		n              int
		pos            Position
		readEmptyLines bool
	)

	if err = stg.resetScanUnsafe(); err != nil {
		return 0, err
	}

	for !stg.scanEof {
		stg.readPhase = phaseEmpty
		if pos, readEmptyLines, err = stg.readEmptyLines(); err != nil {
			return freed, err
		}

		if readEmptyLines && emptyLines == nil {
			emptyLines = &Position{Offset: pos.Offset, Len: pos.Len}
		} else if readEmptyLines && emptyLines != nil {
			emptyLines.Len += pos.Len
		}

		if stg.scanEof {
			break
		}

		stg.readPhase = phaseRead
		if pos, err = stg.fillLine(); err != nil {
			return freed, err
		}

		if emptyLines == nil {
			continue
		}

		n, err = stg.handle.WriteAt(stg.line, int64(emptyLines.Offset))
		if err != nil {
			return freed, err
		}

		emptyLines.Offset += n
	}

	if emptyLines == nil {
		return 0, nil
	}

	err = stg.handle.Truncate(stg.scanEnd - int64(emptyLines.Len))
	if err != nil {
		return freed, err
	}

	return emptyLines.Len, nil
}
