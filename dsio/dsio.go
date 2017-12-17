// DSWALWriter is a write to persist the data store change
// log to the file system.
package dsio

import (
	"os"
	"bufio"
	"bytes"
)

// A WAL record to persist in the fs
type DSLogRecord struct {
	RecordKey string
	RecordVal string
}

type DSWALRecordWriter struct {
	CurSegWALOffset int64
	CurSegRecordOffset int64
	FH *os.File
}

// Return a new DSWALRecordWriter instance
func New() *DSWALRecordWriter  {
	fh, err := os.Create("dat")
	if err != nil {
		panic(err)
	}

	return &DSWALRecordWriter{
		CurSegWALOffset: 0,
		CurSegRecordOffset: 0,
		FH: fh,
	}
}

// The method is to persist the WAL in the file system
func (dsw *DSWALRecordWriter) WriteRecord(dsLogRecord *DSLogRecord) (int64, error) {
	bufFileHandler := bufio.NewWriter(dsw.FH)

	buffer := bytes.NewBufferString(dsLogRecord.RecordKey)
	buffer.WriteString(",")
	buffer.WriteString(dsLogRecord.RecordVal)
	buffer.WriteString("\n")

	kn, err := bufFileHandler.Write(buffer.Bytes())
	if err != nil {
		panic(err)
		return dsw.CurSegRecordOffset, err
	}

	dsw.CurSegRecordOffset += dsw.CurSegWALOffset + int64(len(dsLogRecord.RecordKey)) + 1
	dsw.CurSegWALOffset += int64(kn)

	bufFileHandler.Flush()

	return dsw.CurSegRecordOffset, nil
}

func (dsw *DSWALRecordWriter) Stop() {
	dsw.FH.Close()
}
