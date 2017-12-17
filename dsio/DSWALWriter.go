// DSWALWriter is a write to persist the data store change
// log to the file system.
package dsio

import (
	"os"
	"bufio"
	"bytes"
)

const (
	logFileName = "data/dat"
)

var curSegWALOffset int64 = 0
var curSegRecordOffset int64 = 0
var fh *os.File = nil

// A WAL record to persist in the fs
type DSLogRecord struct {
	recordKey string
	recordVal string
}

func init()  {
	var err error
	fh, err = os.Create(logFileName)
	if err != nil {
		panic(err)
	}
}

// The method is to persist the WAL in the file system
func WriteRecord(dsLogRecord *DSLogRecord) (int64, error) {
	bufFileHandler := bufio.NewWriter(fh)

	buffer := bytes.Buffer(dsLogRecord.recordKey)
	buffer.WriteString(",")
	buffer.WriteString(dsLogRecord.recordVal)
	buffer.WriteString("\n")

	kn, err := bufFileHandler.Write(buffer.Bytes())
	if err != nil {
		panic(err)
		return curSegRecordOffset, err
	}

	curSegRecordOffset += curSegWALOffset + int64(len(dsLogRecord.recordKey)) + 1
	curSegWALOffset += int64(kn)

	bufFileHandler.Flush()

	return curSegRecordOffset, nil
}
