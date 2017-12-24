// DSWALWriter is a write to persist the data store change
// log to the file system.
package dsio

import (
	"os"
	"bufio"
	"bytes"
	"strings"
	"strconv"
	"fmt"
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
	RFH *os.File
	MemTable map[string]string
}

// Return a new DSWALRecordWriter instance
func New() *DSWALRecordWriter  {
	fh, err := os.Create("dat")
	rfh, rErr := os.Open("dat")
	if err != nil || rErr != nil {
		panic(err)
		panic(rErr)
	}

	return &DSWALRecordWriter{
		CurSegWALOffset: 0,
		CurSegRecordOffset: 0,
		FH: fh,
		RFH: rfh,
		MemTable: map[string]string{},
	}
}

// The method is to persist the WAL in the file system
func (dsw *DSWALRecordWriter) WriteRecord(dsLogRecord *DSLogRecord) (int64, error) {
	fmt.Println(dsw.FH == nil)
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

	buffer = bytes.NewBufferString(strconv.FormatInt(dsw.CurSegRecordOffset,10))
	buffer.WriteString("#")
	buffer.WriteString(strconv.FormatInt(int64(len(dsLogRecord.RecordKey)),10))

	dsw.MemTable[dsLogRecord.RecordKey] = buffer.String()

	fmt.Printf("MemTable: %v", dsw.MemTable)

	return dsw.CurSegRecordOffset, nil
}

// This method is to read the stored record from the file system
func (dsw *DSWALRecordWriter) ReadRecord(dsLogRecord *DSLogRecord) (string, error) {
	dataRefInfo := strings.Split(dsw.MemTable[dsLogRecord.RecordKey], "#")
	dataStartIdx, _ := strconv.ParseInt(dataRefInfo[0], 10, 64)
	dataLen, _ := strconv.Atoi(dataRefInfo[1])
	_, err := dsw.RFH.Seek(dataStartIdx, 0)
	if err != nil {
		panic(err)
	}

	bts := make([]byte, dataLen)
	dsw.RFH.Read(bts)
	s := string(bts)
	fmt.Printf("Query %s at position %d is %s", dsLogRecord.RecordKey, dataStartIdx, s)

	return s, nil
}

func (dsw *DSWALRecordWriter) Stop() {
	dsw.FH.Close()
	dsw.RFH.Close()
}
