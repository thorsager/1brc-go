package main

import (
	"bytes"
	"fmt"
	"io"
)

// NoCopyFileReader is a file reader that does not copy the buffer
type NoCopyFileReader struct {
	//sync.Mutex
	r         io.Reader
	leftover  []byte
	delimiter byte
}

func NewNoCopyFileReader(r io.Reader, delimiter byte) *NoCopyFileReader {
	return &NoCopyFileReader{r: r, delimiter: delimiter}
}

func (ncfr *NoCopyFileReader) Read(d []byte) (int, error) {
	offset := 0
	if len(ncfr.leftover) > 0 {
		copy(d, ncfr.leftover)
		offset = len(ncfr.leftover)
		ncfr.leftover = nil
	}

	n, err := ncfr.r.Read(d[offset : len(d)-maxSegmentLength])
	if err != nil {
		return n, err
	}
	n += offset

	if d[n-1] != ncfr.delimiter { // not complete line
		// find the last delimiter
		lastDelimiter := bytes.LastIndexByte(d[:n], ncfr.delimiter)
		if lastDelimiter == -1 {
			return n, fmt.Errorf("'%s' delimiter not found in the buffer", string(ncfr.delimiter))
		}
		ncfr.leftover = make([]byte, n-lastDelimiter-1)
		copy(ncfr.leftover, d[lastDelimiter+1:n]) // copy the leftover
		n = lastDelimiter + 1
		d = d[0:n]
	}
	return n, nil
}

func (ncfr *NoCopyFileReader) ChunkToChan(bufferSize int, out chan []byte) error {
	buf := make([]byte, bufferSize) // +maxSegmentLength to have space for the leftover
	for {
		n, err := ncfr.Read(buf)
		if n == 0 {
			break
		}
		if err != nil {
			return err
		}
		toSend := make([]byte, n)
		copy(toSend, buf[:n])
		out <- toSend
	}
	return nil
}
