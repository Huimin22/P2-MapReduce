package pfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"unicode/utf8"
)

type RequestType uint8

const (
	ReadPart RequestType = iota
	WritePart
)

type pfileRequest struct {
	rangeStart int64
	rangeEnd   int64
	data       []byte
	response   chan any
}

type PFile struct {
	file        *os.File
	request     chan pfileRequest
	reqType     RequestType
	startDaemon func()
}

func NewPFile(filename string, reqType RequestType) (*PFile, error) {
	var file *os.File
	var err error
	if reqType == ReadPart {
		file, err = os.Open(filename)
	} else {
		file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	}
	if err != nil {
		return nil, err
	}
	p := &PFile{
		file:    file,
		request: make(chan pfileRequest, 1),
		reqType: reqType,
	}
	p.startDaemon = sync.OnceFunc(func() { go p.daemon() })
	return p, nil
}

func (p *PFile) Close() error {
	close(p.request)
	return p.file.Close()
}

func (p *PFile) daemon() {
	// read the file from rangeStart to rangeEnd
	for request := range p.request {
		if p.reqType == WritePart {
			_, err := p.file.Seek(request.rangeStart, 0)
			if err != nil {
				request.response <- err
			}
			_, err = p.file.Write(request.data)
			request.response <- err
		} else {
			_, err := p.file.Seek(request.rangeStart, 0)
			if err != nil {
				request.response <- err
			}

			bytes := make([]byte, request.rangeEnd-request.rangeStart)
			n, err := io.ReadFull(p.file, bytes)
			if err != nil {
				request.response <- err
			}
			if n != int(request.rangeEnd-request.rangeStart) {
				request.response <- io.EOF
			}
			request.response <- bytes
		}
	}
}

func (p *PFile) WritePart(rangeStart int64, rangeEnd int64, bytes []byte) error {
	if p.reqType != WritePart {
		return errors.New("pfile is not in writePart mode")
	}
	// check if the range is not satisfied with the bytes
	if len(bytes) != int(rangeEnd-rangeStart) {
		return fmt.Errorf("bytes length (%d) is not equal to rangeEnd-rangeStart (%d)", len(bytes), rangeEnd-rangeStart)
	}
	p.startDaemon()
	request := pfileRequest{
		rangeStart: rangeStart,
		rangeEnd:   rangeEnd,
		data:       bytes,
		response:   make(chan any, 1),
	}
	p.request <- request
	resp := <-request.response
	if resp == nil {
		return nil
	}
	return resp.(error)
}

func (p *PFile) Stat() (os.FileInfo, error) {
	return p.file.Stat()
}

func (p *PFile) ReadPart(rangeStart int64, rangeEnd int64) ([]byte, error) {
	if p.reqType != ReadPart {
		return nil, errors.New("pfile is not in readPart mode")
	}
	p.startDaemon()
	// read the file from rangeStart to rangeEnd
	request := pfileRequest{
		rangeStart: rangeStart,
		rangeEnd:   rangeEnd,
		response:   make(chan any, 1),
	}
	p.request <- request
	resp := <-request.response
	if bytes, ok := resp.([]byte); ok {
		return bytes, nil
	}
	return nil, resp.(error)
}

// isTextFile checks if the beginning of the file looks like text.
// It reads up to 1024 bytes and checks for null bytes or invalid UTF-8 sequences.
func IsTextFile(filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Read up to 1024 bytes
	buffer := make([]byte, 1024)
	n, err := f.Read(buffer)
	if err != nil && err != io.EOF {
		return false, err
	}
	buffer = buffer[:n]

	if n == 0 {
		return true, nil // Empty file is considered text
	}

	// Check for null bytes (common in binary files)
	for _, b := range buffer {
		if b == 0 {
			return false, nil
		}
	}

	// Check if it's valid UTF-8
	if !utf8.Valid(buffer) {
		// Simple check for high proportion of non-ASCII might be needed
		// but for now, just invalid UTF-8 suggests non-text
		// Consider a more sophisticated check if needed (e.g., http.DetectContentType)
		return false, nil
	}

	// Could add more checks here (e.g., control characters)

	return true, nil
}
