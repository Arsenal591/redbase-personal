package extio

import (
	"errors"
	"io"
)

type BytesIO interface {
	io.ReadWriteSeeker
	io.Closer
	io.ReaderAt
	io.WriterAt
	Clear()
}

type basicBytesIO struct {
	internal []byte
	offset   int
}

func NewBasicBytesIO(internal []byte) *basicBytesIO {
	return &basicBytesIO{
		internal: internal,
		offset:   0,
	}
}

func (m *basicBytesIO) Read(p []byte) (int, error) {
	if m.offset >= len(m.internal) {
		return 0, io.EOF
	}
	n := copy(p, m.internal[m.offset:])
	m.offset += n
	return n, nil
}

func (m *basicBytesIO) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(m.internal[m.offset:])
	m.offset += n
	return int64(n), err
}

func (m *basicBytesIO) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
		m.offset = int(offset)
	} else if whence == io.SeekCurrent {
		m.offset += int(offset)
	} else if whence == io.SeekEnd {
		newOffset := len(m.internal) - int(offset)
		if newOffset < 0 {
			return 0, errors.New("Cannot seek to negative position")
		}
		m.offset = newOffset
	} else {
		return 0, errors.New("Unknown whence parameter")
	}
	return int64(m.offset), nil
}

func (m *basicBytesIO) Write(data []byte) (int, error) {
	if m.offset >= len(m.internal) {
		return 0, io.EOF
	}
	n := copy(m.internal[m.offset:], data)
	m.offset += n
	return n, nil
}

func (m *basicBytesIO) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(m.internal[m.offset:])
	m.offset += n
	return int64(n), err
}

func (m *basicBytesIO) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || int(offset) >= len(m.internal) {
		return 0, io.EOF
	}
	n := copy(p, m.internal[offset:])
	return n, nil
}

func (m *basicBytesIO) WriteAt(p []byte, offset int64) (int, error) {
	if offset < 0 || int(offset) >= len(m.internal) {
		return 0, io.EOF
	}
	n := copy(m.internal[offset:], p)
	return n, nil
}

func (m *basicBytesIO) Close() error {
	return nil
}

func (m *basicBytesIO) Clear() {
	for i := range m.internal {
		m.internal[i] = 0
	}
}
