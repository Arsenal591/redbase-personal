package extio

import (
	"errors"
	"io"
)

type BytesManager struct {
	internal []byte
	offset   int
}

func NewBytesManager(internal []byte) *BytesManager {
	return &BytesManager{
		internal: internal,
		offset:   0,
	}
}

func (m *BytesManager) Read(p []byte) (int, error) {
	if m.offset >= len(m.internal) {
		return 0, io.EOF
	}
	n := copy(p, m.internal[m.offset:])
	m.offset += n
	return n, nil
}

func (m *BytesManager) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(m.internal[m.offset:])
	m.offset += n
	return int64(n), err
}

func (m *BytesManager) Seek(offset int64, whence int) (int64, error) {
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

func (m *BytesManager) Write(data []byte) (int, error) {
	newOffset := m.offset + len(data)
	if newOffset >= len(m.internal) {
		return 0, io.EOF
	}
	copy(m.internal[m.offset:newOffset], data)
	m.offset = newOffset
	return len(data), nil
}

func (m *BytesManager) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(m.internal[m.offset:])
	m.offset += n
	return int64(n), err
}

func (m *BytesManager) Close() error {
	return nil
}

func (m *BytesManager) Clear() {
	for i := range m.internal {
		m.internal[i] = 0
	}
}
