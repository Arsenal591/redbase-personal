package pagedfile

import (
	"encoding/binary"
	"os"
	"pkg/extio"
)

const (
	NonExistPageNum   = -1
	FileHeaderPageNum = 0
)

// FileHeader always lies on the first page of a file, providing necessary page information.
type FileHeader struct {
	FirstFreePage int32 // Page number of a file's first free page.
	NumPages      int32 // Number of pages (including header page)
}

func NewFileHeader() *FileHeader {
	return &FileHeader{
		FirstFreePage: NonExistPageNum,
		NumPages:      1,
	}
}

type FileHeaderMgr struct {
	hdr *FileHeader
	io  extio.BytesIO
}

func NewFileHeaderMgr(io extio.BytesIO) (*FileHeaderMgr, error) {
	hdr := &FileHeader{}
	err := binary.Read(io, RWBytesOrder, hdr)
	if err != nil {
		return nil, err
	}
	return &FileHeaderMgr{
		hdr: hdr,
		io:  io,
	}, nil
}

type FileHandler struct {
	hdrMgr *FileHeaderMgr

	bufPool *BufferPool
	fi      *os.File
}

func NewFileHandler(fi *os.File, pool *BufferPool) (*FileHandler, error) {
	page, err := pool.getPage(fi, FileHeaderPageNum, false)
	if err != nil {
		return nil, err
	}
	hdrMgr, err := NewFileHeaderMgr(page.memBuffer)
	if err != nil {
		return nil, err
	}
	return &FileHandler{
		hdrMgr:  hdrMgr,
		bufPool: pool,
		fi:      fi,
	}, nil

}

func (fh *FileHandler) Close() error {
	err := fh.bufPool.CloseFile(fh)
	if err != nil {
		return err
	}
	fh.hdrMgr = nil
	fh.bufPool = nil
	fh.fi = nil
	return nil
}
