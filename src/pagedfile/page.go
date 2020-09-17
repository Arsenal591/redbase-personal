package pagedfile

import "pkg/extio"

type PageHandle struct {
	memBuffer *extio.BytesManager
	num       TypePageNum
}
