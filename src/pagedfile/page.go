package pagedfile

import "pkg/extio"

type PageHandle struct {
	memBuffer extio.BytesIO
	num       TypePageNum
}
