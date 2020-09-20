package pagedfile

import "encoding/binary"

const (
	PageSize = 4096
)

var (
	RWBytesOrder = binary.BigEndian
)
