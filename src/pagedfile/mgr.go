package pagedfile

import (
	"pkg/extio"
)
type TypePageNum int // page's num of corresponding file
type TypePoolIdx int // page's location in buffer pool

type BufferedPage struct {
	memBuffer *extio.BytesManager // internal memory manager, handles bytes data
	idx       TypePoolIdx         // page's idx
	num       TypePageNum         // page's num
	next      *BufferedPage       // next page in LRU queue
	prev      *BufferedPage       // prev page in LRU queue
	dirty     bool                // whether there is un-flushed data in memory
	pinned    int                 // reference num of this page
	fi        *os.File            // underlying file handler
}

type BufferPool struct {
	cache    map[*os.File]map[TypePageNum]*BufferedPage // mapping from file and num to buffered page
	buffer   []*BufferedPage                            // LRU queue's container
	headUsed *BufferedPage                              // most recently used
	tailUsed *BufferedPage                              // least recently used
	headFree *BufferedPage                              // first unused page
}

// Creates a buffer pool instance with given size
func NewBufferPool(numPages int) *BufferPool {
	ret := &BufferPool{
		cache:    make(map[*os.File]map[TypePageNum]*BufferedPage),
		buffer:   make([]*BufferedPage, numPages),
		headUsed: nil,
		tailUsed: nil,
	}

	// Initialize LRU queue
	for i := 0; i < numPages; i++ {
		ret.buffer[i] = &BufferedPage{
			memBuffer: extio.NewBytesManager(make([]byte, PageSize)),
			idx:       TypePoolIdx(i),
		}
	}
	for i := 0; i < numPages; i++ {
		var next *BufferedPage
		if i+1 < numPages {
			next = ret.buffer[i+1]
		}
		var prev *BufferedPage
		if i > 0 {
			prev = ret.buffer[i-1]
		}
		ret.buffer[i].prev = prev
		ret.buffer[i].next = next
	}
	ret.headFree = ret.buffer[0]

	return ret
}

// Make a page the head of used LRU queue.
// Input argument `page` should not be already in the queue.
func (bp *BufferPool) makeHeadUsed(page *BufferedPage) {
	page.next = bp.headUsed
	if bp.headUsed != nil {
		bp.headUsed.prev = page
	}
	page.prev = nil
	if bp.tailUsed == nil {
		bp.tailUsed = page
	}
	bp.headUsed = page
}

// Make a page the head of free queue.
// Input argument `page` should not be already in the queue.
func (bp *BufferPool) makeHeadFree(page *BufferedPage) {
	page.next = bp.headFree
	if bp.headFree != nil {
		bp.headFree.prev = page
	}
	page.prev = nil
	bp.headFree = page
}

// Remove a page from the used LRU queue.
// Input argument `page` should be already in the queue.
func (bp *BufferPool) removeUsed(page *BufferedPage) {
	if page == bp.headUsed {
		bp.headUsed = page.next
	}
	if page == bp.tailUsed {
		bp.tailUsed = page.prev
	}
	next := page.next
	prev := page.prev
	page.next = nil
	if next != nil {
		next.prev = prev
	}
	page.prev = nil
	if prev != nil {
		prev.next = next
	}
}

// Remove a page from the used free queue.
// Input argument `page` should be already in the queue. (Normally, it should be the head of the queue.)
func (bp *BufferPool) removeFree(page *BufferedPage) {
	if page == bp.headFree {
		bp.headFree = page.next
	}
	next := page.next
	prev := page.prev
	page.next = nil
	if next != nil {
		next.prev = prev
	}
	page.prev = nil
	if prev != nil {
		prev.next = next
	}
}
