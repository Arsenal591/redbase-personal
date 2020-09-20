package pagedfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"

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

func (page *BufferedPage) Print() {
	fmt.Println("----------------")
	nextStr := "nil"
	if page.next != nil {
		nextStr = strconv.Itoa(int(page.next.idx))
	}
	prevStr := "nil"
	if page.prev != nil {
		prevStr = strconv.Itoa(int(page.prev.idx))
	}
	fmt.Printf("Idx %d, prev %s, next %s.\n", page.idx, prevStr, nextStr)

	fmt.Printf("Dirty: %t, Pinned: %d\n", page.dirty, page.pinned)
	if page.fi == nil {
		fmt.Printf("File")
	} else {
		fmt.Printf("File: %s(%d), num: %d\n", page.fi.Name(), int(page.fi.Fd()), page.num)
	}
	fmt.Println("----------------")
}

// Creates a page handle for a given buffered page.
// It is allowed to create multiple page handles for the same page.
func (page *BufferedPage) clonePageHandle() *PageHandle {
	page.pinned += 1
	return &PageHandle{
		memBuffer: page.memBuffer,
		num:       page.num,
	}
}

// Set the page to a different file.
func (page *BufferedPage) setNewFile(fi *os.File, num TypePageNum) {
	page.fi = fi
	page.num = num
	page.pinned = 0
	page.dirty = false
	page.memBuffer.Clear()
}

// Read data from on-disk file into in-memory buffer.
func (page *BufferedPage) readFromDisk() error {
	var err error
	_, err = page.fi.Seek(int64(page.num*PageSize), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = page.memBuffer.Seek(int64(0), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.Copy(page.memBuffer, page.fi)
	return err
}

// Write data from in-memory buffer to on-disk file.
func (page *BufferedPage) writeToDisk() error {
	var err error
	_, err = page.fi.Seek(int64(page.num*PageSize), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = page.memBuffer.Seek(int64(0), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.Copy(page.fi, page.memBuffer)
	if err != nil {
		return err
	}
	page.dirty = false
	return nil
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

// Creates a new file with given filename.
// It will also write file header to the file.
func (bp *BufferPool) CreateFile(fileName string) error {
	fi, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer fi.Close()
	hdr := NewFileHeader()
	err = binary.Write(fi, RWBytesOrder, &hdr)
	if err != nil {
		return err
	}
	return nil
}

func (bp *BufferPool) DestroyFile(fileName string) error {
	return os.Remove(fileName)
}

// Reads a new file with given filename.
// It will first read the file header, obtaining all necessary information before returning the file handle.
func (bp *BufferPool) OpenFile(fileName string) (*FileHandler, error) {
	fi, err := os.OpenFile(fileName, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	hdr := &FileHeader{}
	err = binary.Read(fi, RWBytesOrder, hdr)
	if err != nil {
		fi.Close()
		return nil, err
	}
	return &FileHandler{
		hdr:     hdr,
		bufPool: bp,
		fi:      fi,
	}, nil
}

// Closes a given file handle.
// Before actually closes the file, it will first flush pages to disk.
func (bp *BufferPool) CloseFile(fh *FileHandler) error {
	err := bp.ReleasePages(fh.fi)
	if err != nil {
		return err
	}
	return fh.fi.Close()
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

// Move a page inside the used queue to the front of it.
func (bp *BufferPool) moveToHeadUsed(page *BufferedPage) {
	bp.removeUsed(page)
	bp.makeHeadUsed(page)
}

// Load a free page, including inserting the page into used queue and put the page into map.
func (bp *BufferPool) load(page *BufferedPage) {
	if bp.cache[page.fi] == nil {
		bp.cache[page.fi] = make(map[TypePageNum]*BufferedPage)
	}
	bp.cache[page.fi][page.num] = page
	bp.removeFree(page)
	bp.makeHeadUsed(page)
}

// Evict a used page, including removing the page from used queue and remove it from map.
func (bp *BufferPool) evict(page *BufferedPage) {
	delete(bp.cache[page.fi], page.num)
	if len(bp.cache[page.fi]) == 0 {
		delete(bp.cache, page.fi)
	}
	bp.removeUsed(page)
	bp.makeHeadFree(page)
}

// Find an available page.
// If there is any free page, return the first of it.
// Otherwise, find a page that is least frequently used, evict it, and returns that page.
// If no page is available (all pages have `pinned > 0`, then error `ErrNoAvailablePage` is returned.
// Note that this function does not marked the returned page as in-use.
func (bp *BufferPool) findAvailablePage() (*BufferedPage, error) {
	if bp.headFree != nil {
		return bp.headFree, nil
	}
	pos := bp.tailUsed
	for pos != nil {
		if pos.pinned == 0 {
			break
		}
		pos = pos.prev
	}
	if pos == nil {
		return nil, ErrNoAvailablePage
	}
	if pos.dirty {
		err := pos.writeToDisk()
		if err != nil {
			return nil, err
		}
	}
	bp.evict(pos)
	return pos, nil
}

// Acquires a page for given file and corresponding page number, and returns a `PageHandle` instance.
// If the page is already in cache, returns it directly.
// Otherwise, it first calls `findAvailablePage` to find an available page for it and loads data on disk to memory.
func (bp *BufferPool) getPage(file *os.File, num TypePageNum, unique bool) (*PageHandle, error) {
	if page, ok := bp.cache[file][num]; ok { // already in LRU cache
		if page.pinned > 0 && unique {
			return nil, ErrPageBeingUsed
		}
		bp.moveToHeadUsed(page)
		return page.clonePageHandle(), nil
	} else {
		page, err := bp.findAvailablePage()
		if err != nil {
			return nil, err
		}
		page.setNewFile(file, num)
		err = page.readFromDisk()
		if err != nil {
			return nil, err
		}
		bp.load(page)
		return page.clonePageHandle(), nil
	}
}

// Allocates a new page for given file and page number.
// If the page is already in cache, error `ErrPageAlreadyInBuffer` is returned.
func (bp *BufferPool) allocatePage(file *os.File, num TypePageNum) (*PageHandle, error) {
	if _, ok := bp.cache[file][num]; ok {
		return nil, ErrPageAlreadyInBuffer
	} else {
		page, err := bp.findAvailablePage()
		if err != nil {
			return nil, err
		}
		page.setNewFile(file, num)
		bp.load(page)
		return page.clonePageHandle(), nil
	}
}

// Marks a page as dirty.
// When a page is marked as dirty, BufferPool will flush the data to disk before evicting it from cache.
// If the page is not in cache, error `ErrPageNotInBuffer` is returned.
// If the page is not pinned(referenced), error `ErrPageNotInUse` is returned.
func (bp *BufferPool) markDirty(file *os.File, num TypePageNum) error {
	if page, ok := bp.cache[file][num]; !ok {
		return ErrPageNotInBuffer
	} else {
		if page.pinned == 0 {
			return ErrPageNotInUse
		} else {
			page.dirty = true
			bp.moveToHeadUsed(page)
			return nil
		}
	}
}

// Unpins a page. It will decrease the page's reference counter by 1.
// If the page is not in cache, error `ErrPageNotInBuffer` is returned.
// If the page is not pinned(referenced), error `ErrPageNotInUse` is returned.
func (bp *BufferPool) unpinPage(file *os.File, num TypePageNum) error {
	if page, ok := bp.cache[file][num]; !ok {
		return ErrPageNotInBuffer
	} else {
		if page.pinned == 0 {
			return ErrPageNotInUse
		} else {
			page.pinned -= 1
			return nil
		}
	}
}

// Releases all pages. It will flush all dirty pages of the file to disk.
func (bp *BufferPool) ReleasePages(file *os.File) error {
	for _, page := range bp.cache[file] {
		if page.pinned > 0 {
			return ErrPageBeingUsed
		}
		if page.dirty {
			err := page.writeToDisk()
			if err != nil {
				return err
			}
		}
		bp.evict(page)
	}
	return nil
}

func (bp *BufferPool) ForcePages(file *os.File) error {
	for _, page := range bp.cache[file] {
		if page.dirty {
			err := page.writeToDisk()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bp *BufferPool) Print() {
	nUsed := 0
	fmt.Println("Used list: ")
	pos := bp.headUsed
	for pos != nil {
		pos.Print()
		pos = pos.next
		nUsed += 1
	}
	fmt.Printf("Total used: %d pages.\n", nUsed)

	fmt.Println("Free list: ")
	pos = bp.headFree
	nFree := 0
	for pos != nil {
		fmt.Printf("%d ", pos.idx)
		pos = pos.next
		nFree++
	}
	fmt.Printf("\n")
}
