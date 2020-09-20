package pagedfile

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type LinkedListType int

const (
	UsedList LinkedListType = iota
	FreeList
)

func TestNewBufferPool(t *testing.T) {
	numPages := 4
	pool := NewBufferPool(numPages)
	assert.Equal(t, numPages, len(pool.buffer), "Length of pool buffer.")
	assert.Nil(t, pool.headUsed, "Head of used should be nil.")
	assert.Nil(t, pool.tailUsed, "Tail of used should be nil.")
	assert.Equal(t, pool.buffer[0], pool.headFree, "Head of free should be the first page")

	for i := 0; i < numPages; i++ {
		var prev, next *BufferedPage
		if i > 0 {
			prev = pool.buffer[i-1]
		}
		if i < numPages-1 {
			next = pool.buffer[i+1]
		}
		assert.Equal(t, prev, pool.buffer[i].prev, "Prev of page", i)
		assert.Equal(t, next, pool.buffer[i].next, "Next of page", i)
		assert.Equal(t, TypePoolIdx(i), pool.buffer[i].idx, "Index of page", i)
		assert.Equal(t, false, pool.buffer[i].dirty, "Page not dirty", i)
		assert.Equal(t, 0, pool.buffer[i].pinned, "Pinned of page", i)
		assert.Nil(t, pool.buffer[i].fi, "Not file set for page", i)
	}
}

func utilsMakeLinkedList(pool *BufferPool, idxs []TypePoolIdx, typ LinkedListType) {
	for i, idx := range idxs {
		var prev, next *BufferedPage
		if i > 0 {
			prev = pool.buffer[idxs[i-1]]
		}
		if i < len(idxs)-1 {
			next = pool.buffer[idxs[i+1]]
		}
		pool.buffer[idx].prev = prev
		pool.buffer[idx].next = next
	}

	if typ == UsedList {
		if len(idxs) > 0 {
			pool.headUsed = pool.buffer[idxs[0]]
			pool.tailUsed = pool.buffer[idxs[len(idxs)-1]]
		} else {
			pool.headUsed = nil
			pool.tailUsed = nil
		}
	} else {
		if len(idxs) > 0 {
			pool.headFree = pool.buffer[idxs[0]]
		} else {
			pool.headFree = nil
		}
	}
}

func utilsTestLinkedList(t *testing.T, pool *BufferPool, expected []TypePoolIdx, typ LinkedListType, desc ...string) {
	for i, idx := range expected {
		pos := pool.buffer[idx]
		assert.NotNil(t, pos, fmt.Sprintf("%dth node of expected list not nil.", i+1), desc)
		if i > 0 {
			assert.NotNil(t, pos.prev, fmt.Sprintf("%dth node's prev", i+1), desc)
			assert.Equal(t, expected[i-1], pos.prev.idx, fmt.Sprintf("%dth node's prev", i+1), desc)
		} else {
			assert.Nil(t, pos.prev, "1st node's prev", desc)
		}
		if i < len(expected)-1 {
			assert.NotNil(t, pos.next, fmt.Sprintf("%dth node's next", i+1), desc)
			assert.Equal(t, expected[i+1], pos.next.idx, fmt.Sprintf("%dth node's next", i+1), desc)
		} else {
			assert.Nil(t, pos.next, "Last node's prev", desc)
		}
		assert.Equal(t, idx, pos.idx, fmt.Sprintf("%dth node's idx", i+1), desc)
		pos = pos.next
	}
	if typ == UsedList {
		if len(expected) > 0 {
			assert.Equal(t, expected[0], pool.headUsed.idx, "head used", desc)
			assert.Equal(t, expected[len(expected)-1], pool.tailUsed.idx, "tail used", desc)
		} else {
			assert.Nil(t, pool.headUsed, "head used", desc)
			assert.Nil(t, pool.tailUsed, "tail used", desc)
		}
	} else {
		if len(expected) > 0 {
			assert.Equal(t, expected[0], pool.headFree.idx, "head free", desc)
		} else {
			assert.Nil(t, pool.headFree, "head free", desc)
		}
	}
}

func TestHeadUsed(t *testing.T) {
	testCases := []struct {
		original   []TypePoolIdx
		newHeadIdx TypePoolIdx
	}{
		{
			original:   []TypePoolIdx{0, 1, 3},
			newHeadIdx: 2,
		},
		{
			original:   []TypePoolIdx{3, 1},
			newHeadIdx: 0,
		},
		{
			original:   []TypePoolIdx{},
			newHeadIdx: 2,
		},
	}

	for _, tc := range testCases {
		// prepare
		numPages := 4
		pool := NewBufferPool(numPages)
		utilsMakeLinkedList(pool, tc.original, UsedList)

		// test
		pool.makeHeadUsed(pool.buffer[tc.newHeadIdx])
		utilsTestLinkedList(t, pool, append([]TypePoolIdx{tc.newHeadIdx}, tc.original...), UsedList, "used list")
	}
}

func TestMakeHeadFree(t *testing.T) {
	testCases := []struct {
		original   []TypePoolIdx
		newHeadIdx TypePoolIdx
	}{
		{
			original:   []TypePoolIdx{0, 1, 3},
			newHeadIdx: 2,
		},
		{
			original:   []TypePoolIdx{3, 1},
			newHeadIdx: 0,
		},
		{
			original:   []TypePoolIdx{},
			newHeadIdx: 2,
		},
	}

	for _, tc := range testCases {
		// prepare
		numPages := 4
		pool := NewBufferPool(numPages)
		utilsMakeLinkedList(pool, tc.original, FreeList)

		// test
		pool.makeHeadFree(pool.buffer[tc.newHeadIdx])

		utilsTestLinkedList(t, pool, append([]TypePoolIdx{tc.newHeadIdx}, tc.original...), FreeList, "free list")
	}
}

func TestRemoveUsed(t *testing.T) {
	testCases := []struct {
		original []TypePoolIdx
		removed  TypePoolIdx
		expected []TypePoolIdx
	}{
		{
			original: []TypePoolIdx{0, 1, 2},
			removed:  1,
			expected: []TypePoolIdx{0, 2},
		},
		{
			original: []TypePoolIdx{0, 1, 2},
			removed:  2,
			expected: []TypePoolIdx{0, 1},
		},
		{
			original: []TypePoolIdx{0},
			removed:  0,
			expected: []TypePoolIdx{},
		},
	}

	for _, tc := range testCases {
		// prepare
		numPages := 4
		pool := NewBufferPool(numPages)
		utilsMakeLinkedList(pool, tc.original, UsedList)

		// test
		pool.removeUsed(pool.buffer[tc.removed])
		utilsTestLinkedList(t, pool, tc.expected, UsedList, "used list")
	}
}

func TestRemoveFree(t *testing.T) {
	testCases := []struct {
		original []TypePoolIdx
		removed  TypePoolIdx
		expected []TypePoolIdx
	}{
		{
			original: []TypePoolIdx{0, 1, 2},
			removed:  1,
			expected: []TypePoolIdx{0, 2},
		},
		{
			original: []TypePoolIdx{0, 1, 2},
			removed:  2,
			expected: []TypePoolIdx{0, 1},
		},
		{
			original: []TypePoolIdx{0},
			removed:  0,
			expected: []TypePoolIdx{},
		},
	}

	for _, tc := range testCases {
		// prepare
		numPages := 4
		pool := NewBufferPool(numPages)
		utilsMakeLinkedList(pool, tc.original, UsedList)

		// test
		pool.removeFree(pool.buffer[tc.removed])
		utilsTestLinkedList(t, pool, tc.expected, FreeList, "free list")
	}
}

func TestFindAvaiablePage(t *testing.T) {
	testCases := []struct {
		originalUsed []TypePoolIdx
		pinned       []int
		originalFree []TypePoolIdx
		err          error
		evicted      bool
		expectedIdx  TypePoolIdx
		desc         string
	}{
		{
			originalUsed: []TypePoolIdx{0, 1},
			pinned:       []int{2, 2},
			originalFree: []TypePoolIdx{2, 3},
			err:          nil,
			evicted:      false,
			expectedIdx:  2,
			desc:         "Free page already",
		},
		{
			originalUsed: []TypePoolIdx{0, 1, 3, 2},
			pinned:       []int{0, 0, 0, 0},
			originalFree: []TypePoolIdx{},
			err:          nil,
			evicted:      true,
			expectedIdx:  2,
			desc:         "Have to evict",
		},
		{
			originalUsed: []TypePoolIdx{0, 1, 3, 2},
			pinned:       []int{0, 0, 3, 2},
			originalFree: []TypePoolIdx{},
			err:          nil,
			evicted:      true,
			expectedIdx:  1,
			desc:         "Have to evict, with some pages have pinned > 0",
		},
		{
			originalUsed: []TypePoolIdx{0, 1, 3, 2},
			pinned:       []int{4, 3, 3, 2},
			originalFree: []TypePoolIdx{},
			err:          ErrNoAvailablePage,
			evicted:      false,
			expectedIdx:  -1,
			desc:         "No availble page",
		},
	}

	for _, tc := range testCases {
		// prepare
		numPages := 4
		pool := NewBufferPool(numPages)
		utilsMakeLinkedList(pool, tc.originalUsed, UsedList)
		utilsMakeLinkedList(pool, tc.originalFree, FreeList)

		for i, idx := range tc.originalUsed {
			pool.buffer[idx].fi = &os.File{}
			pool.buffer[idx].num = 0
			pool.buffer[idx].pinned = tc.pinned[i]
		}

		// test
		page, err := pool.findAvailablePage()
		assert.Equal(t, tc.err, err, "error", tc.desc)
		if tc.err == nil {
			assert.Equal(t, tc.expectedIdx, page.idx, "returned index", tc.desc)
			_, ok := pool.cache[page.fi][page.num]
			assert.Equal(t, false, ok, "returned page is no longer in cache")

			if !tc.evicted {
				utilsTestLinkedList(t, pool, tc.originalFree, FreeList, "if not evicted, free list should not be modified", tc.desc)
				utilsTestLinkedList(t, pool, tc.originalUsed, UsedList, "if not evicted, used list should not be modified", tc.desc)
			} else {
				utilsTestLinkedList(t, pool, append([]TypePoolIdx{tc.expectedIdx}, tc.originalFree...), FreeList, "free list", "if evicted, free head is changed", tc.desc)
				expectedUsed := make([]TypePoolIdx, 0)
				for _, idx := range tc.originalUsed {
					if idx != tc.expectedIdx {
						expectedUsed = append(expectedUsed, idx)
					}
				}
				utilsTestLinkedList(t, pool, expectedUsed, UsedList, "free list", "if evicted, free head is changed", tc.desc)
			}
		} else {
			utilsTestLinkedList(t, pool, tc.originalUsed, UsedList, "on error, used list should not be modified", tc.desc)
			utilsTestLinkedList(t, pool, tc.originalFree, FreeList, "on error, free list should not be modified", tc.desc)
		}
	}
}
