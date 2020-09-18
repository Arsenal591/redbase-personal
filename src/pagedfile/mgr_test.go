package pagedfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
