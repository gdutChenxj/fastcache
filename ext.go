package fastcache

import (
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

func (c *Cache) Write(k []byte, dst uint64, data []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_ = c.buckets[idx].Write(k, h, dst, data)
}

func (b *bucket) Write(k []byte, h uint64, dst uint64, data []byte) error {
	atomic.AddUint64(&b.getCalls, 1)
	chunks := b.chunks
	b.mu.Lock()
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen

				for i := 0; i < len(data); i++ {
					chunk[idx+dst+uint64(i)] = data[i]
				}

			} else {
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
end:
	b.mu.Unlock()
	return nil
}
