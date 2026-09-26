/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ByteBufferOutputStream} backed by a linked list of fixed-size chunks instead of a single
 * re-allocated buffer. Chunks are supplied by the caller (initial chunks via the constructor,
 * additional chunks via {@link #addBuffers(List)}).
 * <p>
 * The stream grows on its own: when a write runs past the attached chunks it attaches one more chunk,
 * taken from the pool without blocking and falling back to a heap-allocated chunk when the pool has no
 * remaining chunks in the middle of a write (a partially written record can neither be rolled back nor blocked on).
 * Heap-allocated chunks are tracked separately (in {@link poolAllocatedChunks}) from pool-owned ones so they are
 * never returned to the pool on {@link #deallocate()}; see {@link #fallbackAllocations()}.
 * <p>
 * {@link #buffer()} returns the written bytes as a single contiguous {@link ByteBuffer}, flattening
 * all chunks into a new buffer with an extra copy.
 * TODO: KAFKA-20580 (remove the extra copy on send, scatter-gather send).
 */
public class ChunkedByteBufferOutputStream extends ByteBufferOutputStream {

    private final List<ByteBuffer> chunks;
    private final List<ByteBuffer> poolAllocatedChunks;
    private final int chunkSize;
    private final BufferPool pool;
    private ByteBuffer currentChunk;
    private int currentChunkIndex;
    private int fallbackAllocations;
    // Set once the stream is closed for appends via close(); no further writes or addBuffers are allowed.
    private boolean closed;
    // Single-buffer view produced by flatten() and cached here so repeat buffer() calls
    // return the same instance. To be removed once scatter-gather (KAFKA-20580) is implemented.
    private ByteBuffer flattenedBuffer;

    /**
     * Constructs a chunked output stream backed by the given pre-allocated chunks. Ownership of
     * {@code initialChunks} transfers to this stream (they will be returned to the pool via
     * {@link #deallocate()}).
     *
     * @param initialChunks pre-allocated chunks. Must be non-empty and each chunk's capacity must
     *                      equal {@code chunkSize}
     * @param chunkSize     the size of each chunk in bytes
     * @param pool          the buffer pool used for deallocation
     */
    public ChunkedByteBufferOutputStream(List<ByteBuffer> initialChunks, int chunkSize, BufferPool pool) {
        validateInitialChunks(initialChunks, chunkSize);
        this.chunkSize = chunkSize;
        this.pool = pool;
        this.chunks = new ArrayList<>(initialChunks);
        this.poolAllocatedChunks = new ArrayList<>(initialChunks);
        this.currentChunk = this.chunks.get(0);
        this.currentChunkIndex = 0;
    }

    /**
     * Validates the chunk contract: {@code initialChunks} non-empty, each chunk's capacity equal to
     * {@code chunkSize}.
     */
    private static void validateInitialChunks(List<ByteBuffer> initialChunks, int chunkSize) {
        if (initialChunks == null || initialChunks.isEmpty())
            throw new IllegalArgumentException("initialChunks must be non-empty");
        validateChunkCapacities(initialChunks, chunkSize);
    }

    /**
     * Validates that every chunk's capacity equals {@code chunkSize}, which the stream's capacity
     * bookkeeping relies on.
     */
    private static void validateChunkCapacities(List<ByteBuffer> chunks, int chunkSize) {
        for (ByteBuffer chunk : chunks) {
            if (chunk.capacity() != chunkSize)
                throw new IllegalArgumentException("each chunk must have capacity " + chunkSize
                    + ", but found a chunk of capacity " + chunk.capacity());
        }
    }

    @Override
    public void write(int b) {
        ensureNotDeallocated();
        ensureWritable();
        advanceWhileCurrentChunkFull();
        currentChunk.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        ensureNotDeallocated();
        ensureWritable();
        while (len > 0) {
            advanceWhileCurrentChunkFull();
            int toWrite = Math.min(len, currentChunk.remaining());
            currentChunk.put(bytes, off, toWrite);
            off += toWrite;
            len -= toWrite;
        }
    }

    @Override
    public void write(ByteBuffer sourceBuffer) {
        ensureNotDeallocated();
        ensureWritable();
        while (sourceBuffer.hasRemaining()) {
            advanceWhileCurrentChunkFull();
            int toWrite = Math.min(sourceBuffer.remaining(), currentChunk.remaining());
            int oldLimit = sourceBuffer.limit();
            sourceBuffer.limit(sourceBuffer.position() + toWrite);
            currentChunk.put(sourceBuffer);
            sourceBuffer.limit(oldLimit);
        }
    }

    /**
     * Guards against writes (and {@link #addBuffers}) after the stream has been closed for appends
     * via {@link #close()}.
     */
    private void ensureWritable() {
        if (closed)
            throw new IllegalStateException("cannot write after the stream has been closed");
    }

    /**
     * Guards against any use after {@link #deallocate()} has returned the chunks.
     */
    private void ensureNotDeallocated() {
        if (currentChunk == null)
            throw new IllegalStateException("operation not allowed after the stream has been deallocated");
    }

    /**
     * Makes room for the next write by advancing past the chunks that are already full.
     */
    private void advanceWhileCurrentChunkFull() {
        while (!currentChunk.hasRemaining()) {
            advanceToNextChunk();
        }
    }

    /**
     * Advances {@code currentChunk} to the next pre-supplied chunk.
     */
    private void advanceToNextChunk() {
        if (currentChunkIndex + 1 >= chunks.size()) {
            ByteBuffer next = null;
            try {
                List<ByteBuffer> chunk = pool.allocateChunks(chunkSize, 0);
                next = chunk.get(0);
            } catch (BufferExhaustedException e) {
                // No chunks remaining in pool — leave next null so we take the heap fallback
            } catch (InterruptedException e) {
                // The acquire is non-blocking (0 ms), so this only fires when the calling thread
                // was already interrupted. Preserve the interrupt flag and, as with an exhausted
                // pool, leave next null so we take the heap fallback below
                Thread.currentThread().interrupt();
            }
            if (next != null) {
                chunks.add(next);
                poolAllocatedChunks.add(next);
            } else {
                // Heap fallback: the pool could not satisfy the allocation without blocking, but the
                // in-flight record can neither be rolled back nor blocked on, so allocate on the heap
                // to guarantee forward progress
                chunks.add(ByteBuffer.allocate(chunkSize));
                fallbackAllocations++;
            }
        }
        currentChunkIndex++;
        currentChunk = chunks.get(currentChunkIndex);
    }

    /**
     * Appends pre-allocated chunks to this stream. Ownership of {@code newChunks} transfers to
     * the stream; they will be returned to the pool via {@link #deallocate()}.
     */
    void addBuffers(List<ByteBuffer> newChunks) {
        ensureNotDeallocated();
        ensureWritable();
        validateChunkCapacities(newChunks, chunkSize);
        chunks.addAll(newChunks);
        poolAllocatedChunks.addAll(newChunks);
    }

    /**
     * Returns the written bytes as a {@link ByteBuffer}. Must be called only after the stream is
     * {@link #close() closed for appends}.
     * <p>
     * Currently the chunks are flattened into a single new buffer, built once and cached so repeat
     * calls return the same instance, which callers such as
     * {@code MemoryRecordsBuilder#writeDefaultBatchHeader} rely on when they write the batch header
     * directly into the returned buffer.
     *
     * @throws IllegalStateException if the stream has not been closed for appends
     */
    @Override
    public ByteBuffer buffer() {
        ensureNotDeallocated();
        if (!closed)
            throw new IllegalStateException("buffer() must not be called before the stream is closed for appends");
        if (flattenedBuffer == null)
            flattenedBuffer = flatten();
        return flattenedBuffer;
    }

    /**
     * Flattens the written bytes across the data-bearing chunks into a single new buffer (an extra
     * copy). This will be removed once scatter-gather send (KAFKA-20580) is implemented.
     */
    private ByteBuffer flatten() {
        // Written bytes only live in chunks up to currentChunk, later chunks are untouched.
        int totalSize = 0;
        for (int i = 0; i <= currentChunkIndex; i++) {
            totalSize += chunks.get(i).position();
        }
        ByteBuffer flattened = ByteBuffer.allocate(totalSize);
        for (int i = 0; i <= currentChunkIndex; i++) {
            ByteBuffer chunk = chunks.get(i);
            int chunkPos = chunk.position();
            chunk.flip();
            flattened.put(chunk);
            chunk.limit(chunk.capacity());
            chunk.position(chunkPos);
        }
        return flattened;
    }

    /**
     * Closes the stream for appends: no further writes or {@link #addBuffers} are allowed, and the
     * fully-unused chunks are released to the pool.
     */
    @Override
    public void close() {
        closed = true;
        releaseUnusedChunks();
    }

    /**
     * Return the fully-unused chunks to the pool. The data-bearing chunks are
     * kept until batch completion ({@link #deallocate()}), as they hold the in-flight data.
     */
    private void releaseUnusedChunks() {
        if (currentChunk == null)  // already deallocated; nothing attached
            return;
        List<ByteBuffer> unused = chunks.subList(currentChunkIndex + 1, chunks.size());
        for (ByteBuffer chunk : unused) {
            boolean poolOwned = removeByIdentity(poolAllocatedChunks, chunk);
            if (poolOwned && pool != null)
                pool.deallocate(chunk);
        }
        // Remove the released chunks from `chunks`, so they are
        // not deallocated again on batch completion.
        unused.clear();
    }

    /**
     * Removes the first element identical ({@code ==}) to {@code target} from {@code list}, returning
     * whether it was present. Uses reference identity rather than {@link Object#equals} because
     * {@link ByteBuffer#equals} compares contents, which would match the wrong chunk (e.g. two empty
     * chunks compare equal).
     */
    private static boolean removeByIdentity(List<ByteBuffer> list, ByteBuffer target) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == target) {
                list.remove(i);
                return true;
            }
        }
        return false;
    }

    /**
     * Total bytes written across all chunks.
     */
    @Override
    public int position() {
        ensureNotDeallocated();
        // Written bytes only live in chunks up to currentChunk, later chunks are untouched.
        int total = 0;
        for (int i = 0; i <= currentChunkIndex; i++) {
            total += chunks.get(i).position();
        }
        return total;
    }

    /**
     * Sets the write position, walking across pre-supplied chunks if the requested position
     * exceeds the first chunk's capacity. Only valid before any write.
     */
    @Override
    public void position(int position) {
        ensureNotDeallocated();
        if (currentChunkIndex != 0 || currentChunk.position() != 0) {
            throw new IllegalStateException("position() can only be called before any writes");
        }
        int remaining = position;
        int idx = 0;
        while (remaining > 0 && idx < chunks.size()) {
            ByteBuffer chunk = chunks.get(idx);
            int take = Math.min(remaining, chunk.capacity());
            chunk.position(take);
            remaining -= take;
            if (remaining > 0)
                idx++;
        }
        if (remaining > 0) {
            throw new IllegalArgumentException("position " + position
                + " exceeds total pre-allocated capacity");
        }
        currentChunkIndex = idx;
        currentChunk = chunks.get(idx);
    }

    /**
     * Total capacity across all attached chunks (written + free).
     * Every chunk has the same size, so this equals {@code position() + remaining()} without walking the list.
     */
    int attachedCapacity() {
        ensureNotDeallocated();
        return chunks.size() * chunkSize;
    }

    /**
     * Number of chunks that had to be allocated from the heap because the pool was exhausted
     * mid-record. Zero on the normal path; a non-zero value means the producer transiently exceeded
     * buffer.memory to guarantee forward progress. Exposed for metrics and tests.
     */
    int fallbackAllocations() {
        return fallbackAllocations;
    }

    /**
     * Total bytes available across the current chunk and every queued (not-yet-active) chunk.
     */
    @Override
    public int remaining() {
        ensureNotDeallocated();
        int total = currentChunk.remaining();
        for (int i = currentChunkIndex + 1; i < chunks.size(); i++)
            total += chunks.get(i).remaining();
        return total;
    }

    @Override
    public int initialCapacity() {
        ensureNotDeallocated();
        return chunkSize;
    }

    /**
     * Throws if {@code requiredBytes} cannot be written to this stream.
     * This only checks the attached chunks, it does not make room.
     *
     * @throws IllegalStateException if the attached chunks have less free space than required
     */
    void throwIfInsufficientRemaining(int requiredBytes) {
        ensureNotDeallocated();
        // A single write can be split across several chunks, so the required bytes needn't be
        // contiguous: only the total free space matters. Advancing here would waste the tail of the
        // current chunk, so writes advance lazily and this only validates. Automatic growth lives in
        // advanceToNextChunk; this method only checks the currently attached chunks (the accumulator
        // uses it to decide when to attach extension chunks up front).
        if (requiredBytes > remaining())
            throw new IllegalStateException("required " + requiredBytes
                + " bytes but only " + remaining() + " remaining across the attached chunks");
    }

    /**
     * Returns all pool-allocated chunks to the buffer pool. Called at batch completion.
     */
    void deallocate(BufferPool pool) {
        if (pool != null) {
            for (ByteBuffer chunk : poolAllocatedChunks) {
                pool.deallocate(chunk);
            }
        }
        chunks.clear();
        poolAllocatedChunks.clear();
        currentChunk = null;
        currentChunkIndex = -1;
        flattenedBuffer = null;
    }

    void deallocate() {
        deallocate(pool);
    }
}
