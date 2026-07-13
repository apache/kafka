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

import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ByteBufferOutputStream} backed by a linked list of fixed-size chunks instead of a single
 * re-allocated buffer. Chunks are supplied by the caller (initial chunks via the constructor,
 * additional chunks via {@link #addBuffers(List)}).
 * <p>
 * Current/temporary behavior:
 * <ul>
 * <li>The stream does not grow on its own: a write whose size exceeds the remaining free bytes
 *     across all attached chunks throws {@link IllegalStateException}, so the caller must attach
 *     enough chunks before any such write.
 *     TODO: KAFKA-20579 (automatic mid-write growth for compression support).</li>
 * <li>{@link #buffer()} returns the written bytes as a single contiguous {@link ByteBuffer},
 *     flattening all chunks into a new buffer with an extra copy.
 *     TODO: KAFKA-20580 (remove the extra copy on send, scatter-gather send).</li>
 * </ul>
 */
public class ChunkedByteBufferOutputStream extends ByteBufferOutputStream {

    private final List<ByteBuffer> chunks;
    private final int chunkSize;
    private final BufferPool pool;
    private ByteBuffer currentChunk;
    private int currentChunkIndex;
    private ByteBuffer flattenedBuffer;
    private boolean dirty;

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
        super(validatedFirstChunk(initialChunks, chunkSize));
        this.chunkSize = chunkSize;
        this.pool = pool;
        this.chunks = new ArrayList<>(initialChunks);
        this.currentChunk = this.chunks.get(0);
        this.currentChunkIndex = 0;
        this.dirty = true;
    }

    /**
     * Validates the chunk contract: {@code initialChunks} non-empty, each chunk's capacity equal to
     * {@code chunkSize}. Returns the first chunk.
     */
    private static ByteBuffer validatedFirstChunk(List<ByteBuffer> initialChunks, int chunkSize) {
        if (initialChunks == null || initialChunks.isEmpty())
            throw new IllegalArgumentException("initialChunks must be non-empty");
        for (ByteBuffer chunk : initialChunks) {
            if (chunk.capacity() != chunkSize)
                throw new IllegalArgumentException("each chunk must have capacity " + chunkSize
                    + ", but found a chunk of capacity " + chunk.capacity());
        }
        return initialChunks.get(0);
    }

    @Override
    public void write(int b) {
        ensureChunkCapacity(1);
        currentChunk.put((byte) b);
        dirty = true;
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        while (len > 0) {
            ensureChunkCapacity(1);
            int toWrite = Math.min(len, currentChunk.remaining());
            currentChunk.put(bytes, off, toWrite);
            off += toWrite;
            len -= toWrite;
        }
        dirty = true;
    }

    @Override
    public void write(ByteBuffer sourceBuffer) {
        while (sourceBuffer.hasRemaining()) {
            ensureChunkCapacity(1);
            int toWrite = Math.min(sourceBuffer.remaining(), currentChunk.remaining());
            int oldLimit = sourceBuffer.limit();
            sourceBuffer.limit(sourceBuffer.position() + toWrite);
            currentChunk.put(sourceBuffer);
            sourceBuffer.limit(oldLimit);
        }
        dirty = true;
    }

    private void ensureChunkCapacity(int needed) {
        while (currentChunk.remaining() < needed) {
            advanceToNextChunk();
        }
    }

    /**
     * Advances {@code currentChunk} to the next pre-supplied chunk.
     */
    private void advanceToNextChunk() {
        if (currentChunkIndex + 1 >= chunks.size()) {
            // TODO: KAFKA-20579. With compression support, grow here instead of throwing.
            throw new IllegalStateException("write exceeded the stream's remaining chunk capacity");
        }
        currentChunkIndex++;
        currentChunk = chunks.get(currentChunkIndex);
    }

    /**
     * Appends pre-allocated chunks to this stream. Ownership of {@code newChunks} transfers to
     * the stream; they will be returned to the pool via {@link #deallocate()}.
     */
    public void addBuffers(List<ByteBuffer> newChunks) {
        chunks.addAll(newChunks);
    }

    @Override
    public ByteBuffer buffer() {
        if (flattenedBuffer != null && !dirty) {
            return flattenedBuffer;
        }
        // Written bytes only live in chunks up to currentChunk; later chunks are untouched.
        int lastDataChunk = Math.min(currentChunkIndex, chunks.size() - 1);
        int totalSize = 0;
        for (int i = 0; i <= lastDataChunk; i++) {
            totalSize += chunks.get(i).position();
        }
        flattenedBuffer = ByteBuffer.allocate(totalSize);
        for (int i = 0; i <= lastDataChunk; i++) {
            ByteBuffer chunk = chunks.get(i);
            int chunkPos = chunk.position();
            chunk.flip();
            flattenedBuffer.put(chunk);
            chunk.limit(chunk.capacity());
            chunk.position(chunkPos);
        }
        dirty = false;
        return flattenedBuffer;
    }

    /**
     * Releases the fully-unused chunks, given that the stream is closed for appends.
     */
    @Override
    public void close() {
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
        if (pool != null) {
            for (ByteBuffer chunk : unused)
                pool.deallocate(chunk);
        }
        // Remove the released chunks from `chunks`, so they are
        // not deallocated again on batch completion.
        unused.clear();
    }

    /**
     * Total bytes written across all chunks.
     */
    @Override
    public int position() {
        // Written bytes only live in chunks up to currentChunk; later chunks are untouched.
        int lastDataChunk = Math.min(currentChunkIndex, chunks.size() - 1);
        int total = 0;
        for (int i = 0; i <= lastDataChunk; i++) {
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
        dirty = true;
    }

    /**
     * Total capacity across all attached chunks (written + free).
     * Every chunk has the same size, so this equals {@code position() + remaining()} without walking the list.
     */
    int attachedCapacity() {
        return chunks.size() * chunkSize;
    }

    /**
     * Total bytes available across the current chunk and every queued (not-yet-active) chunk.
     */
    @Override
    public int remaining() {
        if (currentChunk == null)  // after deallocate no chunks attached, no free capacity
            return 0;
        int total = currentChunk.remaining();
        for (int i = currentChunkIndex + 1; i < chunks.size(); i++)
            total += chunks.get(i).remaining();
        return total;
    }

    @Override
    public int limit() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int initialCapacity() {
        return chunkSize;
    }

    @Override
    public void ensureRemaining(int remainingBytesRequired) {
        // A single call can guarantee at most `chunkSize` of space (the stream advances one chunk
        // at a time). Callers needing more attach chunks via addBuffers first. write(byte[]) loops
        // across chunks, so contiguous capacity isn't required.
        ensureChunkCapacity(Math.min(remainingBytesRequired, chunkSize));
    }

    /**
     * Returns all pool-allocated chunks to the buffer pool. Called at batch completion.
     */
    public void deallocate(BufferPool pool) {
        if (pool != null) {
            for (ByteBuffer chunk : chunks) {
                pool.deallocate(chunk);
            }
        }
        chunks.clear();
        currentChunk = null;
        flattenedBuffer = null;
    }

    public void deallocate() {
        deallocate(pool);
    }
}
