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
 * A {@link ByteBufferOutputStream} backed by a linked list of fixed-size chunks instead of a
 * single re-allocated buffer.
 * <p>
 * <b>This stream does not grow on its own.</b> Chunks are supplied by the caller — initial
 * chunks via the constructor, additional chunks via {@link #addBuffers(List)}. When a write's
 * size exceeds the stream's {@link #remaining()} (sum of free bytes across all attached
 * chunks), {@link IllegalStateException} is thrown. The caller is responsible for attaching
 * additional chunks before any such write.
 * <p>
 * Automatic mid-write growth (allocating from inside {@code write()}) is a follow-up tied to
 * compression support, where compressor buffering can make a write exceed its reservation; until
 * then the write path stays allocation-free and deterministic.
 * <p>
 * When {@link #buffer()} is called (typically at batch close), all chunks are flattened into a
 * single contiguous ByteBuffer — exactly one copy operation.
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
     * {@code initialChunks} transfers to this stream — they will be returned to the pool via
     * {@link #deallocate()}.
     *
     * @param initialChunks pre-allocated chunks; must be non-empty; each chunk's capacity must
     *                      equal {@code chunkSize}
     * @param chunkSize     the size of each chunk in bytes
     * @param pool          the buffer pool used for deallocation
     */
    @SuppressWarnings("this-escape")
    public ChunkedByteBufferOutputStream(List<ByteBuffer> initialChunks, int chunkSize, BufferPool pool) {
        super(initialChunks.get(0));
        this.chunkSize = chunkSize;
        this.pool = pool;
        this.chunks = new ArrayList<>(initialChunks);
        this.currentChunk = this.chunks.get(0);
        this.currentChunkIndex = 0;
        this.dirty = true;
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
        if (currentChunk.remaining() < needed) {
            advanceToNextChunk();
        }
    }

    /**
     * Advances {@code currentChunk} to the next pre-supplied chunk; throws if none is left. The
     * caller must attach additional chunks via {@link #addBuffers(List)} before any write that
     * exceeds {@link #remaining()}. (Mid-record growth — non-blocking pool then heap — is a
     * follow-up that lands with compression support.)
     */
    private void advanceToNextChunk() {
        if (currentChunkIndex + 1 >= chunks.size()) {
            throw new IllegalStateException(
                "No more chunks available; the write exceeded the stream's remaining capacity. "
                    + "Caller must obtain additional chunks (e.g. from ChunkedBufferPool) and attach them "
                    + "via addBuffers before any write whose size exceeds remaining()");
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
        // TODO: KAFKA-20687. This flatten runs at batch close, when the chunk set is final.
        //  Today all chunks (used and unused) are returned to the pool only when the batch
        //  completes (via deallocate(pool)). Consider releasing the fully-unused chunks
        //  early, at close, rather than holding them until completion — removing them from
        //  `chunks` here so the completion-time deallocate(pool) does not double-return them.
        int totalSize = 0;
        for (ByteBuffer chunk : chunks) {
            totalSize += chunk.position();
        }
        flattenedBuffer = ByteBuffer.allocate(totalSize);
        for (ByteBuffer chunk : chunks) {
            int chunkPos = chunk.position();
            chunk.flip();
            flattenedBuffer.put(chunk);
            chunk.limit(chunk.capacity());
            chunk.position(chunkPos);
        }
        dirty = false;
        // The bytes are now copied into flattenedBuffer, but we intentionally do not release the
        //  chunks until batch completion, so the in-flight data stays reserved against the
        //  buffer.memory budget (the pool's available memory reflects it), consistent with the
        //  "full" strategy. Releasing here would return that memory to the pool while the bytes are
        //  still in flight in the heap copy, letting the pool admit more than buffer.memory intends.
        //  This flattening is an initial approach and will be removed with KAFKA-20580.
        return flattenedBuffer;
    }

    /**
     * Total bytes written across all chunks.
     */
    @Override
    public int position() {
        int total = 0;
        for (ByteBuffer chunk : chunks) {
            total += chunk.position();
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
     * Total bytes available across the current chunk and every queued (not-yet-active) chunk.
     */
    @Override
    public int remaining() {
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
        return chunks.isEmpty() ? chunkSize : chunks.get(0).capacity();
    }

    @Override
    public void ensureRemaining(int remainingBytesRequired) {
        // A single call can guarantee at most `chunkSize` of space (the stream advances one chunk
        // at a time); callers needing more attach chunks via addBuffers first. write(byte[]) loops
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
