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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChunkedByteBufferOutputStreamTest {

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @AfterEach
    public void teardown() {
        metrics.close();
    }

    private BufferPool pool(long total, int chunkSize) {
        String metricGroup = "test";
        return new BufferPool(total, chunkSize, metrics, time, metricGroup);
    }

    private List<ByteBuffer> chunks(BufferPool pool, int chunkSize, int count) throws InterruptedException {
        return pool.allocateChunks(chunkSize * count, 100);
    }

    @Test
    @SuppressWarnings({"try", "resource"}) // Each constructor call throws, so no stream is ever created
    public void testConstructorRejectsInvalidChunks() {
        int chunkSize = 16;
        BufferPool p = pool(64, chunkSize);

        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(null, chunkSize, p));
        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(Collections.emptyList(), chunkSize, p));

        // A chunk whose capacity doesn't match chunkSize violates the contract.
        List<ByteBuffer> wrongSize = Collections.singletonList(ByteBuffer.allocate(chunkSize + 1));
        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(wrongSize, chunkSize, p));
    }

    @Test
    public void testOperationsRejectedAfterDeallocate() throws Exception {
        int chunkSize = 8;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 2), chunkSize, p)) {
            stream.write(new byte[]{1, 2, 3}, 0, 3);

            stream.deallocate();

            // Every query/write operation must reject use after deallocation.
            assertThrows(IllegalStateException.class, stream::remaining);
            assertThrows(IllegalStateException.class, stream::position);
            assertThrows(IllegalStateException.class, stream::buffer);
            assertThrows(IllegalStateException.class, stream::attachedCapacity);
            assertThrows(IllegalStateException.class, stream::limit);
            assertThrows(IllegalStateException.class, stream::initialCapacity);
            assertThrows(IllegalStateException.class, () -> stream.position(1));
            assertThrows(IllegalStateException.class, () -> stream.ensureRemaining(1));
            assertThrows(IllegalStateException.class, () -> stream.write(1));
            assertThrows(IllegalStateException.class, () -> stream.write(new byte[]{4}, 0, 1));
            assertThrows(IllegalStateException.class, () -> stream.write(ByteBuffer.wrap(new byte[]{5})));
            assertThrows(IllegalStateException.class, () -> stream.addBuffers(Collections.singletonList(ByteBuffer.allocate(chunkSize))));

            // Lifecycle calls stay idempotent no-ops: a second close()/deallocate() must not throw.
            assertDoesNotThrow(() -> {
                stream.close();
                stream.deallocate();
            });
        }
    }

    @Test
    public void testWritesDisallowedAfterClose() throws Exception {
        int chunkSize = 16;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 2), chunkSize, p)) {
            stream.write(new byte[]{1, 2, 3}, 0, 3);

            // close() closes the stream for appends; any subsequent write must fail.
            stream.close();

            assertThrows(IllegalStateException.class, () -> stream.write(1));
            assertThrows(IllegalStateException.class, () -> stream.write(new byte[]{4}, 0, 1));
            assertThrows(IllegalStateException.class, () -> stream.write(ByteBuffer.wrap(new byte[]{5})));
            // Attaching more chunks is a write-preparation step, so it is disallowed once closed too.
            assertThrows(IllegalStateException.class,
                () -> stream.addBuffers(Collections.singletonList(ByteBuffer.allocate(chunkSize))));

            // buffer() still works after close and returns the same cached instance on repeat calls.
            ByteBuffer first = stream.buffer();
            assertSame(first, stream.buffer(), "buffer() must return the same cached instance once built");

            stream.deallocate();
        }
    }

    @Test
    public void testSingleChunkWriteRoundtrip() throws Exception {
        int chunkSize = 16;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 1), chunkSize, p)) {

            byte[] payload = new byte[]{1, 2, 3, 4, 5};
            stream.write(payload, 0, payload.length);

            ByteBuffer flat = stream.buffer();
            flat.flip();
            byte[] out = new byte[flat.remaining()];
            flat.get(out);
            assertArrayEquals(payload, out);

            stream.deallocate();
        }
    }

    @Test
    public void testWriteAcrossMultipleChunks() throws Exception {
        int chunkSize = 8;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 3), chunkSize, p)) {

            byte[] payload = new byte[20];
            for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
            stream.write(payload, 0, payload.length);

            ByteBuffer flat = stream.buffer();
            flat.flip();
            byte[] out = new byte[flat.remaining()];
            flat.get(out);
            assertArrayEquals(payload, out);

            stream.deallocate();
        }
    }

    @Test
    public void testRemainingSumsFreeBytesAcrossChunks() throws Exception {
        int chunkSize = 8;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 2), chunkSize, p)) {

            assertEquals(2 * chunkSize, stream.remaining());
            stream.write(new byte[3], 0, 3);
            assertEquals(2 * chunkSize - 3, stream.remaining());
            stream.write(new byte[chunkSize], 0, chunkSize); // crosses into chunk 2
            assertEquals(chunkSize - 3, stream.remaining());

            stream.deallocate();
        }
    }

    @Test
    public void testAddBuffersExtendsStream() throws Exception {
        int chunkSize = 8;
        BufferPool p = pool(64, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 1), chunkSize, p)) {

            // Fill the initial chunk.
            stream.write(new byte[chunkSize], 0, chunkSize);
            assertEquals(0, stream.remaining());

            // Extend and write more — must land in the new chunk.
            stream.addBuffers(Collections.singletonList(p.allocate(chunkSize, 100)));
            assertEquals(chunkSize, stream.remaining());

            byte[] more = new byte[]{9, 9, 9};
            stream.write(more, 0, more.length);
            assertEquals(chunkSize - 3, stream.remaining());

            stream.deallocate();
        }
    }

    @Test
    public void testPositionWalksAcrossChunks() throws Exception {
        int chunkSize = 4;
        BufferPool p = pool(32, chunkSize);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 3), chunkSize, p)) {

            stream.position(6); // straddles chunk 0 (4 bytes) and chunk 1 (2 bytes)
            assertEquals(6, stream.position());
            assertEquals(6, stream.remaining());

            stream.deallocate();
        }
    }

    @Test
    public void testDeallocateReturnsAllChunks() throws Exception {
        int chunkSize = 8;
        long total = 64;
        BufferPool p = pool(total, chunkSize);
        List<ByteBuffer> initial = chunks(p, chunkSize, 3);
        try (ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(initial, chunkSize, p)) {
            // Also attach one extra chunk so deallocate must handle the added-buffer case too.
            stream.addBuffers(Collections.singletonList(p.allocate(chunkSize, 100)));
            assertEquals(total - 4L * chunkSize, p.availableMemory());

            stream.deallocate();
        }
        assertEquals(total, p.availableMemory());
    }

    /**
     * The fully-unused chunks are returned to the pool on {@link ChunkedByteBufferOutputStream#close()}
     * (the stream is closed for appends), not as a side effect of reading {@link
     * ChunkedByteBufferOutputStream#buffer()}. Data-bearing chunks stay reserved until
     * {@link ChunkedByteBufferOutputStream#deallocate()}, which must not return the already-released
     * chunks a second time.
     */
    @Test
    public void testUnusedChunksReleasedOnCloseNotOnBuffer() throws Exception {
        int chunkSize = 8;
        long total = 64;
        BufferPool p = pool(total, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 3), chunkSize, p);
        // Write into the first chunk only; chunks 2 and 3 stay unused.
        byte[] payload = new byte[]{1, 2, 3};
        stream.write(payload, 0, payload.length);
        assertEquals(total - 3L * chunkSize, p.availableMemory());

        // reading buffer() on a still-open stream must not release chunks.
        ByteBuffer built = stream.buffer();
        assertEquals(total - 3L * chunkSize, p.availableMemory(),
            "buffer() must not release chunks on a still-open stream");
        built.flip();
        byte[] out = new byte[built.remaining()];
        built.get(out);
        assertArrayEquals(payload, out);

        // close() (appends done) releases the two unused chunks; a second close() is a no-op.
        stream.close();
        assertEquals(total - chunkSize, p.availableMemory(),
            "the two unused chunks should return to the pool on close");
        stream.close();
        assertEquals(total - chunkSize, p.availableMemory());

        // Completion-time deallocate returns only the remaining data-bearing chunk (no double free).
        stream.deallocate();
        assertEquals(total, p.availableMemory(),
            "pool must be exactly restored; released chunks must not be returned twice on completion");
    }
}