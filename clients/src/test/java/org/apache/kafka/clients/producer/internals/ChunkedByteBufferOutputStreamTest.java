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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChunkedByteBufferOutputStreamTest {

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final String metricGroup = "test";

    @AfterEach
    public void teardown() {
        metrics.close();
    }

    private ChunkedBufferPool pool(long total, int chunkSize) {
        return new ChunkedBufferPool(total, chunkSize, metrics, time, metricGroup);
    }

    private List<ByteBuffer> chunks(ChunkedBufferPool pool, int chunkSize, int count) throws InterruptedException {
        return pool.allocateChunks(chunkSize * count, 100);
    }

    /**
     * The constructor fails fast with {@link IllegalArgumentException} when its chunk contract is
     * violated — a null or empty list, or a chunk whose capacity differs from {@code chunkSize} —
     * rather than the bare NPE / IndexOutOfBoundsException that {@code initialChunks.get(0)} would
     * otherwise throw from the {@code super(...)} call.
     */
    @Test
    public void testConstructorRejectsInvalidChunks() throws Exception {
        int chunkSize = 16;
        ChunkedBufferPool p = pool(64, chunkSize);

        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(null, chunkSize, p));
        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(Collections.emptyList(), chunkSize, p));

        // A chunk whose capacity doesn't match chunkSize violates the contract.
        List<ByteBuffer> wrongSize = Collections.singletonList(ByteBuffer.allocate(chunkSize + 1));
        assertThrows(IllegalArgumentException.class,
            () -> new ChunkedByteBufferOutputStream(wrongSize, chunkSize, p));
    }

    /** Writes that fit in the initial chunk: stream produces the bytes back via buffer(). */
    @Test
    public void testSingleChunkWriteRoundtrip() throws Exception {
        int chunkSize = 16;
        ChunkedBufferPool p = pool(64, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 1), chunkSize, p);

        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        stream.write(payload, 0, payload.length);

        ByteBuffer flat = stream.buffer();
        flat.flip();
        byte[] out = new byte[flat.remaining()];
        flat.get(out);
        assertArrayEquals(payload, out);

        stream.deallocate();
    }

    /** Writes that span multiple pre-supplied chunks land in subsequent chunks in order. */
    @Test
    public void testWriteSpansChunks() throws Exception {
        int chunkSize = 8;
        ChunkedBufferPool p = pool(64, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 3), chunkSize, p);

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

    /** remaining() sums current + queued chunk capacities and shrinks as writes consume bytes. */
    @Test
    public void testRemainingSumsChunks() throws Exception {
        int chunkSize = 8;
        ChunkedBufferPool p = pool(64, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 2), chunkSize, p);

        assertEquals(2 * chunkSize, stream.remaining());
        stream.write(new byte[3], 0, 3);
        assertEquals(2 * chunkSize - 3, stream.remaining());
        stream.write(new byte[chunkSize], 0, chunkSize); // crosses into chunk 2
        assertEquals(chunkSize - 3, stream.remaining());

        stream.deallocate();
    }

    /** addBuffers extends capacity so a write that would have overflowed now fits. */
    @Test
    public void testAddBuffersExtendsStream() throws Exception {
        int chunkSize = 8;
        ChunkedBufferPool p = pool(64, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 1), chunkSize, p);

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

    /** Writing past all pre-supplied (and added) chunks throws — uncompressed-only contract. */
    @Test
    public void testOverflowThrows() throws Exception {
        int chunkSize = 4;
        ChunkedBufferPool p = pool(64, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 1), chunkSize, p);

        stream.write(new byte[chunkSize], 0, chunkSize);
        assertThrows(IllegalStateException.class, () -> stream.write((byte) 1));

        stream.deallocate();
    }

    /** position(int) at construction walks across pre-supplied chunks. */
    @Test
    public void testPositionWalksAcrossChunks() throws Exception {
        int chunkSize = 4;
        ChunkedBufferPool p = pool(32, chunkSize);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks(p, chunkSize, 3), chunkSize, p);

        stream.position(6); // straddles chunk 0 (4 bytes) and chunk 1 (2 bytes)
        assertEquals(6, stream.position());
        assertEquals(6, stream.remaining());

        stream.deallocate();
    }

    /** deallocate returns all chunks to the pool. */
    @Test
    public void testDeallocateReturnsAllChunks() throws Exception {
        int chunkSize = 8;
        long total = 64;
        ChunkedBufferPool p = pool(total, chunkSize);
        List<ByteBuffer> initial = chunks(p, chunkSize, 3);
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(initial, chunkSize, p);
        // Also attach one extra chunk so deallocate must handle the added-buffer case too.
        stream.addBuffers(Collections.singletonList(p.allocate(chunkSize, 100)));
        assertEquals(total - 4L * chunkSize, p.availableMemory());

        stream.deallocate();
        assertEquals(total, p.availableMemory());
    }
}
