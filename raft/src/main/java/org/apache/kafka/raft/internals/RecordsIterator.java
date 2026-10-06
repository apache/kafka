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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Batch;

import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class RecordsIterator<T> implements Iterator<Batch<T>>, AutoCloseable {
    private final Logger logger;
    private final Records records;
    private final RecordsDecodingStrategy<T> decodingStrategy;
    private final BufferSupplier bufferSupplier;
    private final int batchSize;
    // Setting to true will make the RecordsIterator perform a CRC Validation
    // on the batch header when iterating over them
    private final boolean doCrcValidation;

    private Iterator<MutableRecordBatch> nextBatches = Collections.emptyIterator();
    private Optional<Batch<T>> nextBatch = Optional.empty();
    // Buffer used as the backing store for nextBatches if needed
    private Optional<ByteBuffer> allocatedBuffer = Optional.empty();
    // Number of bytes from records read up to now
    private int bytesRead = 0;
    private boolean isClosed = false;

    /**
     * This class provides an iterator over records retrieved via the raft client or from a snapshot
     * @param records the records
     * @param decodingStrategy the strategy deciding which records to decode (control, data, or both)
     * @param bufferSupplier the buffer supplier implementation to allocate buffers when reading records. This must return ByteBuffer allocated on the heap
     * @param batchSize the maximum batch size
     */
    public RecordsIterator(
        Records records,
        RecordsDecodingStrategy<T> decodingStrategy,
        BufferSupplier bufferSupplier,
        int batchSize,
        boolean doCrcValidation,
        LogContext logContext
    ) {
        this.records = records;
        this.decodingStrategy = decodingStrategy;
        this.bufferSupplier = bufferSupplier;
        this.batchSize = Math.max(batchSize, Records.HEADER_SIZE_UP_TO_MAGIC);
        this.doCrcValidation = doCrcValidation;
        this.logger = logContext.logger(getClass());
    }

    @Override
    public boolean hasNext() {
        ensureOpen();

        if (nextBatch.isEmpty()) {
            nextBatch = nextBatch();
        }

        return nextBatch.isPresent();
    }

    @Override
    public Batch<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Batch iterator doesn't have any more elements");
        }

        Batch<T> batch = nextBatch.get();
        nextBatch = Optional.empty();

        return batch;
    }

    @Override
    public void close() {
        isClosed = true;
        allocatedBuffer.ifPresent(bufferSupplier::release);
        allocatedBuffer = Optional.empty();
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Serde record batch iterator was closed");
        }
    }

    private MemoryRecords readFileRecords(FileRecords fileRecords, ByteBuffer buffer) {
        int start = buffer.position();
        try {
            fileRecords.readInto(buffer, bytesRead);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read records into memory", e);
        }

        bytesRead += buffer.limit() - start;
        return MemoryRecords.readableRecords(buffer.slice());
    }

    private MemoryRecords createMemoryRecords(FileRecords fileRecords) {
        final ByteBuffer buffer;
        if (allocatedBuffer.isPresent()) {
            buffer = allocatedBuffer.get();
            buffer.compact();
        } else {
            buffer = bufferSupplier.get(Math.min(batchSize, records.sizeInBytes()));
            allocatedBuffer = Optional.of(buffer);
        }

        MemoryRecords memoryRecords = readFileRecords(fileRecords, buffer);

        // firstBatchSize() is always non-null because the minimum buffer is HEADER_SIZE_UP_TO_MAGIC.
        int firstBatchSize = memoryRecords.firstBatchSize();
        if (firstBatchSize <= buffer.remaining()) {
            return memoryRecords;
        } else {
            logger.info(
                "Creating a new buffer; previous buffer {} cannot fit at least {} bytes",
                buffer,
                firstBatchSize
            );
            // Not enough bytes read; create a bigger buffer
            ByteBuffer newBuffer = bufferSupplier.get(memoryRecords.firstBatchSize());
            allocatedBuffer = Optional.of(newBuffer);

            newBuffer.put(buffer);
            bufferSupplier.release(buffer);

            return readFileRecords(fileRecords, newBuffer);
        }
    }

    private Iterator<MutableRecordBatch> nextBatches() {
        int recordSize = records.sizeInBytes();
        if (bytesRead < recordSize) {
            final MemoryRecords memoryRecords;
            if (records instanceof MemoryRecords) {
                bytesRead = recordSize;
                memoryRecords = (MemoryRecords) records;
            } else if (records instanceof FileRecords) {
                memoryRecords = createMemoryRecords((FileRecords) records);
            } else {
                throw new IllegalStateException(String.format("Unexpected Records type %s", records.getClass()));
            }

            return memoryRecords.batchIterator();
        }

        return Collections.emptyIterator();
    }

    private Optional<Batch<T>> nextBatch() {
        if (!nextBatches.hasNext()) {
            nextBatches = nextBatches();
        }

        if (nextBatches.hasNext()) {
            MutableRecordBatch nextBatch = nextBatches.next();
            // Update the buffer position to reflect the read batch
            allocatedBuffer.ifPresent(buffer -> buffer.position(buffer.position() + nextBatch.sizeInBytes()));

            if (!(nextBatch instanceof DefaultRecordBatch)) {
                throw new IllegalStateException(
                    String.format("DefaultRecordBatch expected by record type was %s", nextBatch.getClass())
                );
            }

            return Optional.of(readBatch((DefaultRecordBatch) nextBatch));
        }

        return Optional.empty();
    }

    private Batch<T> readBatch(DefaultRecordBatch batch) {
        if (doCrcValidation) {
            // Perform a CRC validity check on this batch
            batch.ensureValid();
        }

        return decodingStrategy.readBatch(batch, bufferSupplier);
    }
}
