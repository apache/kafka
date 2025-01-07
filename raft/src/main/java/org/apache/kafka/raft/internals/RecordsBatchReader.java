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

import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

public final class RecordsBatchReader<T> implements BatchReader<T> {
    private final long baseOffset;
    private final RecordsIterator<T> iterator;
    private final CloseListener<BatchReader<T>> closeListener;

    private long lastReturnedOffset;

    private Optional<Batch<T>> nextBatch = Optional.empty();
    private boolean isClosed = false;

    private RecordsBatchReader(
        long baseOffset,
        RecordsIterator<T> iterator,
        CloseListener<BatchReader<T>> closeListener
    ) {
        this.baseOffset = baseOffset;
        this.iterator = iterator;
        this.closeListener = closeListener;
        this.lastReturnedOffset = baseOffset;
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
            throw new NoSuchElementException("Records batch reader doesn't have any more elements");
        }

        Batch<T> batch = nextBatch.get();
        nextBatch = Optional.empty();

        lastReturnedOffset = batch.lastOffset();
        return batch;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public OptionalLong lastOffset() {
        if (isClosed) {
            return OptionalLong.of(lastReturnedOffset);
        } else {
            return OptionalLong.empty();
        }
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;

            iterator.close();
            closeListener.onClose(this);
        }
    }

    public static <T> RecordsBatchReader<T> of(
        long baseOffset,
        Records records,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        int maxBatchSize,
        CloseListener<BatchReader<T>> closeListener,
        boolean doCrcValidation
    ) {
        return new RecordsBatchReader<>(
            baseOffset,
            new RecordsIterator<>(records, serde, bufferSupplier, maxBatchSize, doCrcValidation),
            closeListener
        );
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Records batch reader was closed");
        }
    }

    private Optional<Batch<T>> nextBatch() {
        if (iterator.hasNext()) {
            return Optional.of(iterator.next());
        }

        return Optional.empty();
    }
}
