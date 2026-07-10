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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Decides which records {@link RecordsIterator} decodes when turning a batch into a {@link Batch}:
 * {@link ControlAndDataDecodingStrategy} decodes both, {@link ControlOnlyDecodingStrategy} decodes
 * only the control records (used by the kraft partition listener, which needs no serde), and
 * {@link DataOnlyDecodingStrategy} decodes only the data records. Skipped records are returned as a
 * {@link Batch#notDecoded} batch carrying only the offset information. The static helpers are shared
 * by the implementations.
 */
public interface DecodingStrategy<T> {

    Batch<T> readBatch(DefaultRecordBatch batch, InputStream input, BufferSupplier bufferSupplier, int numRecords);

    static <T> Batch<T> readControlBatch(DefaultRecordBatch batch, InputStream input, BufferSupplier bufferSupplier, int numRecords) {
        List<ControlRecord> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            records.add(readRecord(input, batch.sizeInBytes(), bufferSupplier, DecodingStrategy::decodeControlRecord));
        }
        return Batch.control(
            batch.baseOffset(),
            batch.partitionLeaderEpoch(),
            batch.maxTimestamp(),
            batch.sizeInBytes(),
            records
        );
    }

    static <T> Batch<T> readDataBatch(DefaultRecordBatch batch, InputStream input, BufferSupplier bufferSupplier, int numRecords, RecordSerde<T> serde) {
        List<T> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            records.add(readRecord(input, batch.sizeInBytes(), bufferSupplier, (key, value) -> decodeDataRecord(key, value, serde)));
        }
        return Batch.data(
            batch.baseOffset(),
            batch.partitionLeaderEpoch(),
            batch.maxTimestamp(),
            batch.sizeInBytes(),
            records
        );
    }

    static <T> Batch<T> notDecodedBatch(DefaultRecordBatch batch, int numRecords) {
        return Batch.notDecoded(
            batch.baseOffset(),
            batch.partitionLeaderEpoch(),
            batch.maxTimestamp(),
            batch.sizeInBytes(),
            numRecords
        );
    }

    private static <U> U readRecord(
        InputStream stream,
        int totalBatchSize,
        BufferSupplier bufferSupplier,
        BiFunction<Optional<ByteBuffer>, Optional<ByteBuffer>, U> decoder
    ) {
        // Read size of body in bytes
        int size;
        try {
            size = ByteUtils.readVarint(stream);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read record size", e);
        }
        if (size <= 0) {
            throw new RuntimeException("Invalid non-positive frame size: " + size);
        }
        if (size > totalBatchSize) {
            throw new RuntimeException("Specified frame size, " + size + ", is larger than the entire size of the " +
                    "batch, which is " + totalBatchSize);
        }
        ByteBuffer buf = bufferSupplier.get(size);

        // The last byte of the buffer is reserved for a varint set to the number of record headers, which
        // must be 0. Therefore, we set the ByteBuffer limit to size - 1.
        buf.limit(size - 1);

        try {
            int bytesRead = stream.read(buf.array(), 0, size);
            if (bytesRead != size) {
                throw new RuntimeException("Unable to read " + size + " bytes, only read " + bytesRead);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read record bytes", e);
        }
        try {
            ByteBufferAccessor input = new ByteBufferAccessor(buf);

            // Read unused attributes
            input.readByte();

            long timestampDelta = input.readVarlong();
            if (timestampDelta != 0) {
                throw new IllegalArgumentException("Got timestamp delta of " + timestampDelta + ", but this is invalid because it " +
                        "is not 0 as expected.");
            }

            // Read offset delta
            input.readVarint();

            // Read the key
            int keySize = input.readVarint();
            Optional<ByteBuffer> key = Optional.empty();
            if (keySize >= 0) {
                key = Optional.of(input.readByteBuffer(keySize));
            }

            // Read the value
            int valueSize = input.readVarint();
            Optional<ByteBuffer> value = Optional.empty();
            if (valueSize >= 0) {
                value = Optional.of(input.readByteBuffer(valueSize));
            }

            // Read the record body from the file input reader
            U record = decoder.apply(key, value);

            // Read the number of headers. Currently, this must be a single byte set to 0.
            int numHeaders = buf.array()[size - 1];
            if (numHeaders != 0) {
                throw new IllegalArgumentException("Got numHeaders of " + numHeaders + ", but this is invalid because " +
                        "it is not 0 as expected.");
            }

            return record;
        } finally {
            bufferSupplier.release(buf);
        }
    }

    private static <T> T decodeDataRecord(Optional<ByteBuffer> key, Optional<ByteBuffer> value, RecordSerde<T> serde) {
        if (key.isPresent()) {
            throw new IllegalArgumentException("Got key in the record when no key was expected");
        }

        if (value.isEmpty()) {
            throw new IllegalArgumentException("Missing value in the record when a value was expected");
        } else if (value.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty value in the record");
        }

        ByteBuffer valueBuffer = value.get();

        return serde.read(new ByteBufferAccessor(valueBuffer), valueBuffer.remaining());
    }

    private static ControlRecord decodeControlRecord(Optional<ByteBuffer> key, Optional<ByteBuffer> value) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Missing key in the record when a key was expected");
        } else if (key.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty key in the record");
        }

        if (value.isEmpty()) {
            throw new IllegalArgumentException("Missing value in the record when a value was expected");
        } else if (value.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty value in the record");
        }

        return ControlRecord.of(key.get(), value.get());
    }
}
