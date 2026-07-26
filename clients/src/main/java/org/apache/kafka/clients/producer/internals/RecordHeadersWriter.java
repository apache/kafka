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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.AbstractRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

/**
 * Abstraction over the two ways a record's headers can be supplied to the accumulator's append
 * flow: as a materialized {@link Header}{@code []} (the normal path) or as pre-serialized
 * wire-format bytes (the fast path, used when the headers are already serialized — e.g. Kafka
 * Streams changelog writes — so a deserialize/re-serialize round trip can be skipped).
 *
 * <p>This lets {@link RecordAccumulator}'s partitioning / batching / buffer-allocation loop stay
 * single-sourced: only the two header-dependent leaf operations — estimating the record size for
 * buffer allocation and appending to a {@link ProducerBatch} — dispatch on the concrete header
 * form. The {@code byte[]} implementation writes the bytes verbatim; the {@code Header[]}
 * implementation serializes per header as usual.
 */
interface RecordHeadersWriter {

    /**
     * Upper-bound estimate of the serialized record size, used to size a newly allocated batch
     * buffer. Mirrors {@link AbstractRecords#estimateSizeInBytesUpperBound} for the concrete
     * header form.
     */
    int estimateSizeInBytesUpperBound(Compression compression, byte[] key, byte[] value);

    /**
     * Append the record (with this writer's headers) to the given batch, returning the future or
     * {@code null} if the batch has no room.
     */
    FutureRecordMetadata tryAppend(ProducerBatch batch, long timestamp, byte[] key, byte[] value,
                                   Callback callback, long nowMs);

    /** Writer backed by a materialized {@link Header}{@code []}. */
    static RecordHeadersWriter of(final Header[] headers) {
        final Header[] nonNull = headers == null ? Record.EMPTY_HEADERS : headers;
        return new RecordHeadersWriter() {
            @Override
            public int estimateSizeInBytesUpperBound(final Compression compression, final byte[] key, final byte[] value) {
                return AbstractRecords.estimateSizeInBytesUpperBound(
                    RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, nonNull);
            }

            @Override
            public FutureRecordMetadata tryAppend(final ProducerBatch batch, final long timestamp, final byte[] key,
                                                  final byte[] value, final Callback callback, final long nowMs) {
                return batch.tryAppend(timestamp, key, value, nonNull, callback, nowMs);
            }
        };
    }

    /** Writer backed by pre-serialized header bytes in the Kafka record wire format. */
    static RecordHeadersWriter ofRaw(final byte[] rawSerializedHeaders) {
        final byte[] nonNull = rawSerializedHeaders == null ? new byte[0] : rawSerializedHeaders;
        return new RecordHeadersWriter() {
            @Override
            public int estimateSizeInBytesUpperBound(final Compression compression, final byte[] key, final byte[] value) {
                return AbstractRecords.estimateSizeInBytesUpperBound(
                    RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, nonNull);
            }

            @Override
            public FutureRecordMetadata tryAppend(final ProducerBatch batch, final long timestamp, final byte[] key,
                                                  final byte[] value, final Callback callback, final long nowMs) {
                return batch.tryAppendRawHeaders(timestamp, key, value, nonNull, callback, nowMs);
            }
        };
    }
}
