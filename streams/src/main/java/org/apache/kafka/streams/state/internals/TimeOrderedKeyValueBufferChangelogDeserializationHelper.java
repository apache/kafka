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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

final class TimeOrderedKeyValueBufferChangelogDeserializationHelper {
    private TimeOrderedKeyValueBufferChangelogDeserializationHelper() {}

    static final class DeserializationResult {
        private final long time;
        private final Bytes key;
        private final BufferValue bufferValue;

        private DeserializationResult(final long time, final Bytes key, final BufferValue bufferValue) {
            this.time = time;
            this.key = key;
            this.bufferValue = bufferValue;
        }

        long time() {
            return time;
        }

        Bytes key() {
            return key;
        }

        BufferValue bufferValue() {
            return bufferValue;
        }
    }

    static DeserializationResult deserializeV0(final ConsumerRecord<byte[], byte[]> record,
                                               final Bytes key,
                                               final byte[] previousBufferedValue) {

        final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
        final long time = timeAndValue.getLong();
        final byte[] changelogValue = new byte[record.value().length - 8];
        timeAndValue.get(changelogValue);

        final Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(changelogValue));

        final ProcessorRecordContext recordContext = new ProcessorRecordContext(
            record.timestamp(),
            record.offset(),
            record.partition(),
            record.topic(),
            record.headers()
        );

        return new DeserializationResult(
            time,
            key,
            new BufferValue(
                previousBufferedValue == null ? change.oldValue : previousBufferedValue,
                change.oldValue,
                change.newValue,
                recordContext
            )
        );
    }

    static DeserializationResult deserializeV1(final ConsumerRecord<byte[], byte[]> record,
                                               final Bytes key,
                                               final byte[] previousBufferedValue) {
        final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
        final long time = timeAndValue.getLong();
        final byte[] changelogValue = new byte[record.value().length - 8];
        timeAndValue.get(changelogValue);

        final ContextualRecord contextualRecord = ContextualRecord.deserialize(ByteBuffer.wrap(changelogValue));
        final Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(contextualRecord.value()));

        return new DeserializationResult(
            time,
            key,
            new BufferValue(
                previousBufferedValue == null ? change.oldValue : previousBufferedValue,
                change.oldValue,
                change.newValue,
                contextualRecord.recordContext()
            )
        );
    }

    static DeserializationResult duckTypeV2(final ConsumerRecord<byte[], byte[]> record, final Bytes key) {
        DeserializationResult deserializationResult = null;
        RuntimeException v2DeserializationException = null;
        RuntimeException v3DeserializationException = null;
        try {
            deserializationResult = deserializeV2(record, key);
        } catch (final RuntimeException e) {
            v2DeserializationException = e;
        }
        // versions 2.4.0, 2.4.1, and 2.5.0 would have erroneously encoded a V3 record with the
        // V2 header, so we'll try duck-typing to see if this is decodable as V3
        if (deserializationResult == null) {
            try {
                deserializationResult = deserializeV3(record, key);
            } catch (final RuntimeException e) {
                v3DeserializationException = e;
            }
        }

        if (deserializationResult == null) {
            // ok, it wasn't V3 either. Throw both exceptions:
            final RuntimeException exception =
                new RuntimeException("Couldn't deserialize record as v2 or v3: " + record,
                                     v2DeserializationException);
            exception.addSuppressed(v3DeserializationException);
            throw exception;
        }
        return deserializationResult;
    }

    private static DeserializationResult deserializeV2(final ConsumerRecord<byte[], byte[]> record,
                                                       final Bytes key) {
        final ByteBuffer valueAndTime = ByteBuffer.wrap(record.value());
        final ContextualRecord contextualRecord = ContextualRecord.deserialize(valueAndTime);
        final Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(contextualRecord.value()));
        final byte[] priorValue = Utils.getNullableSizePrefixedArray(valueAndTime);
        final long time = valueAndTime.getLong();
        final BufferValue bufferValue = new BufferValue(priorValue, change.oldValue, change.newValue, contextualRecord.recordContext());
        return new DeserializationResult(time, key, bufferValue);
    }

    static DeserializationResult deserializeV3(final ConsumerRecord<byte[], byte[]> record, final Bytes key) {
        final ByteBuffer valueAndTime = ByteBuffer.wrap(record.value());
        final BufferValue bufferValue = BufferValue.deserialize(valueAndTime);
        final long time = valueAndTime.getLong();
        return new DeserializationResult(time, key, bufferValue);
    }
}