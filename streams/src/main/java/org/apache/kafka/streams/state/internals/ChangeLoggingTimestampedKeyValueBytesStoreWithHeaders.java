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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.headers;
import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.timestamp;

/**
 * Change-logging wrapper for a timestamped key-value bytes store whose values also carry headers.
 * <p>
 * the header-aware serialized value format produced by {@link ValueTimestampHeadersSerializer}.
 * <p>
 * Semantics:
 *  - The inner store value format is:
 *        [ varint header_length ][ header_bytes ][ 8-byte timestamp ][ value_bytes ]
 *  - The changelog record value logged via {@code log(...)} remains just {@code value_bytes}
 *    (no timestamp, no headers), and the timestamp is logged separately.
 */
public class ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders
    extends ChangeLoggingKeyValueBytesStore {

    ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public byte[] delete(final Bytes key) {
        final ProcessorRecordContext currentContext = internalContext.recordContext();
        final byte[] oldValue = wrapped().get(key);

        // Copy headers to the new headers object:
        // - If old value exists, use its headers
        // - Otherwise, use current context's headers
        // Key doesn't exist - use current context headers
        final RecordHeaders newHeaders = oldValue != null
            ? new RecordHeaders(headers(oldValue))
            : new RecordHeaders(currentContext.headers());

        // Create temporary context with new headers to avoid polluting input record's context
        final ProcessorRecordContext temporaryContext =
            new ProcessorRecordContext(
                oldValue != null ? timestamp(oldValue) : currentContext.timestamp(),
                currentContext.offset(),
                currentContext.partition(),
                currentContext.topic(),
                newHeaders
            );

        internalContext.setRecordContext(temporaryContext);

        try {
            final byte[] deletedValue = wrapped().delete(key);

            // Log with null value - will use temporary.headers() which we prepared above
            log(key, null, temporaryContext.timestamp(), temporaryContext.headers());
            return deletedValue;
        } finally {
            // Always restore original context so user code never notices
            internalContext.setRecordContext(currentContext);
        }
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueTimestampHeaders) {
        if (valueTimestampHeaders == null) {
            // Deletion path (put with null) - use same logic as delete()
            delete(key);
        } else {
            // Normal put path
            wrapped().put(key, valueTimestampHeaders);
            log(
                key,
                rawValue(valueTimestampHeaders),
                timestamp(valueTimestampHeaders),
                headers(valueTimestampHeaders)
            );
        }
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueTimestampHeaders) {
        final byte[] previous = wrapped().putIfAbsent(key, valueTimestampHeaders);
        if (previous == null) {
            // then it was absent
            log(
                key,
                rawValue(valueTimestampHeaders),
                valueTimestampHeaders == null
                    ? internalContext.recordContext().timestamp()
                    : timestamp(valueTimestampHeaders),
                valueTimestampHeaders == null
                    ? internalContext.recordContext().headers()
                    : headers(valueTimestampHeaders)
            );
        }
        return previous;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        wrapped().putAll(entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueTimestampHeaders = entry.value;
            log(
                entry.key,
                rawValue(valueTimestampHeaders),
                valueTimestampHeaders == null
                    ? internalContext.recordContext().timestamp()
                    : timestamp(valueTimestampHeaders),
                valueTimestampHeaders == null
                    ? internalContext.recordContext().headers()
                    : headers(valueTimestampHeaders)
            );
        }
    }
}