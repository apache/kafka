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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.headers;
import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.timestamp;

/**
 * Change-logging wrapper for window stores that support headers.
 * <p>
 * This class extends {@link ChangeLoggingWindowBytesStore} and correctly handles
 * the header-aware storage format: [headersSize(varint)][headersBytes][timestamp(8)][value]
 * <p>
 * Unlike {@link ChangeLoggingTimestampedWindowBytesStore} which uses
 * {@link ValueAndTimestampDeserializer} for the format [timestamp(8)][value],
 * this class uses {@link ValueTimestampHeadersDeserializer} to extract
 * the timestamp from the correct position in the byte array.
 */
public class ChangeLoggingTimestampedWindowBytesStoreWithHeaders extends ChangeLoggingWindowBytesStore {

    ChangeLoggingTimestampedWindowBytesStoreWithHeaders(final WindowStore<Bytes, byte[]> bytesStore,
                                                        final boolean retainDuplicates) {
        super(bytesStore, retainDuplicates, WindowKeySchema::toStoreKeyBinary);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueTimestampHeaders,
                    final long windowStartTimestamp) {
        if (valueTimestampHeaders == null) {
            // Deletion path - isolate with new context
            handleDelete(key, windowStartTimestamp);
        } else {
            // Normal put path
            wrapped().put(key, valueTimestampHeaders, windowStartTimestamp);
            log(keySerializer.serialize(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), valueTimestampHeaders);
        }
    }

    private void handleDelete(final Bytes key, final long windowStartTimestamp) {
        final ProcessorRecordContext currentContext = internalContext.recordContext();

        // Fetch old value to extract its headers (if exists)
        byte[] oldValueTimestampHeaders = null;
        try (final WindowStoreIterator<byte[]> iter = wrapped().fetch(key, windowStartTimestamp, windowStartTimestamp)) {
            if (iter.hasNext()) {
                oldValueTimestampHeaders = iter.next().value;
            }
        }

        // Create new headers object to isolate delete operation from input record
        // Copy headers to the new headers object:
        // - If old value exists, use its headers and timestamp
        // - Otherwise, use current context's headers and timestamp
        final Headers newHeaders = oldValueTimestampHeaders != null
            ? new RecordHeaders(headers(oldValueTimestampHeaders))
            : new RecordHeaders(currentContext.headers());

        final long timestampToUse = oldValueTimestampHeaders != null
            ? timestamp(oldValueTimestampHeaders)
            : currentContext.timestamp();

        // Create temporary context with new headers
        final ProcessorRecordContext temporaryContext =
            new ProcessorRecordContext(
                timestampToUse,
                currentContext.offset(),
                currentContext.partition(),
                currentContext.topic(),
                newHeaders
            );

        internalContext.setRecordContext(temporaryContext);

        try {
            wrapped().put(key, null, windowStartTimestamp);

            final Bytes changelogKey = keySerializer.serialize(key, windowStartTimestamp, maybeUpdateSeqnumForDups());
            internalContext.logChange(
                name(),
                changelogKey,
                null,  // tombstone
                temporaryContext.timestamp(),
                temporaryContext.headers(),
                wrapped().getPosition()
            );
        } finally {
            // Always restore original context
            internalContext.setRecordContext(currentContext);
        }
    }

    @Override
    void log(final Bytes key,
             final byte[] valueTimestampHeaders) {
        internalContext.logChange(
            name(),
            key,
            rawValue(valueTimestampHeaders),
            valueTimestampHeaders != null
                ? timestamp(valueTimestampHeaders)
                : internalContext.recordContext().timestamp(),
            valueTimestampHeaders != null
                ? headers(valueTimestampHeaders)
                : internalContext.recordContext().headers(),
            wrapped().getPosition()
        );
    }
}
