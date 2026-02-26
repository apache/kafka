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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;

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
class ChangeLoggingTimestampedWindowBytesStoreWithHeaders extends ChangeLoggingWindowBytesStore {

    ChangeLoggingTimestampedWindowBytesStoreWithHeaders(final WindowStore<Bytes, byte[]> bytesStore,
                                                        final boolean retainDuplicates) {
        super(bytesStore, retainDuplicates, WindowKeySchema::toStoreKeyBinary);
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
