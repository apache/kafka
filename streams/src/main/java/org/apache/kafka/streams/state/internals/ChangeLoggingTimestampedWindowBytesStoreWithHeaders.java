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

import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer.timestamp;

/**
 * ChangeLogging wrapper for timestamped window stores with headers support (KIP-1271).
 * <p>
 * This wrapper handles the ValueTimestampHeaders format: [HeaderSize][Headers][Timestamp][Value]
 * Headers are stored in the state store but NOT logged to the changelog topic, per KIP-1271 design.
 * <p>
 * The changelog only contains the key, value, and timestamp - headers are omitted as they are
 * already embedded in the state store value bytes.
 */
class ChangeLoggingTimestampedWindowBytesStoreWithHeaders extends ChangeLoggingWindowBytesStore {

    ChangeLoggingTimestampedWindowBytesStoreWithHeaders(final WindowStore<Bytes, byte[]> bytesStore,
                                                         final boolean retainDuplicates) {
        super(bytesStore, retainDuplicates, WindowKeySchema::toStoreKeyBinary);
    }

    @Override
    void log(final Bytes key, final byte[] valueTimestampHeaders) {
        // Per KIP-1271: headers are stored in state store but NOT in changelog
        // We need to extract the raw value and timestamp from the ValueTimestampHeaders format
        internalContext.logChange(
            name(),
            key,
            rawValue(valueTimestampHeaders),
            valueTimestampHeaders != null ? timestamp(valueTimestampHeaders) : internalContext.recordContext().timestamp(),
            wrapped().getPosition()
        );
    }
}
