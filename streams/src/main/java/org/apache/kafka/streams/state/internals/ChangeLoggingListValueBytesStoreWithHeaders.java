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
import org.apache.kafka.streams.state.KeyValueStore;

import static org.apache.kafka.streams.state.internals.Utils.headers;
import static org.apache.kafka.streams.state.internals.Utils.timestamp;

/**
 * Headers-aware change-logging wrapper for {@link ListValueStore}.
 * <p>
 * Each value passed to {@code put} is the wire-format produced by
 * {@link ValueTimestampHeadersSerializer}, i.e. a {@code [varint headersSize][headers][8B ts][value]}
 * blob representing the newly-appended list element. The inner {@link ListValueStore} appends
 * this blob to the per-key list and we then log the full updated list bytes back to the
 * changelog topic, using the timestamp and headers extracted from the newly-appended element
 * as the changelog record metadata.
 * <p>
 * For tombstones (value == null) we delete the whole list and propagate the current
 * record context's timestamp/headers, mirroring {@link ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders}.
 */
public class ChangeLoggingListValueBytesStoreWithHeaders extends ChangeLoggingKeyValueBytesStore {

    ChangeLoggingListValueBytesStoreWithHeaders(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void put(final Bytes key, final byte[] valueWithHeaders) {
        wrapped().put(key, valueWithHeaders);
        if (valueWithHeaders == null) {
            log(
                key,
                null,
                internalContext.recordContext().timestamp(),
                internalContext.recordContext().headers()
            );
        } else {
            log(
                key,
                wrapped().get(key),
                timestamp(valueWithHeaders),
                headers(valueWithHeaders)
            );
        }
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] valueWithHeaders) {
        final byte[] oldValue = wrapped().get(key);

        if (oldValue != null) {
            put(key, valueWithHeaders);
        }

        // TODO: here we always return null so that deser would not fail.
        //       we only do this since we know the only caller (stream-stream join processor)
        //       would not need the actual value at all
        return null;
    }
}
