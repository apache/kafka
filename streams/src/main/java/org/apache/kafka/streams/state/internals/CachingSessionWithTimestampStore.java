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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

class CachingSessionWithTimestampStore<K, AGG> extends CachingSessionStore<K, AGG> {

    CachingSessionWithTimestampStore(final SessionStore<Bytes, byte[]> bytesStore,
                                     final Serde<K> keySerde,
                                     final Serde<AGG> aggSerde,
                                     final long segmentInterval) {
        super(bytesStore, keySerde, aggSerde, segmentInterval);
    }

    void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = cacheFunction.key(entry.key());
        final ProcessorRecordContext current = context.recordContext();
        context.setRecordContext(entry.entry().context());
        try {
            final Windowed<K> key = SessionKeySchema.from(binaryKey.get(), serdes.keyDeserializer(), topic);
            final Bytes rawKey = Bytes.wrap(serdes.rawKey(key.key()));
            if (flushListener != null) {
                final byte[] rawValueAndTimestamp = entry.newValue();
                final byte[] rawValue;
                if (rawValueAndTimestamp != null) {
                    rawValue = new byte[rawValueAndTimestamp.length - 8];
                    System.arraycopy(rawValueAndTimestamp, 8, rawValue, 0, rawValue.length);
                } else {
                    rawValue = null;
                }
                final AGG newValue = serdes.valueFrom(rawValue);
                final AGG oldValue = newValue == null || sendOldValues ? fetchPrevious(rawKey, key.window()) : null;
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(key, newValue, oldValue, entry.entry().context().timestamp());
                }
            }
            bytesStore.put(new Windowed<>(rawKey, key.window()), entry.newValue());
        } finally {
            context.setRecordContext(current);
        }
    }

    @Override
    AGG fetchPrevious(final Bytes rawKey, final Window window) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore.findSessions(rawKey, window.start(), window.end())) {
            if (!iterator.hasNext()) {
                return null;
            }
            final byte[] rawValueAndTimestamp = iterator.next().value;
            final byte[] rawValue = new byte[rawValueAndTimestamp.length - 8];
            System.arraycopy(rawValueAndTimestamp, 8, rawValue, 0, rawValue.length);
            return serdes.valueFrom(rawValue);
        }
    }

}
