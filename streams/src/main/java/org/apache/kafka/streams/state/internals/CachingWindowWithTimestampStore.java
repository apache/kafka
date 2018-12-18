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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.WindowStore;

class CachingWindowWithTimestampStore<K, V> extends CachingWindowStore<K, V> {

    CachingWindowWithTimestampStore(final WindowStore<Bytes, byte[]> underlying,
                                    final Serde<K> keySerde,
                                    final Serde<V> valueSerde,
                                    final long windowSize,
                                    final long segmentInterval) {
        super(underlying, keySerde, valueSerde, windowSize, segmentInterval);
    }

    @Override
    void maybeForward(final ThreadCache.DirtyEntry entry,
                      final Bytes key,
                      final Windowed<K> windowedKey,
                      final InternalProcessorContext context) {
        if (flushListener != null) {
            final ProcessorRecordContext current = context.recordContext();
            context.setRecordContext(entry.entry().context());
            try {
                final V oldValue = sendOldValues ? fetchPrevious(key, windowedKey.window().start()) : null;
                final byte[] rawValueAndTimestamp = entry.newValue();
                final byte[] rawValue;
                if (rawValueAndTimestamp != null) {
                    rawValue = new byte[rawValueAndTimestamp.length - 8];
                    System.arraycopy(rawValueAndTimestamp, 8, rawValue, 0, rawValue.length);
                } else {
                    rawValue = null;
                }
                flushListener.apply(windowedKey, serdes.valueFrom(rawValue), oldValue, entry.entry().context().timestamp());
            } finally {
                context.setRecordContext(current);
            }
        }
    }

}
