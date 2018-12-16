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

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;

class CachingKeyValueWithTimestampStore<K, V> extends CachingKeyValueStore<K, V> {
    private final LongDeserializer longDeserializer = new LongDeserializer();

    public CachingKeyValueWithTimestampStore(final KeyValueStore<Bytes, byte[]> underlying, final Serde<K> keySerde, final Serde<V> valueSerde) {
        super(underlying, keySerde, valueSerde);
    }

    void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final ProcessorRecordContext current = context.recordContext();
        try {
            context.setRecordContext(entry.entry().context());
            if (flushListener != null) {
                V oldValue = null;
                if (sendOldValues) {
                    final byte[] oldRawValueAndTimestamp = underlying.get(entry.key());
                    if (oldRawValueAndTimestamp != null) {
                        final byte[] oldRawValue = new byte[oldRawValueAndTimestamp.length - 8];
                        System.arraycopy(oldRawValueAndTimestamp, 8, oldRawValue, 0, oldRawValue.length);
                        oldValue = serdes.valueFrom(oldRawValue);
                    }
                }
                // we rely on underlying store to handle null new value bytes as deletes
                final byte[] rawValueAndTimestamp =  entry.newValue();
                final byte[] rawTimestamp;
                final byte[] rawValue;
                final long timestamp;
                if (rawValueAndTimestamp != null) {
                    rawTimestamp = new byte[8];
                    rawValue = new byte[rawValueAndTimestamp.length - 8];
                    System.arraycopy(rawValueAndTimestamp, 0, rawTimestamp, 0, 8);
                    System.arraycopy(rawValueAndTimestamp, 8, rawValue, 0, rawValue.length);
                    timestamp = longDeserializer.deserialize(null, rawTimestamp);
                } else {
                    rawTimestamp = null;
                    rawValue = null;
                    timestamp = entry.entry().context().timestamp();
                }

                underlying.put(entry.key(), rawValueAndTimestamp);
                flushListener.apply(
                    serdes.keyFrom(entry.key().get()),
                    serdes.valueFrom(rawValue),
                    oldValue,
                    timestamp);
            } else {
                underlying.put(entry.key(), entry.newValue());
            }
        } finally {
            context.setRecordContext(current);
        }
    }
}
