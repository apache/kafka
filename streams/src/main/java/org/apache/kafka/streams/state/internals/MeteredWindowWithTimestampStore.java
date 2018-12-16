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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowWithTimestampStore;

public class MeteredWindowWithTimestampStore<K, V> extends MeteredWindowStore<K, ValueAndTimestamp<V>> implements WindowWithTimestampStore<K, V> {

    private final LongSerializer longSerializer = new LongSerializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();

    MeteredWindowWithTimestampStore(final WindowStore<Bytes, byte[]> inner,
                                    final String metricScope,
                                    final Time time,
                                    final Serde<K> keySerde,
                                    final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }


    @Override
    public void put(final K key,
                    final ValueAndTimestamp<V> valueAndTimestamp,
                    final long windowStartTimestamp) {
        if (valueAndTimestamp != null) {
            put(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp(), windowStartTimestamp);
        } else {
            put(key, null, -1L, -1L);
        }
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long timestamp,
                    final long windowStartTimestamp) {
//        final long startNs = time.nanoseconds();
//        try {
//            inner.put(keyBytes(key), serdes.rawValue(value), windowStartTimestamp);
//        } catch (final ProcessorStateException e) {
//            final String message = String.format(e.getMessage(), key, value);
//            throw new ProcessorStateException(message, e);
//        } finally {
//            metrics.recordLatency(this.putTime, startNs, time.nanoseconds());
//        }
    }

}
