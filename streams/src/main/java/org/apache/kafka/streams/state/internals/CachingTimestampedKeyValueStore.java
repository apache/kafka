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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.timestamp;

class CachingTimestampedKeyValueStore<K, V> extends CachingKeyValueStore<K, V> {

    CachingTimestampedKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying,
                                    final Serde<K> keySerde,
                                    final Serde<V> valueSerde) {
        super(underlying, keySerde, valueSerde);
    }

    @Override
    public <FK, FV> FlushEntry<FK, FV> flushEntry(final StateSerdes<FK, FV> serdes,
                                                  final byte[] rawValueAndTimestamp,
                                                  final byte[] oldRawValueAndTimestamp,
                                                  final long timestamp) {
        final FV value;
        final long valueTimestamp;

        if (rawValueAndTimestamp != null) {
            value = serdes.valueFrom(rawValue(rawValueAndTimestamp));
            valueTimestamp = timestamp(rawValueAndTimestamp);
        } else {
            value = null;
            valueTimestamp = ConsumerRecord.NO_TIMESTAMP;
        }

        final FV oldValue;
        if (oldRawValueAndTimestamp != null) {
            oldValue = serdes.valueFrom(rawValue(oldRawValueAndTimestamp));
        } else {
            oldValue = null;
        }

        return new FlushEntry<>(
            value,
            oldValue,
            valueTimestamp);
    }
}
