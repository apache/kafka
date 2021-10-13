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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Map;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

/**
 * The deserializer that is used for {@link TimestampedKeyAndJoinSide}, which is a combo key format of <timestamp, left/right flag, raw-key>
 * @param <K> the raw key type
 */
public class TimestampedKeyAndJoinSideDeserializer<K> implements WrappingNullableDeserializer<TimestampedKeyAndJoinSide<K>, K, Void> {
    private Deserializer<K> keyDeserializer;
    private final Deserializer<Long> timestampDeserializer = new LongDeserializer();

    TimestampedKeyAndJoinSideDeserializer(final Deserializer<K> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final SerdeGetter getter) {
        if (keyDeserializer == null) {
            keyDeserializer = (Deserializer<K>) getter.keySerde().deserializer();
        }

        initNullableDeserializer(keyDeserializer, getter);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        keyDeserializer.configure(configs, isKey);
    }

    @Override
    public TimestampedKeyAndJoinSide<K> deserialize(final String topic, final byte[] data) {
        final boolean bool = data[StateSerdes.TIMESTAMP_SIZE] == 1;
        final K key = keyDeserializer.deserialize(topic, rawKey(data));
        final long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp(data));

        return TimestampedKeyAndJoinSide.make(bool, key, timestamp);
    }

    private byte[] rawTimestamp(final byte[] data) {
        final byte[] rawTimestamp = new byte[8];
        System.arraycopy(data, 0, rawTimestamp, 0, 8);
        return rawTimestamp;
    }

    private byte[] rawKey(final byte[] data) {
        final byte[] rawKey = new byte[data.length - StateSerdes.TIMESTAMP_SIZE - StateSerdes.BOOLEAN_SIZE];
        System.arraycopy(data, StateSerdes.TIMESTAMP_SIZE + StateSerdes.BOOLEAN_SIZE, rawKey, 0, rawKey.length);
        return rawKey;
    }

    @Override
    public void close() {
        keyDeserializer.close();
    }
}
