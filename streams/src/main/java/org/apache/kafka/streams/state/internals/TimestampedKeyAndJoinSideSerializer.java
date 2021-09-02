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

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * The serializer that is used for {@link TimestampedKeyAndJoinSide}, which is a combo key format of <timestamp, left/right flag, raw-key>
 * @param <K> the raw key type
 */
public class TimestampedKeyAndJoinSideSerializer<K> implements WrappingNullableSerializer<TimestampedKeyAndJoinSide<K>, K, Void> {
    private Serializer<K> keySerializer;
    private final Serializer<Long> timestampSerializer = new LongSerializer();

    TimestampedKeyAndJoinSideSerializer(final Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final SerdeGetter getter) {
        if (keySerializer == null) {
            keySerializer = (Serializer<K>) getter.keySerde().serializer();
        }

        initNullableSerializer(keySerializer, getter);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        keySerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final TimestampedKeyAndJoinSide<K> data) {
        final byte boolByte = (byte) (data.isLeftSide() ? 1 : 0);
        final byte[] keyBytes = keySerializer.serialize(topic, data.getKey());
        final byte[] timestampBytes = timestampSerializer.serialize(topic, data.getTimestamp());

        return ByteBuffer
            .allocate(timestampBytes.length + 1 + keyBytes.length)
            .put(timestampBytes)
            .put(boolByte)
            .put(keyBytes)
            .array();
    }

    @Override
    public void close() {
        keySerializer.close();
    }
}
