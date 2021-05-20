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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * Serializes a {@link KeyAndJoinSide}. The serialized bytes starts with a byte that references
 * to the join side of the key followed by the key in bytes.
 */
public class KeyAndJoinSideSerializer<K> implements WrappingNullableSerializer<KeyAndJoinSide<K>, K, Void> {
    private Serializer<K> keySerializer;

    KeyAndJoinSideSerializer(final Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public void setIfUnset(final Serializer<K> defaultKeySerializer, final Serializer<Void> defaultValueSerializer) {
        if (keySerializer == null) {
            keySerializer = Objects.requireNonNull(defaultKeySerializer, "defaultKeySerializer cannot be null");
        }

        initNullableSerializer(keySerializer, defaultKeySerializer, defaultValueSerializer);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        keySerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final KeyAndJoinSide<K> data) {
        final byte boolByte = (byte) (data.isLeftSide() ? 1 : 0);
        final byte[] keyBytes = keySerializer.serialize(topic, data.getKey());

        return ByteBuffer
            .allocate(keyBytes.length + 1)
            .put(boolByte)
            .put(keyBytes)
            .array();
    }

    @Override
    public void close() {
        keySerializer.close();
    }
}
