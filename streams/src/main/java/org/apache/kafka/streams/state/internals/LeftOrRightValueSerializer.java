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
 * Serializes a {@link LeftOrRightValue}. The serialized bytes starts with a byte that references
 * to whether the value is V1 or V2.
 */
public class LeftOrRightValueSerializer<V1, V2> implements WrappingNullableSerializer<LeftOrRightValue<V1, V2>, Void, Object> {
    private Serializer<V1> leftSerializer;
    private Serializer<V2> rightSerializer;

    public LeftOrRightValueSerializer(final Serializer<V1> leftSerializer, final Serializer<V2> rightSerializer) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final Serializer<Void> defaultKeySerializer, final Serializer<Object> defaultValueSerializer) {
        if (leftSerializer == null) {
            leftSerializer = (Serializer<V1>) Objects.requireNonNull(defaultValueSerializer, "defaultValueSerializer cannot be null");
        }

        if (rightSerializer == null) {
            rightSerializer = (Serializer<V2>) Objects.requireNonNull(defaultValueSerializer, "defaultValueSerializer cannot be null");
        }

        initNullableSerializer(leftSerializer, defaultKeySerializer, defaultValueSerializer);
        initNullableSerializer(rightSerializer, defaultKeySerializer, defaultValueSerializer);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        leftSerializer.configure(configs, isKey);
        rightSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final LeftOrRightValue<V1, V2> data) {
        if (data == null) {
            return null;
        }

        final byte[] rawValue = (data.getLeftValue() != null)
            ? leftSerializer.serialize(topic, data.getLeftValue())
            : rightSerializer.serialize(topic, data.getRightValue());

        if (rawValue == null) {
            return null;
        }

        return ByteBuffer
            .allocate(1 + rawValue.length)
            .put((byte) (data.getLeftValue() != null ? 1 : 0))
            .put(rawValue)
            .array();
    }

    @Override
    public void close() {
        leftSerializer.close();
        rightSerializer.close();
    }
}
