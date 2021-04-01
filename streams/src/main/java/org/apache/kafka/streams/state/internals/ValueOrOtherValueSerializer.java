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

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Serializes a {@link ValueOrOtherValue}. The serialized bytes starts with a byte that references
 * to whether the value is V1 or V2.
 */
public class ValueOrOtherValueSerializer<V1, V2> implements Serializer<ValueOrOtherValue<V1, V2>> {
    private final Serializer<V1> thisSerializer;
    private final Serializer<V2> otherSerializer;

    public ValueOrOtherValueSerializer(final Serializer<V1> thisSerializer, final Serializer<V2> otherSerializer) {
        this.thisSerializer = Objects.requireNonNull(thisSerializer);
        this.otherSerializer = Objects.requireNonNull(otherSerializer);
    }

    @Override
    public byte[] serialize(final String topic, final ValueOrOtherValue<V1, V2> data) {
        if (data == null) {
            return null;
        }

        final byte[] rawThisValue = (data.getThisValue() != null) ? thisSerializer.serialize(topic, data.getThisValue()) : null;
        final byte[] rawOtherValue = (data.getOtherValue() != null) ? otherSerializer.serialize(topic, data.getOtherValue()) : null;

        if (rawThisValue == null && rawOtherValue == null) {
            return null;
        }

        return ByteBuffer
            .allocate(1 + (rawThisValue != null ? rawThisValue.length : rawOtherValue.length))
            .put((byte) (rawThisValue != null ? 1 : 0)) // if true, then only store thisValue
            .put(rawThisValue != null ? rawThisValue : rawOtherValue)
            .array();
    }

    @Override
    public void close() {
        thisSerializer.close();
        otherSerializer.close();
    }
}
