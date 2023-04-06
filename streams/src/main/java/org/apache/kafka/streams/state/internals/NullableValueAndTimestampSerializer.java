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

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;
import static org.apache.kafka.streams.state.internals.NullableValueAndTimestampSerde.RAW_BOOLEAN_LENGTH;
import static org.apache.kafka.streams.state.internals.NullableValueAndTimestampSerde.RAW_TIMESTAMP_LENGTH;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.common.serialization.BooleanSerializer;

/**
 * See {@link NullableValueAndTimestampSerde}.
 */
public class NullableValueAndTimestampSerializer<V> implements WrappingNullableSerializer<ValueAndTimestamp<V>, Void, V> {
    public final Serializer<V> valueSerializer;
    private final Serializer<Long> timestampSerializer;
    private final Serializer<Boolean> booleanSerializer;

    NullableValueAndTimestampSerializer(final Serializer<V> valueSerializer) {
        this.valueSerializer = Objects.requireNonNull(valueSerializer);
        timestampSerializer = new LongSerializer();
        booleanSerializer = new BooleanSerializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueSerializer.configure(configs, isKey);
        timestampSerializer.configure(configs, isKey);
        booleanSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final ValueAndTimestamp<V> data) {
        if (data == null) {
            return null;
        }
        final byte[] rawValue = valueSerializer.serialize(topic, data.value());
        final byte[] rawIsTombstone = booleanSerializer.serialize(topic, rawValue == null);
        final byte[] rawTimestamp = timestampSerializer.serialize(topic, data.timestamp());
        if (rawIsTombstone.length != RAW_BOOLEAN_LENGTH) {
            throw new SerializationException("Unexpected length for serialized boolean: " + rawIsTombstone.length);
        }
        if (rawTimestamp.length != RAW_TIMESTAMP_LENGTH) {
            throw new SerializationException("Unexpected length for serialized timestamp: " + rawTimestamp.length);
        }

        final byte[] nonNullRawValue = rawValue == null ? new byte[0] : rawValue;
        return ByteBuffer
            .allocate(RAW_TIMESTAMP_LENGTH + RAW_BOOLEAN_LENGTH + nonNullRawValue.length)
            .put(rawTimestamp)
            .put(rawIsTombstone)
            .put(nonNullRawValue)
            .array();
    }

    @Override
    public void close() {
        valueSerializer.close();
        timestampSerializer.close();
        booleanSerializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // NullableValueAndTimestampSerializer never wraps a null serializer (or configure would throw),
        // but it may wrap a serializer that itself wraps a null serializer.
        initNullableSerializer(valueSerializer, getter);
    }
}