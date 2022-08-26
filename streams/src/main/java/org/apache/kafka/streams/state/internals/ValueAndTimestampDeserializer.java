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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

class ValueAndTimestampDeserializer<V> implements WrappingNullableDeserializer<ValueAndTimestamp<V>, Void, V> {
    private final static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    public final Deserializer<V> valueDeserializer;
    private final Deserializer<Long> timestampDeserializer;

    ValueAndTimestampDeserializer(final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
        timestampDeserializer = new LongDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        valueDeserializer.configure(configs, isKey);
        timestampDeserializer.configure(configs, isKey);
    }

    @Override
    public ValueAndTimestamp<V> deserialize(final String topic,
                                            final byte[] valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            return null;
        }

        final long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp(valueAndTimestamp));
        final V value = valueDeserializer.deserialize(topic, rawValue(valueAndTimestamp));
        return ValueAndTimestamp.make(value, timestamp);
    }

    @Override
    public void close() {
        valueDeserializer.close();
        timestampDeserializer.close();
    }

    static byte[] rawValue(final byte[] rawValueAndTimestamp) {
        final int rawValueLength = rawValueAndTimestamp.length - 8;
        return ByteBuffer
            .allocate(rawValueLength)
            .put(rawValueAndTimestamp, 8, rawValueLength)
            .array();
    }

    private static byte[] rawTimestamp(final byte[] rawValueAndTimestamp) {
        return ByteBuffer
            .allocate(8)
            .put(rawValueAndTimestamp, 0, 8)
            .array();
    }

    static long timestamp(final byte[] rawValueAndTimestamp) {
        return LONG_DESERIALIZER.deserialize(null, rawTimestamp(rawValueAndTimestamp));
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // ValueAndTimestampDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(valueDeserializer, getter);
    }
}