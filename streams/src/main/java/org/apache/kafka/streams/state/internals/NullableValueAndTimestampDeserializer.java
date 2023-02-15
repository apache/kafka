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

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.NullableValueAndTimestampSerde.BooleanSerde.BooleanDeserializer;

/**
 * See {@link NullableValueAndTimestampSerde}.
 */
public class NullableValueAndTimestampDeserializer<V> implements WrappingNullableDeserializer<ValueAndTimestamp<V>, Void, V> {
    public final Deserializer<V> valueDeserializer;
    private final Deserializer<Long> timestampDeserializer;
    private final Deserializer<Boolean> booleanDeserializer;

    NullableValueAndTimestampDeserializer(final Deserializer<V> valueDeserializer) {
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer);
        timestampDeserializer = new LongDeserializer();
        booleanDeserializer = new BooleanDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueDeserializer.configure(configs, isKey);
        timestampDeserializer.configure(configs, isKey);
        booleanDeserializer.configure(configs, isKey);
    }

    @Override
    public ValueAndTimestamp<V> deserialize(final String topic, final byte[] rawValueAndTimestamp) {
        if (rawValueAndTimestamp == null) {
            return null;
        }

        final long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp(rawValueAndTimestamp));
        final boolean isTombstone = booleanDeserializer.deserialize(topic, rawIsTombstone(rawValueAndTimestamp));
        if (isTombstone) {
            return ValueAndTimestamp.makeAllowNullable(null, timestamp);
        } else {
            final V value = valueDeserializer.deserialize(topic, rawValue(rawValueAndTimestamp));
            return ValueAndTimestamp.makeAllowNullable(value, timestamp);
        }
    }

    @Override
    public void close() {
        valueDeserializer.close();
        timestampDeserializer.close();
        booleanDeserializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // NullableValueAndTimestampDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(valueDeserializer, getter);
    }

    private static byte[] rawTimestamp(final byte[] rawValueAndTimestamp) {
        final byte[] rawTimestamp = new byte[8];
        System.arraycopy(rawValueAndTimestamp, 0, rawTimestamp, 0, 8);
        return rawTimestamp;
    }

    private static byte[] rawIsTombstone(final byte[] rawValueAndTimestamp) {
        final byte[] rawIsTombstone = new byte[1];
        System.arraycopy(rawValueAndTimestamp, 8, rawIsTombstone, 0, 1);
        return rawIsTombstone;
    }

    private static byte[] rawValue(final byte[] rawValueAndTimestamp) {
        final int rawValueLength = rawValueAndTimestamp.length - 9;
        final byte[] rawValue = new byte[rawValueLength];
        System.arraycopy(rawValueAndTimestamp, 9, rawValue, 0, rawValueLength);
        return rawValue;
    }
}