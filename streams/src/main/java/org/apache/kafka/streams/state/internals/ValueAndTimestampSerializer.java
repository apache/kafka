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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

public class ValueAndTimestampSerializer<V> implements WrappingNullableSerializer<ValueAndTimestamp<V>, Void, V> {
    public final Serializer<V> valueSerializer;
    private final Serializer<Long> timestampSerializer;

    ValueAndTimestampSerializer(final Serializer<V> valueSerializer) {
        Objects.requireNonNull(valueSerializer);
        this.valueSerializer = valueSerializer;
        timestampSerializer = new LongSerializer();
    }

    private static boolean skipTimestampAndCompareValues(final byte[] left, final byte[] right) {
        for (int i = Long.BYTES; i < left.length; i++) {
            if (left[i] != right[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean timeIsDecreasing(final byte[] oldRecord, final byte[] newRecord) {
        return extractTimestamp(newRecord) <= extractTimestamp(oldRecord);
    }

    private static long extractTimestamp(final byte[] bytes) {
        final byte[] timestampBytes = new byte[Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++) {
            timestampBytes[i] = bytes[i];
        }
        return ByteBuffer.wrap(timestampBytes).getLong();
    }

    /**
     * @param oldRecord  the serialized byte array of the old record in state store
     * @param newRecord the serialized byte array of the new record being processed
     * @return true if the two serialized values are the same (excluding timestamp) or
     *              if the timestamp of right is less than left (indicating out of order record)
     *         false otherwise
     */
    public static boolean compareValuesAndCheckForIncreasingTimestamp(final byte[] oldRecord, final byte[] newRecord) {
        if (oldRecord == newRecord) {
            // same reference, so they are trivially the same (might both be null)
            return true;
        } else if (oldRecord == null || newRecord == null) {
            // only one is null, so they cannot be the same
            return false;
        } else if (newRecord.length != oldRecord.length) {
            // they are different length, so they cannot be the same
            return false;
        } else if (timeIsDecreasing(oldRecord, newRecord)) {
            // the record time represents the beginning of the validity interval, so if the time
            // moves backwards, we need to do the update regardless of whether the value has changed
            return false;
        } else {
            // all other checks have fallen through, so we actually compare the binary data of the two values
            return skipTimestampAndCompareValues(oldRecord, newRecord);
        }
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        valueSerializer.configure(configs, isKey);
        timestampSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic,
                            final ValueAndTimestamp<V> data) {
        if (data == null) {
            return null;
        }
        return serialize(topic, data.value(), data.timestamp());
    }

    public byte[] serialize(final String topic,
                            final V data,
                            final long timestamp) {
        if (data == null) {
            return null;
        }
        final byte[] rawValue = valueSerializer.serialize(topic, data);

        // Since we can't control the result of the internal serializer, we make sure that the result
        // is not null as well.
        // Serializing non-null values to null can be useful when working with Optional-like values
        // where the Optional.empty case is serialized to null.
        // See the discussion here: https://github.com/apache/kafka/pull/7679
        if (rawValue == null) {
            return null;
        }

        final byte[] rawTimestamp = timestampSerializer.serialize(topic, timestamp);
        return ByteBuffer
            .allocate(rawTimestamp.length + rawValue.length)
            .put(rawTimestamp)
            .put(rawValue)
            .array();
    }

    @Override
    public void close() {
        valueSerializer.close();
        timestampSerializer.close();
    }

    @Override
    public void setIfUnset(final Serializer<Void> defaultKeySerializer, final Serializer<V> defaultValueSerializer) {
        // ValueAndTimestampSerializer never wraps a null serializer (or configure would throw),
        // but it may wrap a serializer that itself wraps a null serializer.
        initNullableSerializer(valueSerializer, defaultKeySerializer, defaultValueSerializer);
    }
}
