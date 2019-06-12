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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class ValueAndTimestampSerializer<V> implements Serializer<ValueAndTimestamp<V>> {
    public final Serializer<V> valueSerializer;
    private final Serializer<Long> timestampSerializer;

    ValueAndTimestampSerializer(final Serializer<V> valueSerializer) {
        Objects.requireNonNull(valueSerializer);
        this.valueSerializer = valueSerializer;
        timestampSerializer = new LongSerializer();
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
}