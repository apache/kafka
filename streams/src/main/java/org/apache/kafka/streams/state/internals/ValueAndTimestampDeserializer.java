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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Arrays;
import java.util.Map;

class ValueAndTimestampDeserializer<V> implements Deserializer<ValueAndTimestamp<V>> {
    public final Deserializer<V> valueDeserializer;
    private final Deserializer<Long> timestampDeserializer;

    ValueAndTimestampDeserializer(final Deserializer<V> valueDeserializer) {
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
                                            final byte[] data) {
        if (data == null) {
            return null;
        }
        final long timestamp = timestampDeserializer.deserialize(topic, Arrays.copyOfRange(data, 0, 8));
        final V value = valueDeserializer.deserialize(topic, Arrays.copyOfRange(data, 8, data.length));
        return new ValueAndTimestampImpl<>(value, timestamp);
    }

    @Override
    public void close() {
        valueDeserializer.close();
        timestampDeserializer.close();

    }
}
