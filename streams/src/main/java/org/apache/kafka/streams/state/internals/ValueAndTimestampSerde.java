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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.Objects;

public class ValueAndTimestampSerde<V> implements Serde<ValueAndTimestamp<V>> {
    private final ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
    private final ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;

    public ValueAndTimestampSerde(final Serde<V> valueSerde) {
        Objects.requireNonNull(valueSerde);
        valueAndTimestampSerializer = new ValueAndTimestampSerializer<>(valueSerde.serializer());
        valueAndTimestampDeserializer = new ValueAndTimestampDeserializer<>(valueSerde.deserializer());
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        valueAndTimestampSerializer.configure(configs, isKey);
        valueAndTimestampDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        valueAndTimestampSerializer.close();
        valueAndTimestampDeserializer.close();
    }

    @Override
    public Serializer<ValueAndTimestamp<V>> serializer() {
        return valueAndTimestampSerializer;
    }

    @Override
    public Deserializer<ValueAndTimestamp<V>> deserializer() {
        return valueAndTimestampDeserializer;
    }
}