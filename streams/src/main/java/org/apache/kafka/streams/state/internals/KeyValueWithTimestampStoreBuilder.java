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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Arrays;
import java.util.Map;

public class KeyValueWithTimestampStoreBuilder<K, V> extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, KeyValueStore<K, ValueAndTimestamp<V>>> {

    private final KeyValueBytesStoreSupplier storeSupplier;
    private final ValueAndTimestampSerde<V> valueAndTimestampSerde;

    public KeyValueWithTimestampStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                             final Serde<K> keySerde,
                                             final Serde<V> valueSerde,
                                             final Time time) {
        super(storeSupplier.name(), keySerde, new ValueAndTimestampSerde<>(valueSerde), time);
        valueAndTimestampSerde = (ValueAndTimestampSerde<V>) super.valueSerde;
        // note null check was pointless (since we already dereferenced storeSupplier)
        this.storeSupplier = storeSupplier;
    }

    @Override
    public KeyValueStore<K, ValueAndTimestamp<V>> build() {
        final KeyValueStore<Bytes, byte[]> storeWithTimestamps = storeSupplier.get();
        if (!(storeWithTimestamps instanceof StoreWithTimestamps)) {
            throw new IllegalStateException("the store should have been wrapped already");
        }

        return new MeteredKeyValueWithTimestampStore<>(
            maybeWrapCaching(maybeWrapLogging(storeWithTimestamps)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueAndTimestampSerde);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> innerStoreWithTimestamps) {
        if (!enableCaching) {
            return innerStoreWithTimestamps;
        }
        return new CachingKeyValueWithTimestampStore<>(innerStoreWithTimestamps, keySerde, valueAndTimestampSerde);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> innerStoreWithTimestamps) {
        if (!enableLogging) {
            return innerStoreWithTimestamps;
        }
        return new ChangeLoggingKeyValueWithTimestampBytesStore(innerStoreWithTimestamps);
    }



    // TODO: where should we move those classes (of can we keep them here)?

    public static class ValueAndTimestampSerde<V> implements Serde<ValueAndTimestamp<V>> {
        private ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
        private ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;
        private boolean initialized = false;
        private Serde<V> valueSerde;

        public ValueAndTimestampSerde(final Serde<V> valueSerde) {
            if (valueSerde != null) {
                valueAndTimestampSerializer = new ValueAndTimestampSerializer<>(valueSerde.serializer());
                valueAndTimestampDeserializer = new ValueAndTimestampDeserializer<>(valueSerde.deserializer());
                this.valueSerde = valueSerde;
                initialized = true;
            }
        }

        public ValueAndTimestampSerde init(final Serde<V> valueSerde) {
            valueAndTimestampSerializer = new ValueAndTimestampSerializer<>(valueSerde.serializer());
            valueAndTimestampDeserializer = new ValueAndTimestampDeserializer<>(valueSerde.deserializer());
            this.valueSerde = valueSerde;
            initialized = true;
            return this;
        }

        public boolean initialized() {
            return initialized;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            if (valueAndTimestampSerializer != null) {
                valueAndTimestampSerializer.configure(configs, isKey);
            }
            if (valueAndTimestampDeserializer != null) {
                valueAndTimestampDeserializer.configure(configs, isKey);
            }
        }

        @Override
        public void close() {
            if (valueAndTimestampSerializer != null) {
                valueAndTimestampSerializer.close();
            }
            if (valueAndTimestampDeserializer != null) {
                valueAndTimestampDeserializer.close();
            }
        }

        @Override
        public Serializer<ValueAndTimestamp<V>> serializer() {
            return valueAndTimestampSerializer;
        }

        @Override
        public Deserializer<ValueAndTimestamp<V>> deserializer() {
            return valueAndTimestampDeserializer;
        }

        public Serde<V> valueSerde() {
            return valueSerde;
        }

    }

    static class ValueAndTimestampSerializer<V> implements Serializer<ValueAndTimestamp<V>> {
        public final Serializer<V> valueSerializer;
        private final Serializer<Long> timestampSerializer;

        ValueAndTimestampSerializer(final Serializer<V> valueSerializer) {
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
            final byte[] rawValueAndTimestamp = new byte[rawTimestamp.length + rawValue.length];
            System.arraycopy(rawTimestamp, 0, rawValueAndTimestamp, 0, rawTimestamp.length);
            System.arraycopy(rawValue, 0, rawValueAndTimestamp, rawTimestamp.length, rawValue.length);
            return rawValueAndTimestamp;
        }

        @Override
        public void close() {
            valueSerializer.close();
            timestampSerializer.close();
        }
    }

    static class ValueAndTimestampDeserializer<V> implements Deserializer<ValueAndTimestamp<V>> {
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
            return ValueAndTimestamp.make(value, timestamp);
        }

        @Override
        public void close() {
            valueDeserializer.close();
            timestampDeserializer.close();

        }
    }


}