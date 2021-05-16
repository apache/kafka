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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * A Metered {@link TimestampedKeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredTimestampedKeyValueStore<K, V>
    extends MeteredKeyValueStore<K, ValueAndTimestamp<V>> 
    implements TimestampedKeyValueStore<K, V> {

    MeteredTimestampedKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                                    final String metricScope,
                                    final Time time,
                                    final Serde<K> keySerde,
                                    final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueAndTimestamp<V>> prepareValueSerdeForStore(final Serde<ValueAndTimestamp<V>> valueSerde, final Serde<?> contextKeySerde, final Serde<?> contextValueSerde) {
        if (valueSerde == null) {
            return new ValueAndTimestampSerde<>((Serde<V>) contextValueSerde);
        } else {
            return super.prepareValueSerdeForStore(valueSerde, contextKeySerde, contextValueSerde);
        }
    }


    public RawAndDeserializedValue<V> getWithBinary(final K key) {
        try {
            return maybeMeasureLatency(() -> { 
                final byte[] serializedValue = wrapped().get(keyBytes(key));
                return new RawAndDeserializedValue<V>(serializedValue, outerValue(serializedValue));
            }, time, getSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    public boolean putIfDifferentValues(final K key,
                                        final ValueAndTimestamp<V> newValue,
                                        final byte[] oldSerializedValue) {
        try {
            return maybeMeasureLatency(
                () -> {
                    final byte[] newSerializedValue = serdes.rawValue(newValue);
                    if (ValueAndTimestampSerializer.valuesAreSameAndTimeIsIncreasing(oldSerializedValue, newSerializedValue)) {
                        return false;
                    } else {
                        wrapped().put(keyBytes(key), newSerializedValue);
                        return true;
                    }
                },
                time,
                putSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, newValue);
            throw new ProcessorStateException(message, e);
        }
    }

    static class RawAndDeserializedValue<ValueType> {
        final byte[] serializedValue;
        final ValueAndTimestamp<ValueType> value;
        RawAndDeserializedValue(final byte[] serializedValue, final ValueAndTimestamp<ValueType> value) {
            this.serializedValue = serializedValue;
            this.value = value;
        }
    }
}
