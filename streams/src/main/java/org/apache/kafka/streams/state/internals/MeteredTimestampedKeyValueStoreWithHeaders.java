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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;


import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A Metered {@link TimestampedKeyValueStoreWithHeaders} wrapper that is used for recording operation metrics, and hence
 * its inner KeyValueStore implementation does not need to provide its own metrics collecting functionality.
 *
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes, byte[]&gt;,
 * hence we use {@link Serde}s to convert from &lt;K, ValueTimestampHeaders&lt;V&gt;&gt; to &lt;Bytes, byte[]&gt;.
 *
 * @param <K> key type
 * @param <V> value type (wrapped in {@link ValueTimestampHeaders})
 */
public class MeteredTimestampedKeyValueStoreWithHeaders<K, V>
    extends MeteredKeyValueStore<K, ValueTimestampHeaders<V>>
    implements TimestampedKeyValueStoreWithHeaders<K, V> {

  MeteredTimestampedKeyValueStoreWithHeaders(final KeyValueStore<Bytes, byte[]> inner,
                                             final String metricScope,
                                             final Time time,
                                             final Serde<K> keySerde,
                                             final Serde<ValueTimestampHeaders<V>> valueSerde) {
    super(inner, metricScope, time, keySerde, valueSerde);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Serde<ValueTimestampHeaders<V>> prepareValueSerdeForStore(final Serde<ValueTimestampHeaders<V>> valueSerde,
                                                                      final SerdeGetter getter) {
    if (valueSerde == null) {
      return new ValueTimestampHeadersSerde<>((Serde<V>) getter.valueSerde());
    } else {
      return super.prepareValueSerdeForStore(valueSerde, getter);
    }
  }

  @Override
  public ValueTimestampHeaders<V> get(final K key) {
    Objects.requireNonNull(key, "key cannot be null");
    try {
      return maybeMeasureLatency(() -> outerValue(wrapped().get(keyBytes(key))), time, getSensor);
    } catch (final ProcessorStateException e) {
      final String message = String.format(e.getMessage(), key);
      throw new ProcessorStateException(message, e);
    }
  }

  protected ValueTimestampHeaders<V> outerValue(final byte[] value) {
    Headers headers =
        HeadersDeserializer.deserialize(ValueTimestampHeadersDeserializer.rawHeaders(value));
    return value != null ? serdes.valueFrom(value, headers) : null;
  }

  /**
   * Returns both the raw serialized bytes and the deserialized {@link ValueTimestampHeaders}.
   */
  public RawAndDeserializedValue<V> getWithBinary(final K key, final Headers headers) {
    try {
      return maybeMeasureLatency(() -> {
        final byte[] serializedValue = wrapped().get(keyBytes(key, headers));
        return new RawAndDeserializedValue<>(serializedValue, outerValue(serializedValue));
      }, time, getSensor);
    } catch (final ProcessorStateException e) {
      final String message = String.format(e.getMessage(), key);
      throw new ProcessorStateException(message, e);
    }
  }

  /**
   * Only writes if the new serialized value differs (and timestamp increases) from the old serialized value.
   */
  public boolean putIfDifferentValues(final K key,
                                      final Headers headers,
                                      final ValueTimestampHeaders<V> newValue,
                                      final byte[] oldSerializedValue) {
    try {
      return maybeMeasureLatency(
          () -> {
            final byte[] newSerializedValue = serdes.rawValue(newValue, headers);
            if (ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
                oldSerializedValue,
                newSerializedValue
            )) {
              return false;
            } else {
              wrapped().put(keyBytes(key, headers), newSerializedValue);
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
    final ValueTimestampHeaders<ValueType> value;

    RawAndDeserializedValue(final byte[] serializedValue,
                            final ValueTimestampHeaders<ValueType> value) {
      this.serializedValue = serializedValue;
      this.value = value;
    }
  }
}