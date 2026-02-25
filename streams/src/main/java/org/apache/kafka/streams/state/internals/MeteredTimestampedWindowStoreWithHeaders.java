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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A Metered {@link TimestampedWindowStoreWithHeaders} wrapper that is used for recording operation metrics,
 * and hence its inner WindowStore implementation does not need to provide its own metrics collecting functionality.
 * The inner {@link WindowStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,ValueTimestampHeaders&lt;V&gt;&gt; to &lt;Bytes,byte[]&gt;.
 *
 * @param <K> key type
 * @param <V> value type
 */
class MeteredTimestampedWindowStoreWithHeaders<K, V>
    extends MeteredWindowStore<K, ValueTimestampHeaders<V>>
    implements TimestampedWindowStoreWithHeaders<K, V> {

    MeteredTimestampedWindowStoreWithHeaders(final WindowStore<Bytes, byte[]> inner,
                                             final long windowSizeMs,
                                             final String metricScope,
                                             final Time time,
                                             final Serde<K> keySerde,
                                             final Serde<ValueTimestampHeaders<V>> valueSerde) {
        super(inner, windowSizeMs, metricScope, time, keySerde, valueSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueTimestampHeaders<V>> prepareValueSerde(final Serde<ValueTimestampHeaders<V>> valueSerde, final SerdeGetter getter) {
        if (valueSerde == null) {
            return new ValueTimestampHeadersSerde<>((Serde<V>) getter.valueSerde());
        } else {
            return super.prepareValueSerde(valueSerde, getter);
        }
    }

    @Override
    public void put(final K key, final ValueTimestampHeaders<V> value, final long windowStartTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        final Headers headers = value.headers() == null ? new RecordHeaders() : value.headers();
        try {
            maybeMeasureLatency(
                () -> wrapped().put(keyBytes(key, headers), serdes.rawValue(value, headers), windowStartTimestamp),
                time,
                putSensor
            );
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    protected Bytes keyBytes(final K key, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }
}
