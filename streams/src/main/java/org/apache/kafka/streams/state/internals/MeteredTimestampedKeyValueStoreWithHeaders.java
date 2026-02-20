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
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
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
    public void put(final K key,
                    final ValueTimestampHeaders<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            final Headers headers = value != null ? value.headers() : new RecordHeaders();
            maybeMeasureLatency(() -> wrapped().put(keyBytes(key, headers), serdes.rawValue(value, headers)), time, putSensor);
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public ValueTimestampHeaders<V> putIfAbsent(final K key,
                         final ValueTimestampHeaders<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        final Headers headers = value != null ? value.headers() : new RecordHeaders();
        final ValueTimestampHeaders<V> currentValue = maybeMeasureLatency(
            () -> outerValue(wrapped().putIfAbsent(keyBytes(key, headers), serdes.rawValue(value, headers))),
            time,
            putIfAbsentSensor
        );
        maybeRecordE2ELatency();
        return currentValue;
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        throw new UnsupportedOperationException("Querying is not supported for " + getClass().getSimpleName());
    }

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("Position is not supported for " + getClass().getSimpleName());
    }

    protected Bytes keyBytes(final K key, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }

}