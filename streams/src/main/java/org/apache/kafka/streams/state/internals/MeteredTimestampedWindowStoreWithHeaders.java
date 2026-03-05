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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

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
        final Headers headers = value == null || value.headers() == null ? new RecordHeaders() : value.headers();
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

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        // Get the result from parent which will have ValueTimestampHeaders<V>
        final QueryResult<R> result = super.query(query, positionBound, config);

        // Convert ValueTimestampHeaders<V> to ValueAndTimestamp<V> for backward compatibility
        if (result.isSuccess()) {
            final Object resultValue = result.getResult();
            if (resultValue instanceof WindowStoreIterator) {
                final WindowStoreIterator<ValueTimestampHeaders<V>> headersIterator =
                    (WindowStoreIterator<ValueTimestampHeaders<V>>) resultValue;
                final WindowStoreIterator<ValueAndTimestamp<V>> convertedIterator =
                    new ValueTimestampHeadersToValueAndTimestampWindowIterator<>(headersIterator);
                final QueryResult<R> convertedResult = (QueryResult<R>) QueryResult.forResult(convertedIterator);
                convertedResult.setPosition(result.getPosition());
                for (final String info : result.getExecutionInfo()) {
                    convertedResult.addExecutionInfo(info);
                }
                return convertedResult;
            } else if (resultValue instanceof KeyValueIterator) {
                final KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> headersIterator =
                    (KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>>) resultValue;
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> convertedIterator =
                    new ValueTimestampHeadersToValueAndTimestampKeyValueIterator<>(headersIterator);
                final QueryResult<R> convertedResult = (QueryResult<R>) QueryResult.forResult(convertedIterator);
                convertedResult.setPosition(result.getPosition());
                for (final String info : result.getExecutionInfo()) {
                    convertedResult.addExecutionInfo(info);
                }
                return convertedResult;
            }
        }
        return result;
    }

    /**
     * Iterator wrapper that converts ValueTimestampHeaders to ValueAndTimestamp.
     */
    private static class ValueTimestampHeadersToValueAndTimestampWindowIterator<V> implements WindowStoreIterator<ValueAndTimestamp<V>> {
        private final WindowStoreIterator<ValueTimestampHeaders<V>> inner;

        ValueTimestampHeadersToValueAndTimestampWindowIterator(final WindowStoreIterator<ValueTimestampHeaders<V>> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public Long peekNextKey() {
            return inner.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public org.apache.kafka.streams.KeyValue<Long, ValueAndTimestamp<V>> next() {
            final org.apache.kafka.streams.KeyValue<Long, ValueTimestampHeaders<V>> entry = inner.next();
            final ValueTimestampHeaders<V> vth = entry.value;
            final ValueAndTimestamp<V> vat = vth == null ? null :
                ValueAndTimestamp.make(vth.value(), vth.timestamp());
            return org.apache.kafka.streams.KeyValue.pair(entry.key, vat);
        }
    }

    /**
     * Iterator wrapper that converts ValueTimestampHeaders to ValueAndTimestamp.
     */
    private static class ValueTimestampHeadersToValueAndTimestampKeyValueIterator<K, V> implements KeyValueIterator<K, ValueAndTimestamp<V>> {
        private final KeyValueIterator<K, ValueTimestampHeaders<V>> inner;

        ValueTimestampHeadersToValueAndTimestampKeyValueIterator(final KeyValueIterator<K, ValueTimestampHeaders<V>> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public K peekNextKey() {
            return inner.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public org.apache.kafka.streams.KeyValue<K, ValueAndTimestamp<V>> next() {
            final org.apache.kafka.streams.KeyValue<K, ValueTimestampHeaders<V>> entry = inner.next();
            final ValueTimestampHeaders<V> vth = entry.value;
            final ValueAndTimestamp<V> vat = vth == null ? null :
                ValueAndTimestamp.make(vth.value(), vth.timestamp());
            return org.apache.kafka.streams.KeyValue.pair(entry.key, vat);
        }
    }
}
