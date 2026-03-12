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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Objects;
import java.util.function.Function;

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
public class MeteredTimestampedWindowStoreWithHeaders<K, V>
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
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        if (query instanceof WindowKeyQuery) {
            result = runWindowKeyQuery((WindowKeyQuery<K, ValueTimestampHeaders<V>>) query, positionBound, config);
        } else if (query instanceof WindowRangeQuery) {
            result = runWindowRangeQuery((WindowRangeQuery<K, ValueTimestampHeaders<V>>) query, positionBound, config);
        } else {
            result = wrapped().query(query, positionBound, config);
        }

        if (config.isCollectExecutionInfo()) {
            final String conversionType = isUnderlyingStoreTimestamped()
                ? "with conversion to ValueAndTimestamp"
                : "with extraction of plain values";
            result.addExecutionInfo(
                "Handled in " + getClass() + " " + conversionType + " in "
                    + (time.nanoseconds() - start) + "ns");
        }
        return result;
    }

    /**
     * Handles WindowKeyQuery by creating a MeteredWindowStoreIterator with conversion from
     * ValueTimestampHeaders to either ValueAndTimestamp<V> (for timestamped stores) or V (for non-timestamped stores).
     */
    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runWindowKeyQuery(final WindowKeyQuery<K, ValueTimestampHeaders<V>> query,
                                                 final PositionBound positionBound,
                                                 final QueryConfig config) {
        final QueryResult<R> queryResult;
        if (query.getTimeFrom().isPresent() && query.getTimeTo().isPresent()) {
            final WindowKeyQuery<Bytes, byte[]> rawKeyQuery =
                    WindowKeyQuery.withKeyAndWindowStartRange(
                        keyBytes(query.getKey(), new RecordHeaders()),
                        query.getTimeFrom().get(),
                        query.getTimeTo().get()
                    );
            final QueryResult<WindowStoreIterator<byte[]>> rawResult = wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                if (isUnderlyingStoreTimestamped()) {
                    // For timestamped stores, return ValueAndTimestamp<V>
                    final Function<byte[], ValueAndTimestamp<V>> valueFrom = bytes -> {
                        final ValueTimestampHeaders<V> vth = serdes.valueFrom(bytes, new RecordHeaders());
                        return vth == null ? null : ValueAndTimestamp.make(vth.value(), vth.timestamp());
                    };

                    final MeteredWindowStoreIterator<ValueAndTimestamp<V>> typedResult =
                            new MeteredWindowStoreIterator<>(
                                rawResult.getResult(),
                                fetchSensor,
                                iteratorDurationSensor,
                                streamsMetrics,
                                valueFrom,
                                time,
                                numOpenIterators,
                                openIterators
                            );
                    final QueryResult<MeteredWindowStoreIterator<ValueAndTimestamp<V>>> typedQueryResult =
                            InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                    queryResult = (QueryResult<R>) typedQueryResult;
                } else {
                    // For non-timestamped stores, return plain V
                    final Function<byte[], V> valueFrom = bytes -> {
                        final ValueTimestampHeaders<V> vth = serdes.valueFrom(bytes, new RecordHeaders());
                        return vth == null ? null : vth.value();
                    };

                    final MeteredWindowStoreIterator<V> typedResult =
                            new MeteredWindowStoreIterator<>(
                                rawResult.getResult(),
                                fetchSensor,
                                iteratorDurationSensor,
                                streamsMetrics,
                                valueFrom,
                                time,
                                numOpenIterators,
                                openIterators
                            );
                    final QueryResult<MeteredWindowStoreIterator<V>> typedQueryResult =
                            InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                    queryResult = (QueryResult<R>) typedQueryResult;
                }
            } else {
                queryResult = (QueryResult<R>) rawResult;
            }
        } else {
            queryResult = QueryResult.forFailure(
                    FailureReason.UNKNOWN_QUERY_TYPE,
                    "This store (" + getClass() + ") doesn't know how to execute"
                        + " the given query (" + query + ") because it only supports closed-range"
                        + " queries."
                        + " Contact the store maintainer if you need support for a new query type."
            );
        }
        return queryResult;
    }


    /**
     * Handles WindowRangeQuery by creating a MeteredWindowedKeyValueIterator with conversion from
     * ValueTimestampHeaders to either ValueAndTimestamp<V> (for timestamped stores) or V (for non-timestamped stores).
     */
    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runWindowRangeQuery(final WindowRangeQuery<K, ValueTimestampHeaders<V>> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig config) {
        final QueryResult<R> result;
        if (query.getTimeFrom().isPresent() && query.getTimeTo().isPresent()) {
            final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
                    WindowRangeQuery.withWindowStartRange(
                        query.getTimeFrom().get(),
                        query.getTimeTo().get()
                    );
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                    wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                final Function<byte[], K> keyFrom = bytes -> serdes.keyFrom(bytes, new RecordHeaders());

                if (isUnderlyingStoreTimestamped()) {
                    // For timestamped stores, return ValueAndTimestamp<V>
                    final Function<byte[], ValueAndTimestamp<V>> valueFrom = bytes -> {
                        final ValueTimestampHeaders<V> vth = serdes.valueFrom(bytes, new RecordHeaders());
                        return vth == null ? null : ValueAndTimestamp.make(vth.value(), vth.timestamp());
                    };

                    final MeteredWindowedKeyValueIterator<K, ValueAndTimestamp<V>> typedResult =
                            new MeteredWindowedKeyValueIterator<>(
                                rawResult.getResult(),
                                fetchSensor,
                                iteratorDurationSensor,
                                streamsMetrics,
                                keyFrom,
                                valueFrom,
                                time,
                                numOpenIterators,
                                openIterators
                            );
                    final QueryResult<MeteredWindowedKeyValueIterator<K, ValueAndTimestamp<V>>> typedQueryResult =
                            InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                    result = (QueryResult<R>) typedQueryResult;
                } else {
                    // For non-timestamped stores, return plain V
                    final Function<byte[], V> valueFrom = bytes -> {
                        final ValueTimestampHeaders<V> vth = serdes.valueFrom(bytes, new RecordHeaders());
                        return vth == null ? null : vth.value();
                    };

                    final MeteredWindowedKeyValueIterator<K, V> typedResult =
                            new MeteredWindowedKeyValueIterator<>(
                                rawResult.getResult(),
                                fetchSensor,
                                iteratorDurationSensor,
                                streamsMetrics,
                                keyFrom,
                                valueFrom,
                                time,
                                numOpenIterators,
                                openIterators
                            );
                    final QueryResult<MeteredWindowedKeyValueIterator<K, V>> typedQueryResult =
                            InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                    result = (QueryResult<R>) typedQueryResult;
                }
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else {
            result = QueryResult.forFailure(
                    FailureReason.UNKNOWN_QUERY_TYPE,
                    "This store (" + getClass() + ") doesn't know how to"
                        + " execute the given query (" + query + ") because"
                        + " WindowStores only supports WindowRangeQuery.withWindowStartRange."
                        + " Contact the store maintainer if you need support for a new query type."
            );
        }
        return result;
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetch(final K keyFrom,
                                                                         final K keyTo,
                                                                         final long timeFrom,
                                                                         final long timeTo) {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().fetch(
                keyBytes(keyFrom, new RecordHeaders()),
                keyBytes(keyTo, new RecordHeaders()),
                timeFrom,
                timeTo)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetch(final K keyFrom,
                                                                                 final K keyTo,
                                                                                 final long timeFrom,
                                                                                 final long timeTo) {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().backwardFetch(
                keyBytes(keyFrom, new RecordHeaders()),
                keyBytes(keyTo, new RecordHeaders()),
                timeFrom,
                timeTo)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchAll(final long timeFrom, final long timeTo) {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().fetchAll(timeFrom, timeTo)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchAll(final long timeFrom, final long timeTo) {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().backwardFetchAll(timeFrom, timeTo)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> all() {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().all()
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardAll() {
        return new MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            wrapped().backwardAll()
        );
    }

    private class MeteredTimestampedWindowStoreWithHeadersKeyValueIterator
        implements KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>>, MeteredIterator {

        private final KeyValueIterator<Windowed<Bytes>, byte[]> iter;
        private final long startNs;
        private final long startTimestampMs;
        private KeyValue<Windowed<K>, ValueTimestampHeaders<V>> cachedNext;

        private MeteredTimestampedWindowStoreWithHeadersKeyValueIterator(
            final KeyValueIterator<Windowed<Bytes>, byte[]> iter) {
            this.iter = iter;
            this.startNs = time.nanoseconds();
            this.startTimestampMs = time.milliseconds();
            numOpenIterators.increment();
            openIterators.add(this);
        }

        @Override
        public long startTimestamp() {
            return this.startTimestampMs;
        }

        @Override
        public boolean hasNext() {
            return cachedNext != null || iter.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, ValueTimestampHeaders<V>> next() {
            if (cachedNext != null) {
                final KeyValue<Windowed<K>, ValueTimestampHeaders<V>> result = cachedNext;
                cachedNext = null;
                return result;
            }

            final KeyValue<Windowed<Bytes>, byte[]> next = iter.next();

            if (next == null) {
                return null;
            }

            final ValueTimestampHeaders<V> valueTimestampHeaders = serdes.valueFrom(next.value, new RecordHeaders());
            final Headers headers = valueTimestampHeaders != null ? valueTimestampHeaders.headers() : new RecordHeaders();
            final K key = serdes.keyFrom(next.key.key().get(), headers);
            final Windowed<K> windowedKey = new Windowed<>(key, next.key.window());
            return KeyValue.pair(windowedKey, valueTimestampHeaders);
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                final long duration = time.nanoseconds() - startNs;
                fetchSensor.record(duration);
                iteratorDurationSensor.record(duration);
                numOpenIterators.decrement();
                openIterators.remove(this);
            }
        }

        @Override
        public Windowed<K> peekNextKey() {
            if (cachedNext == null) {
                cachedNext = next();
            }
            return cachedNext == null ? null : cachedNext.key;
        }
    }

    private boolean isUnderlyingStoreTimestamped() {
        Object store = wrapped();
        do {
            if (store instanceof TimestampedBytesStore
                    || store instanceof TimestampedToHeadersWindowStoreAdapter) {
                return true;
            }
            store = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        } while ((store instanceof WrappedStateStore));
        return store instanceof TimestampedBytesStore;
    }
}
