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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
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

    @SuppressWarnings("rawtypes")
    private final Map<Class, StoreQueryUtils.QueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                KeyQuery.class,
                (query, positionBound, config, store) -> runKeyQuery(query, positionBound, config)
            ),
            mkEntry(
                TimestampedKeyQuery.class,
                (query, positionBound, config, store) -> runTimestampedKeyQuery(query, positionBound, config)
            ),
            mkEntry(
                RangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            ),
            mkEntry(
                TimestampedRangeQuery.class,
                (query, positionBound, config, store) -> runTimestampedRangeQuery(query, positionBound, config)
            )
        );

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
            () -> deserializeValue(wrapped().putIfAbsent(keyBytes(key, headers), serdes.rawValue(value, headers))),
            time,
            putIfAbsentSensor
        );
        maybeRecordE2ELatency();
        return currentValue;
    }

    /**
     * Executes a query against this store.
     *
     * <p>Note: Query results do NOT include headers, even though headers are
     * preserved in the underlying store. This behavior provides compatibility
     * with existing IQv2 APIs that operate on timestamped stores.
     *
     * @param query the query to execute
     * @param positionBound the position bound
     * @param config the query configuration
     * @return the query result
     */

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        final long start = time.nanoseconds();
        final QueryResult<R> result;

        final StoreQueryUtils.QueryHandler handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            result = wrapped().query(query, positionBound, config);
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " in " + (time.nanoseconds() - start) + "ns");
            }
        } else {
            result = (QueryResult<R>) handler.apply(
                query,
                positionBound,
                config,
                this
            );
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo("Handled in " + getClass() + " with serdes "
                        + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                           final PositionBound positionBound,
                                           final QueryConfig config) {
        final QueryResult<R> result;
        final KeyQuery<K, V> typedKeyQuery = (KeyQuery<K, V>) query;
        final KeyQuery<Bytes, byte[]> rawKeyQuery =
            KeyQuery.withKey(keyBytes(typedKeyQuery.getKey(), new RecordHeaders()));
        final QueryResult<byte[]> rawResult =
            wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final Function<byte[], ValueTimestampHeaders<V>> deserializer = StoreQueryUtils.deserializeValue(serdes, wrapped());
            final ValueTimestampHeaders<V> valueTimestampHeaders = deserializer.apply(rawResult.getResult());
            final V plainValue = valueTimestampHeaders == null ? null : valueTimestampHeaders.value();
            final QueryResult<V> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, plainValue);
            result = (QueryResult<R>) typedQueryResult;
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runTimestampedKeyQuery(final Query<R> query,
                                                      final PositionBound positionBound,
                                                      final QueryConfig config) {
        final QueryResult<R> result;
        final TimestampedKeyQuery<K, V> typedKeyQuery = (TimestampedKeyQuery<K, V>) query;
        final KeyQuery<Bytes, byte[]> rawKeyQuery =
                KeyQuery.withKey(keyBytes(typedKeyQuery.key(), new RecordHeaders()));
        final QueryResult<byte[]> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final Function<byte[], ValueTimestampHeaders<V>> deserializer = StoreQueryUtils.deserializeValue(serdes, wrapped());
            final ValueTimestampHeaders<V> valueTimestampHeaders = deserializer.apply(rawResult.getResult());
            // Convert ValueTimestampHeaders to ValueAndTimestamp for the result
            final ValueAndTimestamp<V> valueAndTimestamp = valueTimestampHeaders == null
                    ? null
                    : ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp());
            final QueryResult<ValueAndTimestamp<V>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, valueAndTimestamp);
            result = (QueryResult<R>) typedQueryResult;
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                             final PositionBound positionBound,
                                             final QueryConfig config) {

        final QueryResult<R> result;
        final RangeQuery<K, V> typedQuery = (RangeQuery<K, V>) query;
        RangeQuery<Bytes, byte[]> rawRangeQuery;
        final ResultOrder order = typedQuery.resultOrder();
        rawRangeQuery = RangeQuery.withRange(
                keyBytes(typedQuery.getLowerBound().orElse(null), new RecordHeaders()),
                keyBytes(typedQuery.getUpperBound().orElse(null), new RecordHeaders())
        );
        if (order.equals(ResultOrder.DESCENDING)) {
            rawRangeQuery = rawRangeQuery.withDescendingKeys();
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            rawRangeQuery = rawRangeQuery.withAscendingKeys();
        }
        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult =
            wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final KeyValueIterator<K, V> resultIterator = new MeteredTimestampedKeyValueStoreWithHeadersIterator(
                    iterator,
                    getSensor,
                    StoreQueryUtils.deserializeValue(serdes, wrapped()),
                    true
            );
            final QueryResult<KeyValueIterator<K, V>> typedQueryResult =
                InternalQueryResultUtil.copyAndSubstituteDeserializedResult(
                        rawResult,
                        resultIterator
                );
            result = (QueryResult<R>) typedQueryResult;
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runTimestampedRangeQuery(final Query<R> query,
                                                        final PositionBound positionBound,
                                                        final QueryConfig config) {

        final QueryResult<R> result;
        final TimestampedRangeQuery<K, V> typedQuery = (TimestampedRangeQuery<K, V>) query;
        RangeQuery<Bytes, byte[]> rawRangeQuery;
        final ResultOrder order = typedQuery.resultOrder();
        rawRangeQuery = RangeQuery.withRange(
                keyBytes(typedQuery.lowerBound().orElse(null), new RecordHeaders()),
                keyBytes(typedQuery.upperBound().orElse(null), new RecordHeaders())
        );
        if (order.equals(ResultOrder.DESCENDING)) {
            rawRangeQuery = rawRangeQuery.withDescendingKeys();
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            rawRangeQuery = rawRangeQuery.withAscendingKeys();
        }
        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult =
                wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final KeyValueIterator<K, ValueAndTimestamp<V>> resultIterator =
                    (KeyValueIterator<K, ValueAndTimestamp<V>>) new MeteredTimestampedKeyValueStoreWithHeadersIterator(
                            iterator,
                            getSensor,
                            StoreQueryUtils.deserializeValue(serdes, wrapped()),
                            false
            );
            final QueryResult<KeyValueIterator<K, ValueAndTimestamp<V>>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(
                            rawResult,
                            resultIterator
                    );
            result = (QueryResult<R>) typedQueryResult;
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScan(final P prefix,
                                                                                                  final PS prefixKeySerializer) {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        return new MeteredValueTimestampHeadersIterator(
            wrapped().prefixScan(prefix, prefixKeySerializer),
            prefixScanSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from,
                                                               final K to) {
        return new MeteredValueTimestampHeadersIterator(
            wrapped().range(
                keyBytes(from, new RecordHeaders()),
                keyBytes(to, new RecordHeaders())
            ),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRange(final K from,
                                                                      final K to) {
        return new MeteredValueTimestampHeadersIterator(
            wrapped().reverseRange(
                keyBytes(from, new RecordHeaders()),
                keyBytes(to, new RecordHeaders())
            ),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
        return new MeteredValueTimestampHeadersIterator(
            wrapped().all(),
            allSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAll() {
        return new MeteredValueTimestampHeadersIterator(
            wrapped().reverseAll(),
            allSensor
        );
    }

    @SuppressWarnings("unchecked")
    private class MeteredTimestampedKeyValueStoreWithHeadersIterator implements KeyValueIterator<K, V>, MeteredIterator {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;
        private final long startTimestampMs;
        private final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer;

        private final boolean returnPlainValue;
        private KeyValue<K, V> cachedNext;

        private MeteredTimestampedKeyValueStoreWithHeadersIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                                                   final Sensor sensor,
                                                                   final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer,
                                                                   final boolean returnPlainValue) {
            this.iter = iter;
            this.sensor = sensor;
            this.valueTimestampHeadersDeserializer = valueTimestampHeadersDeserializer;
            this.startNs = time.nanoseconds();
            this.startTimestampMs = time.milliseconds();
            this.returnPlainValue = returnPlainValue;
            openIterators.add(this);
        }

        @Override
        public long startTimestamp() {
            return startTimestampMs;
        }

        @Override
        public boolean hasNext() {
            return cachedNext != null || iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            if (cachedNext != null) {
                final KeyValue<K, V> result = cachedNext;
                cachedNext = null;
                return result;
            }

            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            final ValueTimestampHeaders<V> valueTimestampHeaders = valueTimestampHeadersDeserializer.apply(keyValue.value);
            final Headers headers = valueTimestampHeaders != null ? valueTimestampHeaders.headers() : new RecordHeaders();
            if (returnPlainValue) {
                final V plainValue = valueTimestampHeaders == null ? null : valueTimestampHeaders.value();
                return KeyValue.pair(
                    serdes.keyFrom(keyValue.key.get(), headers),
                    plainValue
                );
            } else {
                // Return as ValueAndTimestamp
                final ValueAndTimestamp<V> valueAndTimestamp = valueTimestampHeaders == null
                    ? null
                    : ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp());
                return KeyValue.pair(
                    serdes.keyFrom(keyValue.key.get(), headers),
                    (V) valueAndTimestamp
                );
            }
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                final long duration = time.nanoseconds() - startNs;
                sensor.record(duration);
                iteratorDurationSensor.record(duration);
                openIterators.remove(this);
            }
        }

        @Override
        public K peekNextKey() {
            if (cachedNext == null) {
                cachedNext = next();
            }
            return cachedNext.key;
        }
    }

    private class MeteredValueTimestampHeadersIterator implements KeyValueIterator<K, ValueTimestampHeaders<V>>, MeteredIterator {
        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;
        private final long startTimestampMs;
        private KeyValue<K, ValueTimestampHeaders<V>> cachedNext;

        private MeteredValueTimestampHeadersIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                                     final Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
            this.startTimestampMs = time.milliseconds();
            numOpenIterators.increment();
            openIterators.add(this);
        }

        @Override
        public long startTimestamp() {
            return startTimestampMs;
        }

        @Override
        public boolean hasNext() {
            return cachedNext != null || iter.hasNext();
        }

        @Override
        public KeyValue<K, ValueTimestampHeaders<V>> next() {
            if (cachedNext != null) {
                final KeyValue<K, ValueTimestampHeaders<V>> result = cachedNext;
                cachedNext = null;
                return result;
            }

            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            final ValueTimestampHeaders<V> valueTimestampHeaders = serdes.valueFrom(keyValue.value, new RecordHeaders());
            final Headers headers = valueTimestampHeaders != null ? valueTimestampHeaders.headers() : new RecordHeaders();
            final K key = serdes.keyFrom(keyValue.key.get(), headers);
            return KeyValue.pair(key, valueTimestampHeaders);
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                final long duration = time.nanoseconds() - startNs;
                sensor.record(duration);
                iteratorDurationSensor.record(duration);
                numOpenIterators.decrement();
                openIterators.remove(this);
            }
        }

        @Override
        public K peekNextKey() {
            if (cachedNext == null) {
                cachedNext = next();
            }
            return cachedNext.key;
        }
    }

    protected Bytes keyBytes(final K key, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }
}