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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedRangeQuery;
import org.apache.kafka.streams.query.TimestampedRangeWithHeadersQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;


/**
 * A Metered {@link TimestampedKeyValueStoreWithHeaders} wrapper that is used for recording operation metrics, and hence
 * its inner KeyValueStore implementation does not need to provide its own metrics collecting functionality.
 *
 * <p> The inner {@link KeyValueStore} of this class is of type &lt;Bytes, byte[]&gt;,
 * hence we use {@link Serde}s to convert from &lt;K, ValueTimestampHeaders&lt;V&gt;&gt; to &lt;Bytes, byte[]&gt;.
 *
 * @param <K> key type
 * @param <V> value type (wrapped in {@link ValueTimestampHeaders})
 */
public class MeteredTimestampedKeyValueStoreWithHeaders<K, V>
    extends MeteredKeyValueStore<K, ValueTimestampHeaders<V>>
    implements TimestampedKeyValueStoreWithHeaders<K, V> {

    MeteredTimestampedKeyValueStoreWithHeaders(
        final KeyValueStore<Bytes, byte[]> inner,
        final String metricScope,
        final Time time,
        final Serde<K> keySerde,
        final Serde<ValueTimestampHeaders<V>> valueSerde
    ) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }

    private final Map<Class<?>, QueryHandler<?>> queryHandlers =
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
                TimestampedKeyWithHeadersQuery.class,
                (query, positionBound, config, store) -> runTimestampedKeyWithHeadersQuery(query, positionBound, config)
            ),
            mkEntry(
                RangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            ),
            mkEntry(
                TimestampedRangeQuery.class,
                (query, positionBound, config, store) -> runTimestampedRangeQuery(query, positionBound, config)
            ),
            mkEntry(
                TimestampedRangeWithHeadersQuery.class,
                (query, positionBound, config, store) -> runTimestampedRangeWithHeadersQuery(query, positionBound, config)
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
    public ValueTimestampHeaders<V> get(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return getInternal(wrapped(), key);
    }

    private ValueTimestampHeaders<V> getInternal(final ReadOnlyKeyValueStore<Bytes, byte[]> store, final K key) {
        try {
            return maybeMeasureLatency(
                () -> deserializeValue(store.get(serializeKey(key, internalContext.headers()))),
                time,
                getSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final ValueTimestampHeaders<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            maybeMeasureLatency(
                () -> {
                    if (value == null) {
                        final ProcessorRecordContext currentContext = internalContext.recordContext();

                        // Create new headers object to isolate tombstone operation from input record
                        final Headers tombstoneHeaders = new RecordHeaders(currentContext.headers());

                        // Create temporary context with new headers
                        final ProcessorRecordContext temporaryContext = new ProcessorRecordContext(
                            currentContext.timestamp(),
                            currentContext.offset(),
                            currentContext.partition(),
                            currentContext.topic(),
                            tombstoneHeaders
                        );

                        try {
                            internalContext.setRecordContext(temporaryContext);
                            wrapped().put(serializeKey(key, tombstoneHeaders), serializeValue(null));
                        } finally {
                            // Restore original context
                            internalContext.setRecordContext(currentContext);
                        }
                    } else {
                        // it's ok to only pass headers into `serializeKey`, because for the value case passed-in headers are
                        // getting ignored anyway, because the value (of type `ValueTimestampHeaders`) itself carries the headers
                        final Headers headers = value.headers();
                        wrapped().put(serializeKey(key, headers), serializeValue(value));
                    }
                },
                time,
                putSensor
            );
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
        final ValueTimestampHeaders<V> currentValue = maybeMeasureLatency(
            () -> {
                if (value == null) {
                    final ProcessorRecordContext currentContext = internalContext.recordContext();

                    // Create new headers object to isolate tombstone operation from input record
                    final Headers tombstoneHeaders = new RecordHeaders(currentContext.headers());

                    // Create temporary context with new headers
                    final ProcessorRecordContext temporaryContext = new ProcessorRecordContext(
                        currentContext.timestamp(),
                        currentContext.offset(),
                        currentContext.partition(),
                        currentContext.topic(),
                        tombstoneHeaders
                    );

                    try {
                        internalContext.setRecordContext(temporaryContext);
                        return deserializeValue(wrapped().putIfAbsent(serializeKey(key, tombstoneHeaders), serializeValue(null)));
                    } finally {
                        // Restore original context
                        internalContext.setRecordContext(currentContext);
                    }
                } else {
                    // it's ok to only pass headers into `serializeKey`, because for the value case passed-in headers are
                    // getting ignored anyway, because the value (of type `ValueTimestampHeaders`) itself carries the headers
                    final Headers headers = value.headers();
                    // `rawOldValue` returned from `wrapped().putIfAbsent(...)` is type ValueTimestampHeader
                    // -> no need to pass in Headers into `deserializeValue()`
                    return deserializeValue(wrapped().putIfAbsent(serializeKey(key, headers), serializeValue(value)));
                }
            },
            time,
            putIfAbsentSensor
        );
        maybeRecordE2ELatency();
        return currentValue;
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueTimestampHeaders<V>>> entries) {
        entries.forEach(entry -> Objects.requireNonNull(entry.key, "key cannot be null"));

        final boolean hasNullValue = entries.stream().anyMatch(entry -> entry.value == null);

        if (hasNullValue) {
            entries.forEach(entry -> put(entry.key, entry.value));
        } else {
            maybeMeasureLatency(() -> wrapped().putAll(innerEntries(entries)), time, putAllSensor);
        }
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, ValueTimestampHeaders<V>>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, ValueTimestampHeaders<V>> entry : from) {
            // it's ok to only pass headers into `serializeKey`, because for the value case passed-in headers are
            // getting ignored anyway, because the value (of type `ValueTimestampHeaders`) itself carries the headers
            final Headers headers = entry.value != null ? entry.value.headers() : internalContext.headers();
            byteEntries.add(KeyValue.pair(serializeKey(entry.key, headers), serializeValue(entry.value)));
        }
        return byteEntries;
    }

    @Override
    public ValueTimestampHeaders<V> delete(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(
                () -> {
                    final ProcessorRecordContext currentContext = internalContext.recordContext();

                    // Create new headers object to isolate delete operation from input record
                    final Headers tombstoneHeaders = new RecordHeaders(currentContext.headers());

                    // Create temporary context with new headers
                    final ProcessorRecordContext temporaryContext = new ProcessorRecordContext(
                        currentContext.timestamp(),
                        currentContext.offset(),
                        currentContext.partition(),
                        currentContext.topic(),
                        tombstoneHeaders
                    );

                    try {
                        internalContext.setRecordContext(temporaryContext);
                        final byte[] deletedValue = wrapped().delete(serializeKey(key, tombstoneHeaders));
                        return deserializeValue(deletedValue);
                    } finally {
                        // Restore original context
                        internalContext.setRecordContext(currentContext);
                    }
                },
                time,
                deleteSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    /**
     * Executes a query against this store.
     *
     * @param query the query to execute
     * @param positionBound the position bound
     * @param config the query configuration
     * @return the query result
     */
    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final long start = time.nanoseconds();
        final QueryResult<R> result;

        final QueryHandler<?> handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            result = wrapped().query(query, positionBound, config);
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo("Handled in " + getClass() + " in " + (time.nanoseconds() - start) + "ns");
            }
        } else {
            result = ((QueryHandler<R>) handler).apply(
                query,
                positionBound,
                config,
                this
            );
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo("Handled in " + getClass() + " with serdes " + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runKeyQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final KeyQuery<K, V> typedKeyQuery = (KeyQuery<K, V>) query;

        final KeyQuery<Bytes, byte[]> rawKeyQuery = KeyQuery.withKey(serializeKey(typedKeyQuery.getKey(), internalContext.headers()));
        final QueryResult<byte[]> rawResult = wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            // value will be `rawValueTimestampHeader`; no need to pass headers explicitly
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
    private <R> QueryResult<R> runTimestampedKeyQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final TimestampedKeyQuery<K, V> typedKeyQuery = (TimestampedKeyQuery<K, V>) query;

        final KeyQuery<Bytes, byte[]> rawKeyQuery = KeyQuery.withKey(serializeKey(typedKeyQuery.key(), internalContext.headers()));
        final QueryResult<byte[]> rawResult = wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            // value will be `rawValueTimestampHeader`; no need to pass headers explicitly
            final Function<byte[], ValueTimestampHeaders<V>> deserializer = StoreQueryUtils.deserializeValue(serdes, wrapped());
            final ValueTimestampHeaders<V> valueTimestampHeaders = deserializer.apply(rawResult.getResult());
            // Convert ValueTimestampHeaders to ValueAndTimestamp for the result
            final ValueAndTimestamp<V> valueAndTimestamp =
                valueTimestampHeaders == null
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
    private <R> QueryResult<R> runTimestampedKeyWithHeadersQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final TimestampedKeyWithHeadersQuery<K, V> typedKeyQuery = (TimestampedKeyWithHeadersQuery<K, V>) query;

        // Forward a raw byte-level KeyQuery to the wrapped store, propagating skipCache so the caching
        // layer can honor it; the result bytes are the serialized ValueTimestampHeaders, which we
        // deserialize below to recover value, timestamp, and headers.
        // The existing KeyQuery/TimestampedKeyQuery handlers do not yet propagate skipCache across the
        // metered stores; that general fix is tracked in KAFKA-20776.
        KeyQuery<Bytes, byte[]> rawKeyQuery = KeyQuery.withKey(serializeKey(typedKeyQuery.key(), internalContext.headers()));
        if (typedKeyQuery.isSkipCache()) {
            rawKeyQuery = rawKeyQuery.skipCache();
        }
        final QueryResult<byte[]> rawResult = wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final Function<byte[], ValueTimestampHeaders<V>> deserializer = StoreQueryUtils.deserializeValue(serdes, wrapped());
            final ValueTimestampHeaders<V> valueTimestampHeaders = deserializer.apply(rawResult.getResult());
            if (valueTimestampHeaders != null && valueTimestampHeaders.timestamp() < 0) {
                // The result is modeled as a Record, whose constructor rejects negative timestamps. A
                // negative stored timestamp cannot arise from the normal record-driven flow (the PAPI
                // Record a processor stores already forbids it), so it indicates corrupted/unexpected
                // store state; surface it as a failed result rather than letting `new Record<>` throw
                // out of query().
                final QueryResult<ReadOnlyRecord<K, V>> failure = QueryResult.forFailure(
                    FailureReason.STORE_EXCEPTION,
                    "Stored record for the queried key has a negative timestamp ("
                        + valueTimestampHeaders.timestamp() + "); cannot construct a ReadOnlyRecord.");
                // Preserve the wrapped store's execution info (empty unless collectExecutionInfo is set),
                // matching the success path and the raw-failure path below.
                rawResult.getExecutionInfo().forEach(failure::addExecutionInfo);
                failure.setPosition(rawResult.getPosition());
                result = (QueryResult<R>) failure;
            } else {
                // Surface the result as a ReadOnlyRecord (implemented by Record), keeping the headers.
                // A null wrapper means the key is absent or tombstoned, which we surface as a null result.
                final ReadOnlyRecord<K, V> record;
                if (valueTimestampHeaders == null) {
                    record = null;
                } else {
                    final Record<K, V> headerRecord = new Record<>(
                        typedKeyQuery.key(),
                        valueTimestampHeaders.value(),
                        valueTimestampHeaders.timestamp(),
                        valueTimestampHeaders.headers());
                    // An IQ result is a read-only snapshot, so its headers should be immutable too.
                    // Record copies the headers into a RecordHeaders; mark it read-only so a caller
                    // cannot mutate the returned Headers.
                    ((RecordHeaders) headerRecord.headers()).setReadOnly();
                    record = headerRecord;
                }
                final QueryResult<ReadOnlyRecord<K, V>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, record);
                result = (QueryResult<R>) typedQueryResult;
            }
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    private RangeQuery<Bytes, byte[]> rawRangeQuery(final Optional<K> lowerBound,
                                                    final Optional<K> upperBound,
                                                    final ResultOrder order) {
        RangeQuery<Bytes, byte[]> rawRangeQuery = RangeQuery.withRange(
            serializeKey(lowerBound.orElse(null), internalContext.headers()),
            serializeKey(upperBound.orElse(null), internalContext.headers())
        );
        if (order.equals(ResultOrder.DESCENDING)) {
            rawRangeQuery = rawRangeQuery.withDescendingKeys();
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            rawRangeQuery = rawRangeQuery.withAscendingKeys();
        }
        return rawRangeQuery;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final RangeQuery<K, V> typedQuery = (RangeQuery<K, V>) query;

        final RangeQuery<Bytes, byte[]> rawRangeQuery =
            rawRangeQuery(typedQuery.getLowerBound(), typedQuery.getUpperBound(), typedQuery.resultOrder());

        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult = wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final KeyValueIterator<K, V> resultIterator = new MeteredTimestampedKeyValueStoreWithHeadersQueryIterator(
                iterator,
                getSensor,
                // value will be `rawValueTimestampHeader`; no need to pass headers explicitly
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
    private <R> QueryResult<R> runTimestampedRangeQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final TimestampedRangeQuery<K, V> typedQuery = (TimestampedRangeQuery<K, V>) query;

        final RangeQuery<Bytes, byte[]> rawRangeQuery =
            rawRangeQuery(typedQuery.lowerBound(), typedQuery.upperBound(), typedQuery.resultOrder());

        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult = wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final KeyValueIterator<K, ValueAndTimestamp<V>> resultIterator =
                (KeyValueIterator<K, ValueAndTimestamp<V>>) new MeteredTimestampedKeyValueStoreWithHeadersQueryIterator(
                    iterator,
                    getSensor,
                    // value will be `rawValueTimestampHeader`; no need to pass headers explicitly
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

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runTimestampedRangeWithHeadersQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final TimestampedRangeWithHeadersQuery<K, V> typedQuery = (TimestampedRangeWithHeadersQuery<K, V>) query;

        final RangeQuery<Bytes, byte[]> rawRangeQuery =
            rawRangeQuery(typedQuery.lowerBound(), typedQuery.upperBound(), typedQuery.resultOrder());

        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult = wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final ReadOnlyRecordIterator<K, V> resultIterator =
                new MeteredTimestampedKeyValueStoreWithHeadersReadOnlyRecordIterator(
                    iterator,
                    getSensor,
                    StoreQueryUtils.deserializeValue(serdes, wrapped())
                );
            final QueryResult<ReadOnlyRecordIterator<K, V>> typedQueryResult =
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
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScan(
        final P prefix, final PS prefixKeySerializer
    ) {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        return prefixScanInternal(wrapped(), prefix, prefixKeySerializer);
    }

    private <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScanInternal(
        final ReadOnlyKeyValueStore<Bytes, byte[]> store, final P prefix, final PS prefixKeySerializer
    ) {
        return new MeteredTimestampedKeyValueStoreWithHeadersIterator(
            store.prefixScan(prefix, prefixKeySerializer), prefixScanSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from, final K to) {
        return rangeInternal(wrapped(), from, to);
    }

    private KeyValueIterator<K, ValueTimestampHeaders<V>> rangeInternal(
        final ReadOnlyKeyValueStore<Bytes, byte[]> store, final K from, final K to
    ) {
        return new MeteredTimestampedKeyValueStoreWithHeadersIterator(
            store.range(
                serializeKey(from, internalContext.headers()),
                serializeKey(to, internalContext.headers())
            ),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRange(final K from, final K to) {
        return reverseRangeInternal(wrapped(), from, to);
    }

    private KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRangeInternal(
        final ReadOnlyKeyValueStore<Bytes, byte[]> store, final K from, final K to
    ) {
        return new MeteredTimestampedKeyValueStoreWithHeadersIterator(
            store.reverseRange(
                serializeKey(from, internalContext.headers()),
                serializeKey(to, internalContext.headers())
            ),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
        return allInternal(wrapped());
    }

    private KeyValueIterator<K, ValueTimestampHeaders<V>> allInternal(
        final ReadOnlyKeyValueStore<Bytes, byte[]> store
    ) {
        return new MeteredTimestampedKeyValueStoreWithHeadersIterator(store.all(), allSensor);
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAll() {
        return reverseAllInternal(wrapped());
    }

    private KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAllInternal(
        final ReadOnlyKeyValueStore<Bytes, byte[]> store
    ) {
        return new MeteredTimestampedKeyValueStoreWithHeadersIterator(store.reverseAll(), allSensor);
    }

    /**
     * Shared scaffolding for the metered iterators below: tracks {@code num-open-iterators},
     * {@code oldest-iterator-open-since-ms}, and per-operation iterator duration, and delegates
     * closing the wrapped raw iterator. Subclasses only need to implement the deserializing
     * {@code next()}/{@code hasNext()} (and, where applicable, {@code peekNextKey()}).
     */
    private abstract class AbstractMeteredIterator implements MeteredIterator {

        final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;
        private final long startTimestampMs;

        AbstractMeteredIterator(final KeyValueIterator<Bytes, byte[]> iter, final Sensor sensor) {
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
    }

    @SuppressWarnings("unchecked")
    private class MeteredTimestampedKeyValueStoreWithHeadersQueryIterator extends AbstractMeteredIterator implements KeyValueIterator<K, V> {

        private final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer;

        private final boolean returnPlainValue;
        private KeyValue<K, V> cachedNext;

        private MeteredTimestampedKeyValueStoreWithHeadersQueryIterator(
            final KeyValueIterator<Bytes, byte[]> iter,
            final Sensor sensor,
            final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer,
            final boolean returnPlainValue
        ) {
            super(iter, sensor);
            this.valueTimestampHeadersDeserializer = valueTimestampHeadersDeserializer;
            this.returnPlainValue = returnPlainValue;
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
            final Headers headers = valueTimestampHeaders.headers();

            if (returnPlainValue) {
                return KeyValue.pair(deserializeKey(keyValue.key.get(), headers), valueTimestampHeaders.value());
            } else {
                // Return as ValueAndTimestamp
                return KeyValue.pair(
                    deserializeKey(keyValue.key.get(), headers),
                    (V) ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp())
                );
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

    /**
     * Iterator backing {@link TimestampedRangeWithHeadersQuery}: yields each entry as a
     * {@link ReadOnlyRecord} (implemented by {@link Record}) carrying key, value, timestamp, and the
     * stored headers, with the headers frozen so a caller cannot mutate the read-only result.
     *
     * <p>A {@link ReadOnlyRecord} timestamp is contractually non-negative, so an entry with a negative
     * stored timestamp cannot be represented. The dominant, deterministic cause is a store that does
     * not persist timestamps: a {@code WithHeaders} store built over a plain {@link KeyValueStore}
     * supplier surfaces every entry with {@code NO_TIMESTAMP} (-1). A genuine negative write is
     * otherwise blocked (the source {@code RecordQueue} drops negative-timestamp records at ingestion),
     * though {@link ValueAndTimestamp#make}/{@link ValueTimestampHeaders#make} do not themselves reject
     * one written directly.
     *
     * <p>This mirrors the rule the point query {@link TimestampedKeyWithHeadersQuery} applies. But
     * because a lazily-evaluated range has already returned a successful {@link QueryResult} before any
     * entry is read, such an entry cannot be surfaced as a query-level failure; it is instead reported
     * by throwing a {@link StreamsException} while advancing the iterator.
     *
     * <p>That throw does not close the iterator: a caller that catches it and abandons the iterator
     * leaks the underlying raw iterator and permanently inflates {@code num-open-iterators}. Callers
     * must close this iterator in a {@code finally} block or a try-with-resources statement, even when
     * {@code next()} throws.
     */
    private class MeteredTimestampedKeyValueStoreWithHeadersReadOnlyRecordIterator
        extends AbstractMeteredIterator implements ReadOnlyRecordIterator<K, V> {

        private final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer;

        private MeteredTimestampedKeyValueStoreWithHeadersReadOnlyRecordIterator(
            final KeyValueIterator<Bytes, byte[]> iter,
            final Sensor sensor,
            final Function<byte[], ValueTimestampHeaders<V>> valueTimestampHeadersDeserializer
        ) {
            super(iter, sensor);
            this.valueTimestampHeadersDeserializer = valueTimestampHeadersDeserializer;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public ReadOnlyRecord<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            final ValueTimestampHeaders<V> valueTimestampHeaders = valueTimestampHeadersDeserializer.apply(keyValue.value);
            final Headers headers = valueTimestampHeaders.headers();
            final K key = deserializeKey(keyValue.key.get(), headers);
            if (valueTimestampHeaders.timestamp() < 0) {
                throw new StreamsException(
                    "Cannot represent the stored record for key [" + key + "] as a ReadOnlyRecord: its "
                        + "timestamp (" + valueTimestampHeaders.timestamp() + ") is negative.");
            }
            final Record<K, V> record = new Record<>(
                key,
                valueTimestampHeaders.value(),
                valueTimestampHeaders.timestamp(),
                headers);
            ((RecordHeaders) record.headers()).setReadOnly();
            return record;
        }
    }

    private class MeteredTimestampedKeyValueStoreWithHeadersIterator
        extends AbstractMeteredIterator implements KeyValueIterator<K, ValueTimestampHeaders<V>> {

        private KeyValue<K, ValueTimestampHeaders<V>> cachedNext;

        private MeteredTimestampedKeyValueStoreWithHeadersIterator(
            final KeyValueIterator<Bytes, byte[]> iter,
            final Sensor sensor
        ) {
            super(iter, sensor);
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
            final ValueTimestampHeaders<V> valueTimestampHeaders = deserializeValue(keyValue.value);
            final K key = deserializeKey(keyValue.key.get(), valueTimestampHeaders.headers());
            return KeyValue.pair(key, valueTimestampHeaders);
        }

        @Override
        public K peekNextKey() {
            if (cachedNext == null) {
                cachedNext = next();
            }
            return cachedNext.key;
        }
    }

    @Override
    public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyHeadersView(wrapped().readOnly(isolationLevel));
    }

    private final class ReadOnlyHeadersView implements ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> {

        private final ReadOnlyKeyValueStore<Bytes, byte[]> underlying;

        ReadOnlyHeadersView(final ReadOnlyKeyValueStore<Bytes, byte[]> underlying) {
            this.underlying = underlying;
        }

        @Override
        public ValueTimestampHeaders<V> get(final K key) {
            Objects.requireNonNull(key, "key cannot be null");
            return getInternal(underlying, key);
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from, final K to) {
            return rangeInternal(underlying, from, to);
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRange(final K from, final K to) {
            return reverseRangeInternal(underlying, from, to);
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
            return allInternal(underlying);
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAll() {
            return reverseAllInternal(underlying);
        }

        @Override
        public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScan(
            final P prefix, final PS prefixKeySerializer
        ) {
            Objects.requireNonNull(prefix, "prefix cannot be null");
            Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
            return prefixScanInternal(underlying, prefix, prefixKeySerializer);
        }

        @Override
        public long approximateNumEntries() {
            return underlying.approximateNumEntries();
        }
    }

    @Override
    protected Bytes serializeKey(final K key) {
        throw new UnsupportedOperationException("MeteredTimestampedKeyValueStoreWithHeaders required to pass in Headers when serializing a key.");
    }

    private Bytes serializeKey(final K key, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }

    @Override
    protected K deserializeKey(final byte[] rawKey) {
        throw new UnsupportedOperationException("MeteredTimestampedKeyValueStoreWithHeaders required to pass in Headers when deserializing a key.");
    }

    private K deserializeKey(final byte[] rawKey, final Headers headers) {
        return serdes.keyFrom(rawKey, headers);
    }
}
