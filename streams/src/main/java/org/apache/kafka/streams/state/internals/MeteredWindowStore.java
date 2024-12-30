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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class MeteredWindowStore<K, V>
    extends WrappedStateStore<WindowStore<Bytes, byte[]>, Windowed<K>, V>
    implements WindowStore<K, V> {

    private final long windowSizeMs;
    private final String metricsScope;
    private final Time time;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private StateSerdes<K, V> serdes;
    private StreamsMetricsImpl streamsMetrics;
    private Sensor putSensor;
    private Sensor fetchSensor;
    private Sensor flushSensor;
    private Sensor e2eLatencySensor;
    private Sensor iteratorDurationSensor;
    private InternalProcessorContext<?, ?> context;
    private TaskId taskId;

    private final LongAdder numOpenIterators = new LongAdder();
    private final NavigableSet<MeteredIterator> openIterators = new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp));

    @SuppressWarnings("rawtypes")
    private final Map<Class, QueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                WindowRangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(
                    query,
                    positionBound,
                    config
                )
            ),
            mkEntry(
                WindowKeyQuery.class,
                (query, positionBound, config, store) -> runKeyQuery(
                    query,
                    positionBound,
                    config
                )
            )
        );

    MeteredWindowStore(final WindowStore<Bytes, byte[]> inner,
                       final long windowSizeMs,
                       final String metricsScope,
                       final Time time,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde) {
        super(inner);
        this.windowSizeMs = windowSizeMs;
        this.metricsScope = metricsScope;
        this.time = time;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext<?, ?>) context : null;
        taskId = context.taskId();
        initStoreSerde(context);
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }
    protected Serde<V> prepareValueSerde(final Serde<V> valueSerde, final SerdeGetter getter) {
        return WrappingNullableUtils.prepareValueSerde(valueSerde, getter);
    }

    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        fetchSensor = StateStoreMetrics.fetchSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        e2eLatencySensor = StateStoreMetrics.e2ELatencySensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        iteratorDurationSensor = StateStoreMetrics.iteratorDurationSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        StateStoreMetrics.addNumOpenIteratorsGauge(taskId.toString(), metricsScope, name(), streamsMetrics,
                (config, now) -> numOpenIterators.sum());
        StateStoreMetrics.addOldestOpenIteratorGauge(taskId.toString(), metricsScope, name(), streamsMetrics,
                (config, now) -> {
                    try {
                        return openIterators.isEmpty() ? null : openIterators.first().startTimestamp();
                    } catch (final NoSuchElementException ignored) {
                        return null;
                    }
                }
        );
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = StoreSerdeInitializer.prepareStoreSerde(
            context, storeName, changelogTopic, keySerde, valueSerde, this::prepareValueSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<Windowed<K>, V> listener,
                                    final boolean sendOldValues) {
        final WindowStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                record -> listener.apply(
                    record.withKey(WindowKeySchema.fromStoreKey(record.key(), windowSizeMs, serdes.keyDeserializer(), serdes.topic()))
                        .withValue(new Change<>(
                            record.value().newValue != null ? serdes.valueFrom(record.value().newValue) : null,
                            record.value().oldValue != null ? serdes.valueFrom(record.value().oldValue) : null,
                            record.value().isLatest
                        ))
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long windowStartTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            maybeMeasureLatency(
                () -> wrapped().put(keyBytes(key), serdes.rawValue(value), windowStartTimestamp),
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
    public V fetch(final K key,
                   final long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        return maybeMeasureLatency(
            () -> {
                final byte[] result = wrapped().fetch(keyBytes(key), timestamp);
                if (result == null) {
                    return null;
                }
                return serdes.valueFrom(result);
            },
            time,
            fetchSensor
        );
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowStoreIterator<>(
            wrapped().fetch(keyBytes(key), timeFrom, timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key,
                                                final long timeFrom,
                                                final long timeTo) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowStoreIterator<>(
            wrapped().backwardFetch(keyBytes(key), timeFrom, timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final long timeFrom,
                                                  final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(
                keyBytes(keyFrom),
                keyBytes(keyTo),
                timeFrom,
                timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                          final K keyTo,
                                                          final long timeFrom,
                                                          final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetch(
                keyBytes(keyFrom),
                keyBytes(keyTo),
                timeFrom,
                timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetchAll(timeFrom, timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final long timeFrom,
                                                             final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetchAll(timeFrom, timeTo),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().all(),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardAll(),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            serdes::keyFrom,
            serdes::valueFrom,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public void flush() {
        maybeMeasureLatency(super::flush, time, flushSensor);
    }

    @Override
    public void close() {
        try {
            wrapped().close();
        } finally {
            streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId.toString(), name());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = time.nanoseconds();
        final QueryResult<R> result;

        final QueryHandler handler = queryHandlers.get(query.getClass());
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
                result.addExecutionInfo(
                    "Handled in " + getClass() + " with serdes "
                        + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                             final PositionBound positionBound,
                                             final QueryConfig config) {
        final QueryResult<R> result;
        final WindowRangeQuery<K, V> typedQuery = (WindowRangeQuery<K, V>) query;
        // There's no store API for open time ranges
        if (typedQuery.getTimeFrom().isPresent() && typedQuery.getTimeTo().isPresent()) {
            final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
                WindowRangeQuery.withWindowStartRange(
                    typedQuery.getTimeFrom().get(),
                    typedQuery.getTimeTo().get()
                );
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                wrapped().query(
                    rawKeyQuery,
                    positionBound,
                    config
                );
            if (rawResult.isSuccess()) {
                final MeteredWindowedKeyValueIterator<K, V> typedResult =
                    new MeteredWindowedKeyValueIterator<>(
                        rawResult.getResult(),
                        fetchSensor,
                        iteratorDurationSensor,
                        streamsMetrics,
                        serdes::keyFrom,
                        StoreQueryUtils.deserializeValue(serdes, wrapped()),
                        time,
                        numOpenIterators,
                        openIterators
                    );
                final QueryResult<MeteredWindowedKeyValueIterator<K, V>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                result = (QueryResult<R>) typedQueryResult;
            } else {
                // the generic type doesn't matter, since failed queries have no result set.
                result = (QueryResult<R>) rawResult;
            }
        } else {

            result = QueryResult.forFailure(
                FailureReason.UNKNOWN_QUERY_TYPE,
                "This store (" + getClass() + ") doesn't know how to"
                    + " execute the given query (" + query + ") because"
                    + " WindowStores only supports WindowRangeQuery.withWindowStartRange."
                    + " Contact the store maintainer if you need support"
                    + " for a new query type."
            );
        }
        return result;
    }


    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                           final PositionBound positionBound,
                                           final QueryConfig config) {
        final QueryResult<R> queryResult;
        final WindowKeyQuery<K, V> typedQuery = (WindowKeyQuery<K, V>) query;
        // There's no store API for open time ranges
        if (typedQuery.getTimeFrom().isPresent() && typedQuery.getTimeTo().isPresent()) {
            final WindowKeyQuery<Bytes, byte[]> rawKeyQuery =
                WindowKeyQuery.withKeyAndWindowStartRange(
                    keyBytes(typedQuery.getKey()),
                    typedQuery.getTimeFrom().get(),
                    typedQuery.getTimeTo().get()
                );
            final QueryResult<WindowStoreIterator<byte[]>> rawResult = wrapped().query(
                rawKeyQuery,
                positionBound,
                config
            );
            if (rawResult.isSuccess()) {
                final MeteredWindowStoreIterator<V> typedResult = new MeteredWindowStoreIterator<>(
                    rawResult.getResult(),
                    fetchSensor,
                    iteratorDurationSensor,
                    streamsMetrics,
                    StoreQueryUtils.deserializeValue(serdes, wrapped()),
                    time,
                    numOpenIterators,
                    openIterators
                );
                final QueryResult<MeteredWindowStoreIterator<V>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                queryResult = (QueryResult<R>) typedQueryResult;
            } else {
                // the generic type doesn't matter, since failed queries have no result set.
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

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    protected V outerValue(final byte[] value) {
        return value != null ? serdes.valueFrom(value) : null;
    }

    private void maybeRecordE2ELatency() {
        // Context is null if the provided context isn't an implementation of InternalProcessorContext.
        // In that case, we _can't_ get the current timestamp, so we don't record anything.
        if (e2eLatencySensor.shouldRecord() && context != null) {
            final long currentTime = time.milliseconds();
            final long e2eLatency =  currentTime - context.timestamp();
            e2eLatencySensor.record(e2eLatency, currentTime);
        }
    }
}
