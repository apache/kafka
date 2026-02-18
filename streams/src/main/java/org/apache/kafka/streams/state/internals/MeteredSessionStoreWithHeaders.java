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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class MeteredSessionStoreWithHeaders<K, AGG>
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, Windowed<K>, AggregationWithHeaders<AGG>>
    implements SessionStoreWithHeaders<K, AGG>, MeteredStateStore {

    private final String metricsScope;
    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    private final Time time;
    private StateSerdes<K, AGG> serdes;
    private AggregationWithHeadersSerializer<AGG> aggregationWithHeadersSerializer;
    private AggregationWithHeadersDeserializer<AGG> aggregationWithHeadersDeserializer;
    private StreamsMetricsImpl streamsMetrics;
    private Sensor putSensor;
    private Sensor fetchSensor;
    private Sensor flushSensor;
    private Sensor removeSensor;
    private Sensor e2eLatencySensor;
    private Sensor iteratorDurationSensor;
    private InternalProcessorContext<?, ?> internalContext;
    private TaskId taskId;
    private Sensor restoreSensor;

    private final LongAdder numOpenIterators = new LongAdder();
    private final NavigableSet<MeteredIterator> openIterators = new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp));

    @SuppressWarnings("rawtypes")
    private final Map<Class, QueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                WindowRangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            )
        );

    MeteredSessionStoreWithHeaders(final SessionStore<Bytes, byte[]> inner,
                                   final String metricsScope,
                                   final Serde<K> keySerde,
                                   final Serde<AGG> aggSerde,
                                   final Time time) {
        super(inner);
        this.metricsScope = metricsScope;
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
        this.time = time;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext,
                     final StateStore root) {
        internalContext = stateStoreContext instanceof InternalProcessorContext ? (InternalProcessorContext<?, ?>) stateStoreContext : null;
        taskId = stateStoreContext.taskId();
        initStoreSerde(stateStoreContext);
        streamsMetrics = (StreamsMetricsImpl) stateStoreContext.metrics();

        registerMetrics();
        restoreSensor = StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        super.init(stateStoreContext, root);
    }

    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        fetchSensor = StateStoreMetrics.fetchSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeSensor = StateStoreMetrics.removeSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        e2eLatencySensor = StateStoreMetrics.e2ELatencySensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        iteratorDurationSensor = StateStoreMetrics.iteratorDurationSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        StateStoreMetrics.addNumOpenIteratorsGauge(taskId.toString(), metricsScope, name(), streamsMetrics,
            (config, now) -> numOpenIterators.sum());
        StateStoreMetrics.addOldestOpenIteratorGauge(taskId.toString(), metricsScope, name(), streamsMetrics,
            (config, now) -> {
                try {
                    final Iterator<MeteredIterator> openIteratorsIterator = openIterators.iterator();
                    return openIteratorsIterator.hasNext() ? openIteratorsIterator.next().startTimestamp() : 0L;
                } catch (final NoSuchElementException e) {
                    return 0L;
                }
            }
        );
    }

    @Override
    public void recordRestoreTime(final long restoreTimeNs) {
        restoreSensor.record(restoreTimeNs);
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = StoreSerdeInitializer.prepareStoreSerde(
            context, storeName, changelogTopic, keySerde, aggSerde, WrappingNullableUtils::prepareValueSerde);
        aggregationWithHeadersSerializer = new AggregationWithHeadersSerializer<>(serdes.valueSerde().serializer());
        aggregationWithHeadersDeserializer = new AggregationWithHeadersDeserializer<>(serdes.valueSerde().deserializer());
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<Windowed<K>, AggregationWithHeaders<AGG>> listener,
                                    final boolean sendOldValues) {
        final SessionStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                record -> listener.apply(
                    record.withKey(SessionKeySchema.from(record.key(), serdes.keyDeserializer(), new RecordHeaders(), serdes.topic()))
                        .withValue(new Change<>(
                            record.value().newValue != null ? deserializeAggregationWithHeaders(record.value().newValue) : null,
                            record.value().oldValue != null ? deserializeAggregationWithHeaders(record.value().oldValue) : null,
                            record.value().isLatest
                        ))
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public void put(final Windowed<K> sessionKey,
                    final AggregationWithHeaders<AGG> aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        Objects.requireNonNull(sessionKey.key(), "sessionKey.key() can't be null");
        Objects.requireNonNull(sessionKey.window(), "sessionKey.window() can't be null");

        try {
            maybeMeasureLatency(
                () -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    final byte[] serializedAggregation = serializeAggregationWithHeaders(aggregate);
                    wrapped().put(new Windowed<>(key, sessionKey.window()), serializedAggregation);
                },
                time,
                putSensor
            );
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        Objects.requireNonNull(sessionKey.key(), "sessionKey.key() can't be null");
        Objects.requireNonNull(sessionKey.window(), "sessionKey.window() can't be null");

        try {
            maybeMeasureLatency(
                () -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    wrapped().remove(new Windowed<>(key, sessionKey.window()));
                },
                time,
                removeSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key());
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public AggregationWithHeaders<AGG> fetchSession(final K key, final long sessionStartTime, final long sessionEndTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return maybeMeasureLatency(
            () -> {
                final Bytes bytesKey = keyBytes(key);
                final byte[] result = wrapped().fetchSession(
                    bytesKey,
                    sessionStartTime,
                    sessionEndTime
                );
                if (result == null) {
                    return null;
                }
                return deserializeAggregationWithHeaders(result);
            },
            time,
            fetchSensor
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(key)),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetch(keyBytes(key)),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K keyFrom,
                                                                             final K keyTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(keyFrom), keyBytes(keyTo)),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K keyFrom,
                                                                                     final K keyTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetch(keyBytes(keyFrom), keyBytes(keyTo)),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K key,
                                                                                    final long earliestSessionEndTime,
                                                                                    final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        final Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().findSessions(
                bytesKey,
                earliestSessionEndTime,
                latestSessionStartTime),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K key,
                                                                                            final long earliestSessionEndTime,
                                                                                            final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        final Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFindSessions(
                bytesKey,
                earliestSessionEndTime,
                latestSessionStartTime
            ),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K keyFrom,
                                                                                    final K keyTo,
                                                                                    final long earliestSessionEndTime,
                                                                                    final long latestSessionStartTime) {
        final Bytes bytesKeyFrom = keyBytes(keyFrom);
        final Bytes bytesKeyTo = keyBytes(keyTo);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().findSessions(
                bytesKeyFrom,
                bytesKeyTo,
                earliestSessionEndTime,
                latestSessionStartTime),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final long earliestSessionEndTime,
                                                                                    final long latestSessionEndTime) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K keyFrom,
                                                                                            final K keyTo,
                                                                                            final long earliestSessionEndTime,
                                                                                            final long latestSessionStartTime) {
        final Bytes bytesKeyFrom = keyBytes(keyFrom);
        final Bytes bytesKeyTo = keyBytes(keyTo);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFindSessions(
                bytesKeyFrom,
                bytesKeyTo,
                earliestSessionEndTime,
                latestSessionStartTime
            ),
            fetchSensor,
            iteratorDurationSensor,
            streamsMetrics,
            bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
            this::deserializeAggregationWithHeaders,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        maybeMeasureLatency(() -> super.commit(changelogOffsets), time, flushSensor);
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
        final WindowRangeQuery<K, AggregationWithHeaders<AGG>> typedQuery = (WindowRangeQuery<K, AggregationWithHeaders<AGG>>) query;
        if (typedQuery.getKey().isPresent()) {
            final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
                WindowRangeQuery.withKey(
                    Bytes.wrap(serdes.rawKey(typedQuery.getKey().get(), new RecordHeaders()))
                );
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                final MeteredWindowedKeyValueIterator<K, AggregationWithHeaders<AGG>> typedResult =
                    new MeteredWindowedKeyValueIterator<>(
                        rawResult.getResult(),
                        fetchSensor,
                        iteratorDurationSensor,
                        streamsMetrics,
                        bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
                        this::deserializeAggregationWithHeaders,
                        time,
                        numOpenIterators,
                        openIterators
                    );
                final QueryResult<MeteredWindowedKeyValueIterator<K, AggregationWithHeaders<AGG>>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
                result = (QueryResult<R>) typedQueryResult;
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else {
            result = QueryResult.forFailure(
                FailureReason.UNKNOWN_QUERY_TYPE,
                "This store (" + getClass() + ") doesn't know how to"
                    + " execute the given query (" + query + ") because"
                    + " SessionStores only support WindowRangeQuery.withKey."
                    + " Contact the store maintainer if you need support"
                    + " for a new query type."
            );
        }
        return result;
    }

    private Bytes keyBytes(final K key) {
        return key == null ? null : Bytes.wrap(serdes.rawKey(key, new RecordHeaders()));
    }

    private byte[] serializeAggregationWithHeaders(final AggregationWithHeaders<AGG> aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }
        return aggregationWithHeadersSerializer.serialize(serdes.topic(), aggregationWithHeaders);
    }

    private AggregationWithHeaders<AGG> deserializeAggregationWithHeaders(final byte[] rawAggregation) {
        if (rawAggregation == null) {
            return null;
        }
        return aggregationWithHeadersDeserializer.deserialize(serdes.topic(), rawAggregation);
    }

    private void maybeRecordE2ELatency() {
        if (e2eLatencySensor.shouldRecord() && internalContext != null) {
            final long currentTime = time.milliseconds();
            final long e2eLatency = currentTime - internalContext.recordContext().timestamp();
            e2eLatencySensor.record(e2eLatency, currentTime);
        }
    }
}
