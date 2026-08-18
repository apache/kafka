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
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
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

public class MeteredSessionStore<K, V>
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, Windowed<K>, V>
    implements SessionStore<K, V>, MeteredStateStore {

    protected final String metricsScope;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final Time time;
    protected StateSerdes<K, V> serdes;
    protected StreamsMetricsImpl streamsMetrics;
    protected Sensor putSensor;
    protected Sensor fetchSensor;
    protected Sensor commitSensor;
    protected Sensor removeSensor;
    protected Sensor e2eLatencySensor;
    protected Sensor iteratorDurationSensor;
    protected InternalProcessorContext<?, ?> internalContext;
    protected TaskId taskId;
    protected Sensor restoreSensor;

    protected final LongAdder numOpenIterators = new LongAdder();
    protected final NavigableSet<MeteredIterator> openIterators = new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp));

    private final Map<Class<?>, QueryHandler<?>> queryHandlers =
        mkMap(
            mkEntry(
                WindowRangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            )
        );


    MeteredSessionStore(
        final SessionStore<Bytes, byte[]> inner,
        final String metricsScope,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
        super(inner);
        this.metricsScope = metricsScope;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    @Override
    public void init(
        final StateStoreContext stateStoreContext,
        final StateStore root
    ) {
        internalContext = stateStoreContext instanceof InternalProcessorContext ? (InternalProcessorContext<?, ?>) stateStoreContext : null;
        taskId = stateStoreContext.taskId();
        initStoreSerde(stateStoreContext);
        streamsMetrics = (StreamsMetricsImpl) stateStoreContext.metrics();

        registerMetrics();
        restoreSensor = StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        super.init(stateStoreContext, root);
    }

    @SuppressWarnings("deprecation")
    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        fetchSensor = StateStoreMetrics.fetchSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        // flushSensor is deprecated per KIP-1035 and will be removed in the next major release.
        // Here we just register the sensor without recording
        StateStoreMetrics.flushSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        commitSensor = StateStoreMetrics.commitSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
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
        if (!persistent()) {
            StateStoreMetrics.addNumKeysGauge(taskId.toString(), metricsScope, name(), streamsMetrics,
                    (config, now) -> {
                        final InMemorySessionStore inMemoryStore = findInner(InMemorySessionStore.class);
                        return inMemoryStore != null ? inMemoryStore.numEntries() : -1L;
                    }
            );
        }
    }

    @Override
    public void recordRestoreTime(final long restoreTimeNs) {
        restoreSensor.record(restoreTimeNs);
    }

    protected Serde<V> prepareValueSerdeForStore(final Serde<V> valueSerde, final SerdeGetter getter) {
        return WrappingNullableUtils.prepareValueSerde(valueSerde, getter);
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = StoreSerdeInitializer.prepareStoreSerde(
            context, storeName, changelogTopic, keySerde, valueSerde, this::prepareValueSerdeForStore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(
        final CacheFlushListener<Windowed<K>, V> listener,
        final boolean sendOldValues
    ) {
        final SessionStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                record -> {
                    final Change<byte[]> change = record.value();
                    listener.apply(
                        record
                            .withKey(SessionKeySchema.from(record.key(), serdes.keyDeserializer(), record.headers(), serdes.topic()))
                            .withValue(new Change<>(
                                change.newValue != null ? serdes.valueFrom(change.newValue, record.headers()) : null,
                                change.oldValue != null ? serdes.valueFrom(change.oldValue, record.headers()) : null,
                                change.isLatest
                            ))
                    );
                },
                sendOldValues);
        }
        return false;
    }

    @Override
    public void put(
        final Windowed<K> sessionKey,
        final V aggregate
    ) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        Objects.requireNonNull(sessionKey.key(), "sessionKey.key() can't be null");
        Objects.requireNonNull(sessionKey.window(), "sessionKey.window() can't be null");

        try {
            maybeMeasureLatency(
                () -> {
                    final Bytes key = serializeKey(sessionKey.key());
                    wrapped().put(new Windowed<>(key, sessionKey.window()), serializeValue(aggregate));
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
                    final Bytes key = serializeKey(sessionKey.key());
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
    public V fetchSession(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return maybeMeasureLatency(
            () -> deserializeValue(wrapped().fetchSession(serializeKey(key), earliestSessionEndTime, latestSessionStartTime)),
            time,
            fetchSensor
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return meteredWindowedIterator(wrapped().fetch(serializeKey(key)));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return meteredWindowedIterator(wrapped().backwardFetch(serializeKey(key)));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo) {
        return meteredWindowedIterator(wrapped().fetch(serializeKey(keyFrom), serializeKey(keyTo)));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom, final K keyTo) {
        return meteredWindowedIterator(wrapped().backwardFetch(serializeKey(keyFrom), serializeKey(keyTo)));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return meteredWindowedIterator(wrapped().findSessions(serializeKey(key), earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K key,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return meteredWindowedIterator(wrapped().backwardFindSessions(serializeKey(key), earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K keyFrom,
                                                         final K keyTo,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        return meteredWindowedIterator(wrapped().findSessions(serializeKey(keyFrom), serializeKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final long earliestSessionEndTime,
                                                         final long latestSessionEndTime) {
        return meteredWindowedIterator(wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K keyFrom,
                                                                 final K keyTo,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        return meteredWindowedIterator(wrapped().backwardFindSessions(serializeKey(keyFrom), serializeKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public ReadOnlySessionStore<K, V> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(wrapped().readOnly(isolationLevel));
    }

    private final class ReadOnlyView implements ReadOnlySessionStore<K, V> {

        private final ReadOnlySessionStore<Bytes, byte[]> underlying;

        ReadOnlyView(final ReadOnlySessionStore<Bytes, byte[]> underlying) {
            this.underlying = underlying;
        }

        @Override
        public V fetchSession(
            final K key,
            final long earliestSessionEndTime,
            final long latestSessionStartTime
        ) {
            Objects.requireNonNull(key, "key cannot be null");
            return maybeMeasureLatency(
                () -> deserializeValue(underlying.fetchSession(serializeKey(key), earliestSessionEndTime, latestSessionStartTime)),
                time,
                fetchSensor
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
            Objects.requireNonNull(key, "key cannot be null");
            return meteredWindowedIterator(underlying.fetch(serializeKey(key)));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetch(final K key) {
            Objects.requireNonNull(key, "key cannot be null");
            return meteredWindowedIterator(underlying.backwardFetch(serializeKey(key)));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo) {
            return meteredWindowedIterator(underlying.fetch(serializeKey(keyFrom), serializeKey(keyTo)));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom, final K keyTo) {
            return meteredWindowedIterator(underlying.backwardFetch(serializeKey(keyFrom), serializeKey(keyTo)));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> findSessions(
            final K key,
            final long earliestSessionEndTime,
            final long latestSessionStartTime
        ) {
            Objects.requireNonNull(key, "key cannot be null");
            return meteredWindowedIterator(underlying.findSessions(serializeKey(key), earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFindSessions(
            final K key,
            final long earliestSessionEndTime,
            final long latestSessionStartTime
        ) {
            Objects.requireNonNull(key, "key cannot be null");
            return meteredWindowedIterator(underlying.backwardFindSessions(serializeKey(key), earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> findSessions(
            final K keyFrom,
            final K keyTo,
            final long earliestSessionEndTime,
            final long latestSessionStartTime
        ) {
            return meteredWindowedIterator(underlying.findSessions(serializeKey(keyFrom), serializeKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFindSessions(
            final K keyFrom,
            final K keyTo,
            final long earliestSessionEndTime,
            final long latestSessionStartTime
        ) {
            return meteredWindowedIterator(underlying.backwardFindSessions(serializeKey(keyFrom), serializeKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
        }

    }

    private KeyValueIterator<Windowed<K>, V> meteredWindowedIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> iter) {
        return new MeteredWindowedKeyValueIterator<>(
            iter,
            fetchSensor,
            iteratorDurationSensor,
            this::deserializeKey,
            this::deserializeValue,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        maybeMeasureLatency(() -> super.commit(changelogOffsets), time, commitSensor);
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
    private <R> QueryResult<R> runRangeQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> result;
        final WindowRangeQuery<K, V> typedQuery = (WindowRangeQuery<K, V>) query;
        if (typedQuery.getKey().isPresent()) {
            final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
                WindowRangeQuery.withKey(serializeKey(typedQuery.getKey().get()));
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                final MeteredWindowedKeyValueIterator<K, V> typedResult =
                    new MeteredWindowedKeyValueIterator<>(
                        rawResult.getResult(),
                        fetchSensor,
                        iteratorDurationSensor,
                        this::deserializeKey,
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
                    + " SessionStores only support WindowRangeQuery.withKey."
                    + " Contact the store maintainer if you need support"
                    + " for a new query type."
            );
        }
        return result;
    }

    private Bytes serializeKey(final K key) {
        return Bytes.wrap(serdes.rawKey(key, internalContext.headers()));
    }

    private K deserializeKey(final byte[] rawKey) {
        return serdes.keyFrom(rawKey, internalContext.headers());
    }

    protected byte[] serializeValue(final V value) {
        return value != null ? serdes.rawValue(value, internalContext.headers()) : null;
    }

    protected V deserializeValue(final byte[] rawValue) {
        return rawValue != null ? serdes.valueFrom(rawValue, internalContext.headers()) : null;
    }

    void maybeRecordE2ELatency() {
        // Context is null if the provided context isn't an implementation of InternalProcessorContext.
        // In that case, we _can't_ get the current timestamp, so we don't record anything.
        if (e2eLatencySensor.shouldRecord() && internalContext != null) {
            final long currentTime = time.milliseconds();
            final long e2eLatency =  currentTime - internalContext.recordContext().timestamp();
            e2eLatencySensor.record(e2eLatency, currentTime);
        }
    }
}
