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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerde;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;
import static org.apache.kafka.streams.state.internals.StoreQueryUtils.getDeserializeValue;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 *
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V>
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
    implements KeyValueStore<K, V> {

    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private final String metricsScope;
    protected final Time time;
    protected Sensor putSensor;
    private Sensor putIfAbsentSensor;
    protected Sensor getSensor;
    private Sensor deleteSensor;
    private Sensor putAllSensor;
    private Sensor allSensor;
    private Sensor rangeSensor;
    private Sensor prefixScanSensor;
    private Sensor flushSensor;
    private Sensor e2eLatencySensor;
    private InternalProcessorContext context;
    private StreamsMetricsImpl streamsMetrics;
    private TaskId taskId;

    @SuppressWarnings("rawtypes")
    private final Map<Class, QueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                RangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            ),
            mkEntry(
                KeyQuery.class,
                (query, positionBound, config, store) -> runKeyQuery(query, positionBound, config)
            )
        );

    MeteredKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                         final String metricsScope,
                         final Time time,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        super(inner);
        this.metricsScope = metricsScope;
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext) context : null;
        taskId = context.taskId();
        initStoreSerde(context);
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
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

    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        putIfAbsentSensor = StateStoreMetrics.putIfAbsentSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        putAllSensor = StateStoreMetrics.putAllSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        getSensor = StateStoreMetrics.getSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        allSensor = StateStoreMetrics.allSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        rangeSensor = StateStoreMetrics.rangeSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        prefixScanSensor = StateStoreMetrics.prefixScanSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        deleteSensor = StateStoreMetrics.deleteSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        e2eLatencySensor = StateStoreMetrics.e2ELatencySensor(taskId.toString(), metricsScope, name(), streamsMetrics);
    }

    protected Serde<V> prepareValueSerdeForStore(final Serde<V> valueSerde, final SerdeGetter getter) {
        return WrappingNullableUtils.prepareValueSerde(valueSerde, getter);
    }


    @Deprecated
    private void initStoreSerde(final ProcessorContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        final String prefix = getPrefix(context.appConfigs(), context.applicationId());
        serdes = new StateSerdes<>(
            changelogTopic != null ?
                changelogTopic :
                ProcessorStateManager.storeChangelogTopic(prefix, storeName, taskId.topologyName()),
            prepareKeySerde(keySerde, new SerdeGetter(context)),
            prepareValueSerdeForStore(valueSerde, new SerdeGetter(context))
        );
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        final String prefix = getPrefix(context.appConfigs(), context.applicationId());
        serdes = new StateSerdes<>(
            changelogTopic != null ?
                changelogTopic :
                ProcessorStateManager.storeChangelogTopic(prefix, storeName, taskId.topologyName()),
            prepareKeySerde(keySerde, new SerdeGetter(context)),
            prepareValueSerdeForStore(valueSerde, new SerdeGetter(context))
        );
    }

    private static String getPrefix(final Map<String, Object> configs, final String applicationId) {
        if (configs == null) {
            return applicationId;
        } else {
            return StreamsConfig.InternalConfig.getString(
                configs,
                StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE,
                applicationId
            );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<K, V> listener,
                                    final boolean sendOldValues) {
        final KeyValueStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                record -> listener.apply(
                    record.withKey(serdes.keyFrom(record.key()))
                        .withValue(new Change<>(
                            record.value().newValue != null ? serdes.valueFrom(record.value().newValue) : null,
                            record.value().oldValue != null ? serdes.valueFrom(record.value().oldValue) : null
                        ))
                ),
                sendOldValues);
        }
        return false;
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

    @Override
    public Position getPosition() {
        return wrapped().getPosition();
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                             final PositionBound positionBound,
                                             final QueryConfig config) {

        final QueryResult<R> result;
        final RangeQuery<K, V> typedQuery = (RangeQuery<K, V>) query;
        final RangeQuery<Bytes, byte[]> rawRangeQuery;
        if (typedQuery.getLowerBound().isPresent() && typedQuery.getUpperBound().isPresent()) {
            rawRangeQuery = RangeQuery.withRange(
                keyBytes(typedQuery.getLowerBound().get()),
                keyBytes(typedQuery.getUpperBound().get())
            );
        } else if (typedQuery.getLowerBound().isPresent()) {
            rawRangeQuery = RangeQuery.withLowerBound(keyBytes(typedQuery.getLowerBound().get()));
        } else if (typedQuery.getUpperBound().isPresent()) {
            rawRangeQuery = RangeQuery.withUpperBound(keyBytes(typedQuery.getUpperBound().get()));
        } else {
            rawRangeQuery = RangeQuery.withNoBounds();
        }
        final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult =
            wrapped().query(rawRangeQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final KeyValueIterator<Bytes, byte[]> iterator = rawResult.getResult();
            final KeyValueIterator<K, V> resultIterator = new MeteredKeyValueTimestampedIterator(
                iterator,
                getSensor,
                getDeserializeValue(serdes, wrapped())
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
    private <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                           final PositionBound positionBound,
                                           final QueryConfig config) {
        final QueryResult<R> result;
        final KeyQuery<K, V> typedKeyQuery = (KeyQuery<K, V>) query;
        final KeyQuery<Bytes, byte[]> rawKeyQuery =
            KeyQuery.withKey(keyBytes(typedKeyQuery.getKey()));
        final QueryResult<byte[]> rawResult =
            wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final Function<byte[], V> deserializer = getDeserializeValue(serdes, wrapped());
            final V value = deserializer.apply(rawResult.getResult());
            final QueryResult<V> typedQueryResult =
                InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, value);
            result = (QueryResult<R>) typedQueryResult;
        } else {
            // the generic type doesn't matter, since failed queries have no result set.
            result = (QueryResult<R>) rawResult;
        }
        return result;
    }

    @Override
    public V get(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().get(keyBytes(key))), time, getSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            maybeMeasureLatency(() -> wrapped().put(keyBytes(key), serdes.rawValue(value)), time, putSensor);
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        final V currentValue = maybeMeasureLatency(
            () -> outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value))),
            time,
            putIfAbsentSensor
        );
        maybeRecordE2ELatency();
        return currentValue;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        entries.forEach(entry -> Objects.requireNonNull(entry.key, "key cannot be null"));
        maybeMeasureLatency(() -> wrapped().putAll(innerEntries(entries)), time, putAllSensor);
    }

    @Override
    public V delete(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().delete(keyBytes(key))), time, deleteSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(final P prefix, final PS prefixKeySerializer) {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        return new MeteredKeyValueIterator(wrapped().prefixScan(prefix, prefixKeySerializer), prefixScanSensor);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        final byte[] serFrom = from == null ? null : serdes.rawKey(from);
        final byte[] serTo = to == null ? null : serdes.rawKey(to);
        return new MeteredKeyValueIterator(
            wrapped().range(Bytes.wrap(serFrom), Bytes.wrap(serTo)),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(final K from,
                                               final K to) {
        final byte[] serFrom = from == null ? null : serdes.rawKey(from);
        final byte[] serTo = to == null ? null : serdes.rawKey(to);
        return new MeteredKeyValueIterator(
            wrapped().reverseRange(Bytes.wrap(serFrom), Bytes.wrap(serTo)),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator(wrapped().all(), allSensor);
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return new MeteredKeyValueIterator(wrapped().reverseAll(), allSensor);
    }

    @Override
    public void flush() {
        maybeMeasureLatency(super::flush, time, flushSensor);
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void close() {
        try {
            wrapped().close();
        } finally {
            streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId.toString(), name());
        }
    }

    protected V outerValue(final byte[] value) {
        return value != null ? serdes.valueFrom(value) : null;
    }

    protected Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, V>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, V> entry : from) {
            byteEntries.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        return byteEntries;
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

    private class MeteredKeyValueIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;

        private MeteredKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                        final Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                outerValue(keyValue.value));
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }

    private class MeteredKeyValueTimestampedIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;
        private final Function<byte[], V> valueDeserializer;

        private MeteredKeyValueTimestampedIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                        final Sensor sensor,
                                        final Function<byte[], V> valueDeserializer) {
            this.iter = iter;
            this.sensor = sensor;
            this.valueDeserializer = valueDeserializer;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                    serdes.keyFrom(keyValue.key.get()),
                    valueDeserializer.apply(keyValue.value));
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
