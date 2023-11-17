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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerde;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareValueSerde;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;


import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;

/**
 * A metered {@link VersionedKeyValueStore} wrapper that is used for recording operation
 * metrics, and hence its inner {@link VersionedBytesStore} implementation does not need to provide
 * its own metrics collecting functionality. The inner {@code VersionedBytesStore} of this class
 * is a {@link KeyValueStore} of type &lt;Bytes,byte[]&gt;, so we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;.
 *
 * @param <K> The key type
 * @param <V> The (raw) value type
 */
public class MeteredVersionedKeyValueStore<K, V>
    extends WrappedStateStore<VersionedBytesStore, K, V>
    implements VersionedKeyValueStore<K, V> {

    private final MeteredVersionedKeyValueStoreInternal internal;

    MeteredVersionedKeyValueStore(final VersionedBytesStore inner,
                                  final String metricScope,
                                  final Time time,
                                  final Serde<K> keySerde,
                                  final Serde<V> valueSerde) {
        super(inner);
        internal = new MeteredVersionedKeyValueStoreInternal(inner, metricScope, time, keySerde, valueSerde);
    }

    /**
     * Conceptually, {@link MeteredVersionedKeyValueStore} should {@code extend}
     * {@link MeteredKeyValueStore}, but due to type conflicts, we cannot do this. (Specifically,
     * the first needs to be {@link VersionedKeyValueStore} while the second is {@link KeyValueStore}
     * and the two interfaces conflict.) Thus, we use an internal <it>instance</it> of
     * {@code MeteredKeyValueStore} to mimic inheritance instead.
     * <p>
     * It's not ideal because it requires an extra step to translate between the APIs of
     * {@link VersionedKeyValueStore} in {@link MeteredVersionedKeyValueStore} and
     * the APIs of {@link TimestampedKeyValueStore} in {@link MeteredVersionedKeyValueStoreInternal}.
     * This extra step is all that the methods of {@code MeteredVersionedKeyValueStoreInternal} do.
     * <p>
     * Note that the addition of {@link #get(Object, long)} and {@link #delete(Object, long)} in
     * this class are to match the interface of {@link VersionedKeyValueStore}.
     */
    private class MeteredVersionedKeyValueStoreInternal
        extends MeteredKeyValueStore<K, ValueAndTimestamp<V>> {

        private final VersionedBytesStore inner;
        private final Serde<V> plainValueSerde;
        private StateSerdes<K, V> plainValueSerdes;

        private final Map<Class, QueryHandler> queryHandlers =
            mkMap(
                mkEntry(
                    RangeQuery.class,
                    (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
                ),
                mkEntry(
                    KeyQuery.class,
                    (query, positionBound, config, store) -> runKeyQuery(query, positionBound, config)
                ),
                mkEntry(
                    VersionedKeyQuery.class,
                    (query, positionBound, config, store) -> runVersionedKeyQuery(query, positionBound, config)
                )
            );

        MeteredVersionedKeyValueStoreInternal(final VersionedBytesStore inner,
                                              final String metricScope,
                                              final Time time,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
            super(
                inner,
                metricScope,
                time,
                keySerde,
                valueSerde == null
                    ? null
                    : new ValueAndTimestampSerde<>(valueSerde)
            );
            this.inner = inner;
            this.plainValueSerde = valueSerde;
        }

        public long put(final K key, final V value, final long timestamp) {
            Objects.requireNonNull(key, "key cannot be null");
            try {
                final long validTo = maybeMeasureLatency(() -> inner.put(keyBytes(key), plainValueSerdes.rawValue(value), timestamp), time, putSensor);
                maybeRecordE2ELatency();
                return validTo;
            } catch (final ProcessorStateException e) {
                final String message = String.format(e.getMessage(), key, value);
                throw new ProcessorStateException(message, e);
            }
        }

        public ValueAndTimestamp<V> get(final K key, final long asOfTimestamp) {
            Objects.requireNonNull(key, "key cannot be null");
            try {
                return maybeMeasureLatency(() -> outerValue(inner.get(keyBytes(key), asOfTimestamp)), time, getSensor);
            } catch (final ProcessorStateException e) {
                final String message = String.format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        public ValueAndTimestamp<V> delete(final K key, final long timestamp) {
            Objects.requireNonNull(key, "key cannot be null");
            try {
                return maybeMeasureLatency(() -> outerValue(inner.delete(keyBytes(key), timestamp)), time, deleteSensor);
            } catch (final ProcessorStateException e) {
                final String message = String.format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {

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
        protected <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig config) {
            // throw exception for now to reserve the ability to implement this in the future
            // without clashing with users' custom implementations in the meantime
            throw new UnsupportedOperationException("Versioned stores do not support RangeQuery queries at this time.");
        }

        @Override
        protected <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                                 final PositionBound positionBound,
                                                 final QueryConfig config) {
            // throw exception for now to reserve the ability to implement this in the future
            // without clashing with users' custom implementations in the meantime
            throw new UnsupportedOperationException("Versioned stores do not support KeyQuery queries at this time.");
        }

        @SuppressWarnings("unchecked")
        private <R> QueryResult<R> runVersionedKeyQuery(final Query<R> query,
                                                          final PositionBound positionBound,
                                                          final QueryConfig config) {
            final QueryResult<R> result;
            final VersionedKeyQuery<K, V> typedKeyQuery = (VersionedKeyQuery<K, V>) query;
            VersionedKeyQuery<Bytes, byte[]> rawKeyQuery = VersionedKeyQuery.withKey(keyBytes(typedKeyQuery.key()));
            if (typedKeyQuery.asOfTimestamp().isPresent()) {
                rawKeyQuery = rawKeyQuery.asOf(typedKeyQuery.asOfTimestamp().get());
            }
            final QueryResult<VersionedRecord<byte[]>> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess() && rawResult.getResult() != null) {
                final VersionedRecord<V> versionedRecord = StoreQueryUtils.deserializeVersionedRecord(plainValueSerdes, rawResult.getResult());
                final QueryResult<VersionedRecord<V>> typedQueryResult =
                    InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, versionedRecord);
                result = (QueryResult<R>) typedQueryResult;
            } else {
                // the generic type doesn't matter, since failed queries have no result set.
                result = (QueryResult<R>) rawResult;
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Serde<ValueAndTimestamp<V>> prepareValueSerdeForStore(
            final Serde<ValueAndTimestamp<V>> valueSerde,
            final SerdeGetter getter
        ) {
            if (valueSerde == null) {
                return new ValueAndTimestampSerde<>((Serde<V>) getter.valueSerde());
            } else {
                return super.prepareValueSerdeForStore(valueSerde, getter);
            }
        }

        @Deprecated
        @Override
        protected void initStoreSerde(final ProcessorContext context) {
            super.initStoreSerde(context);

            // additionally init raw value serde
            final String storeName = super.name();
            final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
            plainValueSerdes = new StateSerdes<>(
                changelogTopic,
                prepareKeySerde(keySerde, new SerdeGetter(context)),
                prepareValueSerde(plainValueSerde, new SerdeGetter(context))
            );
        }

        @Override
        protected void initStoreSerde(final StateStoreContext context) {
            super.initStoreSerde(context);

            // additionally init raw value serde
            final String storeName = super.name();
            final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
            plainValueSerdes = new StateSerdes<>(
                changelogTopic,
                prepareKeySerde(keySerde, new SerdeGetter(context)),
                prepareValueSerde(plainValueSerde, new SerdeGetter(context))
            );
        }
    }

    @Override
    public long put(final K key, final V value, final long timestamp) {
        return internal.put(key, value, timestamp);
    }

    @Override
    public VersionedRecord<V> delete(final K key, final long timestamp) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.delete(key, timestamp);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public VersionedRecord<V> get(final K key) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.get(key);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public VersionedRecord<V> get(final K key, final long asOfTimestamp) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.get(key, asOfTimestamp);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public String name() {
        return internal.name();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        internal.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        internal.init(context, root);
    }

    @Override
    public void flush() {
        internal.flush();
    }

    @Override
    public void close() {
        internal.close();
    }

    @Override
    public boolean persistent() {
        return internal.persistent();
    }

    @Override
    public boolean isOpen() {
        return internal.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return internal.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return internal.getPosition();
    }
}