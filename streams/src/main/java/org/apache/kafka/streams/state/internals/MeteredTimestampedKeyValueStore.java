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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;
import static org.apache.kafka.streams.state.internals.StoreQueryUtils.getDeserializeValue;

/**
 * A Metered {@link TimestampedKeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredTimestampedKeyValueStore<K, V>
    extends MeteredKeyValueStore<K, ValueAndTimestamp<V>> 
    implements TimestampedKeyValueStore<K, V> {

    MeteredTimestampedKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                                    final String metricScope,
                                    final Time time,
                                    final Serde<K> keySerde,
                                    final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class, StoreQueryUtils.QueryHandler> queryHandlers =
            mkMap(
                    mkEntry(
                            RangeQuery.class,
                            (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
                    ),
                    mkEntry(
                            TimestampedRangeQuery.class,
                            (query, positionBound, config, store) -> runTimestampedRangeQuery(query, positionBound, config)
                    ),
                    mkEntry(
                            KeyQuery.class,
                            (query, positionBound, config, store) -> runKeyQuery(query, positionBound, config)
                    ),
                    mkEntry(
                            TimestampedKeyQuery.class,
                            (query, positionBound, config, store) -> runTimestampedKeyQuery(query, positionBound, config)
                    )
            );
    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueAndTimestamp<V>> prepareValueSerdeForStore(final Serde<ValueAndTimestamp<V>> valueSerde, final SerdeGetter getter) {
        if (valueSerde == null) {
            return new ValueAndTimestampSerde<>((Serde<V>) getter.valueSerde());
        } else {
            return super.prepareValueSerdeForStore(valueSerde, getter);
        }
    }


    public RawAndDeserializedValue<V> getWithBinary(final K key) {
        try {
            return maybeMeasureLatency(() -> { 
                final byte[] serializedValue = wrapped().get(keyBytes(key));
                return new RawAndDeserializedValue<>(serializedValue, outerValue(serializedValue));
            }, time, getSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    public boolean putIfDifferentValues(final K key,
                                        final ValueAndTimestamp<V> newValue,
                                        final byte[] oldSerializedValue) {
        try {
            return maybeMeasureLatency(
                () -> {
                    final byte[] newSerializedValue = serdes.rawValue(newValue);
                    if (ValueAndTimestampSerializer.valuesAreSameAndTimeIsIncreasing(oldSerializedValue, newSerializedValue)) {
                        return false;
                    } else {
                        wrapped().put(keyBytes(key), newSerializedValue);
                        return true;
                    }
                },
                time,
                putSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, newValue);
            throw new ProcessorStateException(message, e);
        }
    }

    static class RawAndDeserializedValue<ValueType> {
        final byte[] serializedValue;
        final ValueAndTimestamp<ValueType> value;
        RawAndDeserializedValue(final byte[] serializedValue, final ValueAndTimestamp<ValueType> value) {
            this.serializedValue = serializedValue;
            this.value = value;
        }
    }

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
                result.addExecutionInfo(
                        "Handled in " + getClass() + " with serdes "
                                + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
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
                KeyQuery.withKey(keyBytes(typedKeyQuery.key()));
        final QueryResult<byte[]> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final Function<byte[], ValueAndTimestamp<V>> deserializer = getDeserializeValue(serdes, wrapped());
            final ValueAndTimestamp<V> valueAndTimestamp = deserializer.apply(rawResult.getResult());
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
    private <R> QueryResult<R> runTimestampedRangeQuery(final Query<R> query,
                                                        final PositionBound positionBound,
                                                        final QueryConfig config) {

        final QueryResult<R> result;
        final TimestampedRangeQuery<K, V> typedQuery = (TimestampedRangeQuery<K, V>) query;
        RangeQuery<Bytes, byte[]> rawRangeQuery;
        final ResultOrder order = typedQuery.resultOrder();
        rawRangeQuery = RangeQuery.withRange(
                keyBytes(typedQuery.lowerBound().orElse(null)),
                keyBytes(typedQuery.upperBound().orElse(null))
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
            final KeyValueIterator<K, ValueAndTimestamp<V>> resultIterator = (KeyValueIterator<K, ValueAndTimestamp<V>>) new MeteredTimestampedKeyValueStoreIterator(
                    iterator,
                    getSensor,
                    getDeserializeValue(serdes, wrapped()),
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
            final Function<byte[], ValueAndTimestamp<V>> deserializer = getDeserializeValue(serdes, wrapped());
            final ValueAndTimestamp<V> valueAndTimestamp = deserializer.apply(rawResult.getResult());
            final V plainValue = valueAndTimestamp == null ? null : valueAndTimestamp.value();
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
    private  <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                               final PositionBound positionBound,
                                               final QueryConfig config) {

        final QueryResult<R> result;
        final RangeQuery<K, V> typedQuery = (RangeQuery<K, V>) query;
        RangeQuery<Bytes, byte[]> rawRangeQuery;
        final ResultOrder order = typedQuery.resultOrder();
        rawRangeQuery = RangeQuery.withRange(
                keyBytes(typedQuery.getLowerBound().orElse(null)),
                keyBytes(typedQuery.getUpperBound().orElse(null))
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
            final KeyValueIterator<K, V> resultIterator = new MeteredTimestampedKeyValueStoreIterator(
                iterator,
                getSensor,
                getDeserializeValue(serdes, wrapped()),
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
    private class MeteredTimestampedKeyValueStoreIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;
        private final Function<byte[], ValueAndTimestamp<V>> valueAndTimestampDeserializer;

        private final boolean returnPlainValue;

        private MeteredTimestampedKeyValueStoreIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                                        final Sensor sensor,
                                                        final Function<byte[], ValueAndTimestamp<V>> valueAndTimestampDeserializer,
                                                        final boolean returnPlainValue) {
            this.iter = iter;
            this.sensor = sensor;
            this.valueAndTimestampDeserializer = valueAndTimestampDeserializer;
            this.startNs = time.nanoseconds();
            this.returnPlainValue = returnPlainValue;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            if (returnPlainValue) {
                final V plainValue = valueAndTimestampDeserializer.apply(keyValue.value).value();
                return KeyValue.pair(
                        serdes.keyFrom(keyValue.key.get()), plainValue);
            }
            return (KeyValue<K, V>) KeyValue.pair(
                    serdes.keyFrom(keyValue.key.get()),
                    valueAndTimestampDeserializer.apply(keyValue.value));
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
