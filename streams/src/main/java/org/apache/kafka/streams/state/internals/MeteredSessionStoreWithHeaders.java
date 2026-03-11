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
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;
import static org.apache.kafka.streams.state.internals.Utils.keyBytes;

public class MeteredSessionStoreWithHeaders<K, AGG>
    extends MeteredSessionStore<K, AggregationWithHeaders<AGG>>
    implements SessionStoreWithHeaders<K, AGG> {

    MeteredSessionStoreWithHeaders(final SessionStore<Bytes, byte[]> inner,
                                   final String metricsScope,
                                   final Serde<K> keySerde,
                                   final Serde<AggregationWithHeaders<AGG>> aggSerde,
                                   final Time time) {
        super(inner, metricsScope, keySerde, aggSerde, time);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<AggregationWithHeaders<AGG>> prepareValueSerdeForStore(
            final Serde<AggregationWithHeaders<AGG>> valueSerde,
            final SerdeGetter getter) {
        if (valueSerde == null) {
            return new AggregationWithHeadersSerde<>((Serde<AGG>) getter.valueSerde());
        }
        return super.prepareValueSerdeForStore(valueSerde, getter);
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AggregationWithHeaders<AGG> aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        try {
            final Headers headers = aggregate != null ? aggregate.headers() : new RecordHeaders();
            final Bytes key = keyBytes(sessionKey, headers, serdes);
            maybeMeasureLatency(() -> wrapped().put(new Windowed<>(key, sessionKey.window()),
                serdes.rawValue(aggregate, headers)), time, putSensor);
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = time.nanoseconds();
        final QueryResult<R> result;

        if (query instanceof WindowRangeQuery) {
            final WindowRangeQuery<K, AGG> windowRangeQuery = (WindowRangeQuery<K, AGG>) query;
            if (windowRangeQuery.getKey().isPresent()) {
                result = runRangeQuery(query, positionBound, config);
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
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " with serdes "
                        + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
        } else {
            result = wrapped().query(query, positionBound, config);
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " in " + (time.nanoseconds() - start) + "ns");
            }
        }
        return result;
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().fetch(keyBytes(key, new RecordHeaders(), serdes))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFetch(keyBytes(key, new RecordHeaders(), serdes))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K keyFrom,
                                                                            final K keyTo) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().fetch(
                keyBytes(keyFrom, new RecordHeaders(), serdes),
                keyBytes(keyTo, new RecordHeaders(), serdes))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K keyFrom,
                                                                                    final K keyTo) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFetch(
                keyBytes(keyFrom, new RecordHeaders(), serdes),
                keyBytes(keyTo, new RecordHeaders(), serdes))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K key,
                                                                                   final long earliestSessionEndTime,
                                                                                   final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().findSessions(
                keyBytes(key, new RecordHeaders(), serdes),
                earliestSessionEndTime,
                latestSessionStartTime)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K key,
                                                                                           final long earliestSessionEndTime,
                                                                                           final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFindSessions(
                keyBytes(key, new RecordHeaders(), serdes),
                earliestSessionEndTime,
                latestSessionStartTime)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K keyFrom,
                                                                                   final K keyTo,
                                                                                   final long earliestSessionEndTime,
                                                                                   final long latestSessionStartTime) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().findSessions(
                keyBytes(keyFrom, new RecordHeaders(), serdes),
                keyBytes(keyTo, new RecordHeaders(), serdes),
                earliestSessionEndTime,
                latestSessionStartTime)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K keyFrom,
                                                                                           final K keyTo,
                                                                                           final long earliestSessionEndTime,
                                                                                           final long latestSessionStartTime) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFindSessions(
                keyBytes(keyFrom, new RecordHeaders(), serdes),
                keyBytes(keyTo, new RecordHeaders(), serdes),
                earliestSessionEndTime,
                latestSessionStartTime)
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final long earliestSessionEndTime,
                                                                                   final long latestSessionEndTime) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime)
        );
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                             final PositionBound positionBound,
                                             final QueryConfig config) {
        final WindowRangeQuery<K, AGG> typedQuery = (WindowRangeQuery<K, AGG>) query;
        final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
            WindowRangeQuery.withKey(
                Bytes.wrap(serdes.rawKey(typedQuery.getKey().get(), new RecordHeaders()))
            );
        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
            wrapped().query(rawKeyQuery, positionBound, config);
        if (rawResult.isSuccess()) {
            final MeteredWindowedKeyValueIterator<K, AGG> typedResult =
                new MeteredWindowedKeyValueIterator<>(
                    rawResult.getResult(),
                    fetchSensor,
                    iteratorDurationSensor,
                    streamsMetrics,
                    bytes -> serdes.keyFrom(bytes, new RecordHeaders()),
                    byteArray -> {
                        final AggregationWithHeaders<AGG> awh =
                            serdes.valueDeserializer().deserialize(serdes.topic(), byteArray);
                        return awh == null ? null : awh.aggregation();
                    },
                    time,
                    numOpenIterators,
                    openIterators
                );
            return (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
        } else {
            return (QueryResult<R>) rawResult;
        }
    }

    private class MeteredSessionStoreWithHeadersIterator
        implements KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>>, MeteredIterator {

        private final KeyValueIterator<Windowed<Bytes>, byte[]> iter;
        private final long startNs;
        private final long startTimestampMs;
        private KeyValue<Windowed<K>, AggregationWithHeaders<AGG>> cachedNext;

        private MeteredSessionStoreWithHeadersIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> iter) {
            this.iter = iter;
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
        public KeyValue<Windowed<K>, AggregationWithHeaders<AGG>> next() {
            if (cachedNext != null) {
                final KeyValue<Windowed<K>, AggregationWithHeaders<AGG>> result = cachedNext;
                cachedNext = null;
                return result;
            }

            final KeyValue<Windowed<Bytes>, byte[]> next = iter.next();

            final AggregationWithHeaders<AGG> value = serdes.valueFrom(next.value, new RecordHeaders());
            final Headers headers = value != null ? value.headers() : new RecordHeaders();
            final K key = serdes.keyFrom(next.key.key().get(), headers);
            final Windowed<K> windowedKey = new Windowed<>(key, next.key.window());
            return KeyValue.pair(windowedKey, value);
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
            return cachedNext.key;
        }
    }
}
