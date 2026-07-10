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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
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
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class MeteredSessionStoreWithHeaders<K, AGG>
    extends MeteredSessionStore<K, AggregationWithHeaders<AGG>>
    implements SessionStoreWithHeaders<K, AGG> {

    MeteredSessionStoreWithHeaders(
        final SessionStore<Bytes, byte[]> inner,
        final String metricsScope,
        final Serde<K> keySerde,
        final Serde<AggregationWithHeaders<AGG>> aggSerde,
        final Time time
    ) {
        super(inner, metricsScope, keySerde, aggSerde, time);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<AggregationWithHeaders<AGG>> prepareValueSerdeForStore(
            final Serde<AggregationWithHeaders<AGG>> valueSerde,
            final SerdeGetter getter
    ) {
        if (valueSerde == null) {
            return new AggregationWithHeadersSerde<>((Serde<AGG>) getter.valueSerde());
        }
        return super.prepareValueSerdeForStore(valueSerde, getter);
    }

    private Bytes serializeKey(final K key, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }

    private K deserializeKey(final byte[] rawKey, final Headers headers) {
        return serdes.keyFrom(rawKey, headers);
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AggregationWithHeaders<AGG> aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        try {
            maybeMeasureLatency(
                () -> {
                    if (aggregate == null) {
                        final ProcessorRecordContext currentContext = internalContext.recordContext();

                        // Create new headers object to isolate tombstone operation from input record
                        final Headers deleteHeaders = new RecordHeaders(currentContext.headers());

                        // Create temporary context with new headers
                        final ProcessorRecordContext temporaryContext = new ProcessorRecordContext(
                            currentContext.timestamp(),
                            currentContext.offset(),
                            currentContext.partition(),
                            currentContext.topic(),
                            deleteHeaders
                        );

                        try {
                            internalContext.setRecordContext(temporaryContext);
                            wrapped().put(
                                new Windowed<>(
                                    serializeKey(sessionKey.key(), deleteHeaders),
                                    sessionKey.window()
                                ),
                                null
                            );
                        } finally {
                            // Restore original context
                            internalContext.setRecordContext(currentContext);
                        }
                    } else {
                        // it's ok to only pass headers into `serializeKey`, because for the value case passed-in headers are
                        // getting ignored anyway, because the value (of type `AggregationWithHeaders`) itself carries the headers
                        wrapped().put(
                            new Windowed<>(
                                serializeKey(sessionKey.key(), aggregate.headers()),
                                sessionKey.window()
                            ),
                            serializeValue(aggregate)
                        );
                    }
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
                    final ProcessorRecordContext currentContext = internalContext.recordContext();

                    // Create new headers object to isolate delete operation from input record
                    final Headers deleteHeaders = new RecordHeaders(currentContext.headers());

                    // Create temporary context with new headers
                    final ProcessorRecordContext temporaryContext = new ProcessorRecordContext(
                        currentContext.timestamp(),
                        currentContext.offset(),
                        currentContext.partition(),
                        currentContext.topic(),
                        deleteHeaders
                    );

                    try {
                        internalContext.setRecordContext(temporaryContext);
                        wrapped().remove(
                            new Windowed<>(serializeKey(sessionKey.key(), deleteHeaders), sessionKey.window())
                        );
                    } finally {
                        // Restore original context
                        internalContext.setRecordContext(currentContext);
                    }
                },
                time,
                removeSensor
            );
        } catch (final ProcessorStateException e) {
            throw new ProcessorStateException(String.format(e.getMessage(), sessionKey.key()), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        if (query instanceof WindowRangeQuery) {
            result = runRangeQuery((WindowRangeQuery<K, AGG>) query, positionBound, config);
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " with serdes " + serdes + " in " + (time.nanoseconds() - start) + "ns");
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
            wrapped().fetch(serializeKey(key, internalContext.headers()))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFetch(serializeKey(key, internalContext.headers()))
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(
        final K keyFrom,
        final K keyTo
    ) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().fetch(
                serializeKey(keyFrom, internalContext.headers()),
                serializeKey(keyTo, internalContext.headers())
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(
        final K keyFrom,
        final K keyTo
    ) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFetch(
                serializeKey(keyFrom, internalContext.headers()),
                serializeKey(keyTo, internalContext.headers())
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(
        final K key,
        final long earliestSessionEndTime,
        final long latestSessionStartTime
    ) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().findSessions(
                serializeKey(key, internalContext.headers()),
                earliestSessionEndTime,
                latestSessionStartTime
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(
        final K key,
        final long earliestSessionEndTime,
        final long latestSessionStartTime
    ) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFindSessions(
                serializeKey(key, internalContext.headers()),
                earliestSessionEndTime,
                latestSessionStartTime
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(
        final K keyFrom,
        final K keyTo,
        final long earliestSessionEndTime,
        final long latestSessionStartTime
    ) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().findSessions(
                serializeKey(keyFrom, internalContext.headers()),
                serializeKey(keyTo, internalContext.headers()),
                earliestSessionEndTime,
                latestSessionStartTime
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(
        final K keyFrom,
        final K keyTo,
        final long earliestSessionEndTime,
        final long latestSessionStartTime
    ) {
        return new MeteredSessionStoreWithHeadersIterator(
            wrapped().backwardFindSessions(
                serializeKey(keyFrom, internalContext.headers()),
                serializeKey(keyTo, internalContext.headers()),
                earliestSessionEndTime,
                latestSessionStartTime
            )
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(
        final long earliestSessionEndTime,
        final long latestSessionEndTime
    ) {
        return new MeteredSessionStoreWithHeadersIterator(wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime));
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runRangeQuery(
        final WindowRangeQuery<K, AGG> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
        final QueryResult<R> queryResult;

        if (query.getKey().isPresent()) {
            final WindowRangeQuery<Bytes, byte[]> rawKeyQuery =
                WindowRangeQuery.withKey(serializeKey(query.getKey().get(), internalContext.headers()));
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                wrapped().query(rawKeyQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                final MeteredWindowedKeyValueIterator<K, AGG> typedResult =
                    new MeteredWindowedKeyValueWithHeadersIterator<>(
                        rawResult.getResult(),
                        fetchSensor,
                        iteratorDurationSensor,
                        this::deserializeValue,
                        this::deserializeKey,
                        AggregationWithHeaders::headers,
                        aggregationWithHeaders -> aggregationWithHeaders == null ? null : aggregationWithHeaders.aggregation(),
                        time,
                        numOpenIterators,
                        openIterators
                    );
                queryResult = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, typedResult);
            } else {
                queryResult = (QueryResult<R>) rawResult;
            }
        } else {
            queryResult = QueryResult.forFailure(
                FailureReason.UNKNOWN_QUERY_TYPE,
                "This store (" + getClass() + ") doesn't know how to"
                    + " execute the given query (" + query + ") because"
                    + " SessionStores only support WindowRangeQuery.withKey."
                    + " Contact the store maintainer if you need support"
                    + " for a new query type."
            );
        }
        return queryResult;
    }

    @Override
    public ReadOnlySessionStore<K, AggregationWithHeaders<AGG>> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyHeadersView(wrapped().readOnly(isolationLevel));
    }

    private final class ReadOnlyHeadersView implements ReadOnlySessionStore<K, AggregationWithHeaders<AGG>> {

        private final ReadOnlySessionStore<Bytes, byte[]> underlying;

        ReadOnlyHeadersView(final ReadOnlySessionStore<Bytes, byte[]> underlying) {
            this.underlying = underlying;
        }

        @Override
        public AggregationWithHeaders<AGG> fetchSession(
            final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
            Objects.requireNonNull(key, "key cannot be null");
            return maybeMeasureLatency(
                () -> deserializeValue(underlying.fetchSession(
                    serializeKey(key, internalContext.headers()), earliestSessionEndTime, latestSessionStartTime)),
                time,
                fetchSensor
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K key) {
            Objects.requireNonNull(key, "key cannot be null");
            return new MeteredSessionStoreWithHeadersIterator(underlying.fetch(serializeKey(key, internalContext.headers())));
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K key) {
            Objects.requireNonNull(key, "key cannot be null");
            return new MeteredSessionStoreWithHeadersIterator(underlying.backwardFetch(serializeKey(key, internalContext.headers())));
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K keyFrom, final K keyTo) {
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.fetch(serializeKey(keyFrom, internalContext.headers()), serializeKey(keyTo, internalContext.headers()))
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K keyFrom, final K keyTo) {
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.backwardFetch(serializeKey(keyFrom, internalContext.headers()), serializeKey(keyTo, internalContext.headers()))
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(
            final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
            Objects.requireNonNull(key, "key cannot be null");
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.findSessions(serializeKey(key, internalContext.headers()), earliestSessionEndTime, latestSessionStartTime)
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(
            final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
            Objects.requireNonNull(key, "key cannot be null");
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.backwardFindSessions(serializeKey(key, internalContext.headers()), earliestSessionEndTime, latestSessionStartTime)
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(
            final K keyFrom, final K keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.findSessions(
                    serializeKey(keyFrom, internalContext.headers()),
                    serializeKey(keyTo, internalContext.headers()),
                    earliestSessionEndTime,
                    latestSessionStartTime)
            );
        }

        @Override
        public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(
            final K keyFrom, final K keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
            return new MeteredSessionStoreWithHeadersIterator(
                underlying.backwardFindSessions(
                    serializeKey(keyFrom, internalContext.headers()),
                    serializeKey(keyTo, internalContext.headers()),
                    earliestSessionEndTime,
                    latestSessionStartTime)
            );
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

            final AggregationWithHeaders<AGG> value = deserializeValue(next.value);
            final Headers headers = value != null ? value.headers() : new RecordHeaders();
            final K key = deserializeKey(next.key.key().get(), headers);
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
