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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StateSerdes;

import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

public class MeteredSessionStoreWithHeaders<K, AGG>
    extends MeteredSessionStore<K, AggregationWithHeaders<AGG>>
    implements SessionStoreWithHeaders<K, AGG> {

    private final Serde<AGG> rawAggSerde;
    private StateSerdes<K, AGG> serdes;
    private AggregationWithHeadersSerializer<AGG> aggregationWithHeadersSerializer;
    private AggregationWithHeadersDeserializer<AGG> aggregationWithHeadersDeserializer;

    MeteredSessionStoreWithHeaders(final SessionStore<Bytes, byte[]> inner,
                                   final String metricsScope,
                                   final Serde<K> keySerde,
                                   final Serde<AGG> aggSerde,
                                   final Time time) {
        super(inner, metricsScope, keySerde, createAggregationWithHeadersSerde(aggSerde), time);
        this.rawAggSerde = aggSerde;
    }

    private static <AGG> Serde<AggregationWithHeaders<AGG>> createAggregationWithHeadersSerde(final Serde<AGG> aggSerde) {
        return new Serde<AggregationWithHeaders<AGG>>() {
            @Override
            public Serializer<AggregationWithHeaders<AGG>> serializer() {
                return new AggregationWithHeadersSerializer<>(aggSerde.serializer());
            }

            @Override
            public Deserializer<AggregationWithHeaders<AGG>> deserializer() {
                return new AggregationWithHeadersDeserializer<>(aggSerde.deserializer());
            }
        };
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        initStoreSerde(stateStoreContext);
        super.init(stateStoreContext, root);
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = StoreSerdeInitializer.prepareStoreSerde(
            context, storeName, changelogTopic, keySerde, rawAggSerde, WrappingNullableUtils::prepareValueSerde);
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

    // Override default methods to resolve ambiguity between SessionStore and SessionStoreWithHeaders
    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K key,
                                                                                    final Instant earliestSessionEndTime,
                                                                                    final Instant latestSessionStartTime) {
        return findSessions(
            key,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K key,
                                                                                            final Instant earliestSessionEndTime,
                                                                                            final Instant latestSessionStartTime) {
        return backwardFindSessions(
            key,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K keyFrom,
                                                                                    final K keyTo,
                                                                                    final Instant earliestSessionEndTime,
                                                                                    final Instant latestSessionStartTime) {
        return findSessions(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K keyFrom,
                                                                                            final K keyTo,
                                                                                            final Instant earliestSessionEndTime,
                                                                                            final Instant latestSessionStartTime) {
        return backwardFindSessions(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    @Override
    public AggregationWithHeaders<AGG> fetchSession(final K key,
                                                     final Instant sessionStartTime,
                                                     final Instant sessionEndTime) {
        return fetchSession(key,
            ApiUtils.validateMillisecondInstant(sessionStartTime,
                prepareMillisCheckFailMsgPrefix(sessionStartTime, "sessionStartTime")),
            ApiUtils.validateMillisecondInstant(sessionEndTime,
                prepareMillisCheckFailMsgPrefix(sessionEndTime, "sessionEndTime")));
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
}
