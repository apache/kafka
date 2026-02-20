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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

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

    // Factory method to create the wrapped serde
    static <AGG> Serde<AggregationWithHeaders<AGG>> createAggregationWithHeadersSerde(final Serde<AGG> aggSerde) {
        return new Serde<>() {
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
                            record.value().newValue != null ? serdes.valueFrom(record.value().newValue, new RecordHeaders()) : null,
                            record.value().oldValue != null ? serdes.valueFrom(record.value().oldValue, new RecordHeaders()) : null,
                            record.value().isLatest
                        ))
                ),
                sendOldValues);
        }
        return false;
    }
}
