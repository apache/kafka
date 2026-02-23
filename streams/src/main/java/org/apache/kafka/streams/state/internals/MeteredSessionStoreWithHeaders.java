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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

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

    @Override
    public void put(final Windowed<K> sessionKey, final AggregationWithHeaders<AGG> aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        try {
            final Headers headers = aggregate != null ? aggregate.headers() : new RecordHeaders();
            final Bytes key = keyBytes(sessionKey, headers);
            maybeMeasureLatency(() -> wrapped().put(new Windowed<>(key, sessionKey.window()),
                serdes.rawValue(aggregate, headers)), time, putSensor);
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        }

    }

    protected Bytes keyBytes(final Windowed<K> sessionKey, final Headers headers) {
        return Bytes.wrap(serdes.rawKey(sessionKey.key(), headers));
    }
}
