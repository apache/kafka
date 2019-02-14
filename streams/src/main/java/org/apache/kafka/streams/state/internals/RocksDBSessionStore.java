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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;


public class RocksDBSessionStore<K, AGG> extends WrappedStateStore<SegmentedBytesStore> implements SessionStore<K, AGG> {

    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;

    private StateSerdes<K, AGG> serdes;
    private String topic;

    RocksDBSessionStore(final SegmentedBytesStore bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde) {
        super(bytesStore);
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        final String storeName = name();
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);

        serdes = new StateSerdes<>(
            topic,
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);

        super.init(context, root);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            Bytes.wrap(serdes.rawKey(key)),
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator<>(bytesIterator, serdes);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom, final K keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            Bytes.wrap(serdes.rawKey(keyFrom)),
            Bytes.wrap(serdes.rawKey(keyTo)),
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator<>(bytesIterator, serdes);
    }

    @Override
    public AGG fetchSession(final K key, final long startTime, final long endTime) {
        return serdes.valueFrom(wrapped().get(SessionKeySchema.toBinary(Bytes.wrap(serdes.rawKey(key)), startTime, endTime)));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K from, final K to) {
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    @Override
    public void remove(final Windowed<K> key) {
        wrapped().remove(Bytes.wrap(SessionKeySchema.toBinary(key, serdes.keySerializer(), topic)));
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AGG aggregate) {
        wrapped().put(Bytes.wrap(SessionKeySchema.toBinary(sessionKey, serdes.keySerializer(), topic)), serdes.rawValue(aggregate));
    }
}
