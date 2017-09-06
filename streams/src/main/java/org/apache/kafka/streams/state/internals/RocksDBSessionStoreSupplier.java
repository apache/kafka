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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Map;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class RocksDBSessionStoreSupplier<K, V> extends AbstractStoreSupplier<K, V, SessionStore> implements WindowStoreSupplier<SessionStore> {

    private static final String METRIC_SCOPE = "rocksdb-session";
    private static final int NUM_SEGMENTS = 3;
    private final long retentionPeriod;
    private final boolean cached;

    public RocksDBSessionStoreSupplier(String name, long retentionPeriod, Serde<K> keySerde, Serde<V> valueSerde, boolean logged, Map<String, String> logConfig, boolean cached) {
        super(name, keySerde, valueSerde, Time.SYSTEM, logged, logConfig);
        this.retentionPeriod = retentionPeriod;
        this.cached = cached;
    }

    public String name() {
        return name;
    }

    public SessionStore<K, V> get() {
        final SessionKeySchema keySchema = new SessionKeySchema();
        final long segmentInterval = Segments.segmentInterval(retentionPeriod, NUM_SEGMENTS);
        final RocksDBSegmentedBytesStore segmented = new RocksDBSegmentedBytesStore(name,
                                                                                    retentionPeriod,
                                                                                    NUM_SEGMENTS,
                                                                                    keySchema);

        final RocksDBSessionStore<Bytes, byte[]> bytesStore = RocksDBSessionStore.bytesStore(segmented);
        return new MeteredSessionStore<>(maybeWrapCaching(maybeWrapLogged(bytesStore), segmentInterval),
                                         METRIC_SCOPE,
                                         keySerde,
                                         valueSerde,
                                         time);

    }

    private SessionStore<Bytes, byte[]> maybeWrapLogged(final SessionStore<Bytes, byte[]> inner) {
        if (!logged) {
            return inner;
        }
        return new ChangeLoggingSessionBytesStore(inner);
    }

    private SessionStore<Bytes, byte[]> maybeWrapCaching(final SessionStore<Bytes, byte[]> inner, final long segmentInterval) {
        if (!cached) {
            return inner;
        }
        return new CachingSessionStore<>(inner, keySerde, valueSerde, segmentInterval);
    }

    public long retentionPeriod() {
        return retentionPeriod;
    }
}
