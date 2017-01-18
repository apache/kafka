/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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
        SessionStore<K, V> store;

        // for session stores, the key schema needs to be used in both
        // underlying segmented bytes store as well as in caching
        SessionKeySchema keySchema = new SessionKeySchema();

        SegmentedBytesStore segmented = new RocksDBSegmentedBytesStore(name, retentionPeriod, NUM_SEGMENTS, keySchema);

        if (cached && logged) {
            // logging wrapper
            segmented = new ChangeLoggingSegmentedBytesStore(segmented);

            // metering wrapper, currently enforced
            segmented = new MeteredSegmentedBytesStore(segmented, METRIC_SCOPE, time);

            // sessioned
            SessionStore<Bytes, byte[]> bytes = RocksDBSessionStore.bytesStore(segmented);

            // caching wrapper
            store = new CachingSessionStore<>(bytes, keySerde, valueSerde);
        } else if (cached) {
            // metering wrapper, currently enforced
            segmented = new MeteredSegmentedBytesStore(segmented, METRIC_SCOPE, time);

            // windowed
            SessionStore<Bytes, byte[]> bytes = RocksDBSessionStore.bytesStore(segmented);

            // caching wrapper
            store = new CachingSessionStore<>(bytes, keySerde, valueSerde);
        } else if (logged) {
            // logging wrapper
            segmented = new ChangeLoggingSegmentedBytesStore(segmented);

            // metering wrapper, currently enforced
            segmented = new MeteredSegmentedBytesStore(segmented, METRIC_SCOPE, time);

            // windowed
            store = new RocksDBSessionStore<>(segmented, keySerde, valueSerde);
        } else {
            // metering wrapper, currently enforced
            segmented = new MeteredSegmentedBytesStore(segmented, METRIC_SCOPE, time);

            // windowed
            store = new RocksDBSessionStore<>(segmented, keySerde, valueSerde);
        }

        return store;
    }

    public long retentionPeriod() {
        return retentionPeriod;
    }
}
