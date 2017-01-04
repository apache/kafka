/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Map;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */

public class RocksDBSessionStoreSupplier<K, T> implements StateStoreSupplier, WindowStoreSupplier {

    private static final int NUM_SEGMENTS = 3;
    private final String name;
    private final long retentionPeriod;
    private final Serde<K> keySerde;
    private final Serde<T> valueSerde;
    private final boolean logged;
    private final Map<String, String> logConfig;
    private final boolean enableCaching;

    public RocksDBSessionStoreSupplier(String name, long retentionPeriod, Serde<K> keySerde, Serde<T> valueSerde, boolean logged, Map<String, String> logConfig, boolean enableCaching) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.logged = logged;
        this.logConfig = logConfig;
        this.enableCaching = enableCaching;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        final RocksDBSegmentedBytesStore bytesStore = new RocksDBSegmentedBytesStore(name,
                                                                                     retentionPeriod,
                                                                                     NUM_SEGMENTS,
                                                                                     new SessionKeySchema());
        final MeteredSegmentedBytesStore metered = new MeteredSegmentedBytesStore(logged ? new ChangeLoggingSegmentedBytesStore(bytesStore)
                                                                                          : bytesStore, "rocksdb-session-store", new SystemTime());
        if (enableCaching) {
            return new CachingSessionStore<>(metered, keySerde, valueSerde);
        }
        return new RocksDBSessionStore<>(metered, keySerde, valueSerde);

    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return logged;
    }

    public long retentionPeriod() {
        return retentionPeriod;
    }
}
