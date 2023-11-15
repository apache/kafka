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
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class VersionedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, V, VersionedKeyValueStore<K, V>> {

    private final VersionedBytesStoreSupplier storeSupplier;

    public VersionedKeyValueStoreBuilder(final VersionedBytesStoreSupplier storeSupplier,
                                         final Serde<K> keySerde,
                                         final Serde<V> valueSerde,
                                         final Time time) {
        super(
            storeSupplier.name(),
            keySerde,
            valueSerde,
            time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public VersionedKeyValueStore<K, V> build() {
        final KeyValueStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store instanceof VersionedBytesStore)) {
            throw new IllegalStateException("VersionedBytesStoreSupplier.get() must return an instance of VersionedBytesStore");
        }

        return new MeteredVersionedKeyValueStore<>(
            maybeWrapLogging((VersionedBytesStore) store), // no caching layer for versioned stores
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    @Override
    public StoreBuilder<VersionedKeyValueStore<K, V>> withCachingEnabled() {
        throw new IllegalStateException("Versioned stores do not support caching");
    }

    public long historyRetention() {
        return storeSupplier.historyRetentionMs();
    }

    private VersionedBytesStore maybeWrapLogging(final VersionedBytesStore inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingVersionedKeyValueBytesStore(inner);
    }
}