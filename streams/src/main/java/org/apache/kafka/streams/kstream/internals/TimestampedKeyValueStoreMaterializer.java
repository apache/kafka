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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

public class TimestampedKeyValueStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized;

    public TimestampedKeyValueStoreMaterializer(final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        this.materialized = materialized;
    }

    /**
     * @return  StoreBuilder
     */
    public StoreBuilder<TimestampedKeyValueStore<K, V>> materialize() {
        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            switch (materialized.storeType()) {
                case IN_MEMORY:
                    supplier = Stores.inMemoryKeyValueStore(materialized.storeName());
                    break;
                case ROCKS_DB:
                    supplier = Stores.persistentTimestampedKeyValueStore(materialized.storeName());
                    break;
                default:
                    throw new IllegalStateException("Unknown store type: " + materialized.storeType());
            }
        }

        final StoreBuilder<TimestampedKeyValueStore<K, V>> builder = Stores.timestampedKeyValueStoreBuilder(
            supplier,
            materialized.keySerde(),
            materialized.valueSerde());

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }
        return builder;
    }
}
