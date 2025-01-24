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
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes a key-value store as either a {@link TimestampedKeyValueStoreBuilder} or a
 * {@link VersionedKeyValueStoreBuilder} depending on whether the store is versioned or not.
 */
public class KeyValueStoreMaterializer<K, V> extends MaterializedStoreFactory<K, V, KeyValueStore<Bytes, byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueStoreMaterializer.class);

    public KeyValueStoreMaterializer(
            final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized
    ) {
        super(materialized);
    }

    @Override
    public StoreBuilder<?> builder() {
        final KeyValueBytesStoreSupplier supplier = materialized.storeSupplier() == null
                ? dslStoreSuppliers().keyValueStore(new DslKeyValueParams(materialized.storeName(), true))
                : (KeyValueBytesStoreSupplier) materialized.storeSupplier();

        final StoreBuilder<?> builder;
        if (supplier instanceof VersionedBytesStoreSupplier) {
            builder = Stores.versionedKeyValueStoreBuilder(
                    (VersionedBytesStoreSupplier) supplier,
                    materialized.keySerde(),
                    materialized.valueSerde());
        } else {
            builder = Stores.timestampedKeyValueStoreBuilder(
                    supplier,
                    materialized.keySerde(),
                    materialized.valueSerde());
        }

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            if (!(builder instanceof VersionedKeyValueStoreBuilder)) {
                builder.withCachingEnabled();
            } else {
                LOG.info("Not enabling caching for store '{}' as versioned stores do not support caching.", supplier.name());
            }
        }


        return builder;
    }

    @Override
    public long retentionPeriod() {
        throw new IllegalStateException(
                "retentionPeriod is not supported when not a window store");
    }

    @Override
    public long historyRetention() {
        if (!(materialized.storeSupplier() instanceof VersionedBytesStoreSupplier)) {
            throw new IllegalStateException(
                    "historyRetention is not supported when not a versioned store");
        }
        return ((VersionedBytesStoreSupplier) materialized.storeSupplier()).historyRetentionMs();
    }

    @Override
    public boolean isWindowStore() {
        return false;
    }

    @Override
    public boolean isVersionedStore() {
        return materialized.storeSupplier() instanceof VersionedBytesStoreSupplier;
    }

}