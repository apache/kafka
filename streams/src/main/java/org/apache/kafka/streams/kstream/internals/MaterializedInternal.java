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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.StoreSupplier;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public final class MaterializedInternal<K, V, S extends StateStore> extends Materialized<K, V, S> {

    private final boolean queryable;

    public MaterializedInternal(final Materialized<K, V, S> materialized) {
        this(materialized, null, null);
    }

    public MaterializedInternal(final Materialized<K, V, S> materialized,
                                final InternalNameProvider nameProvider,
                                final String generatedStorePrefix) {
        this(materialized, nameProvider, generatedStorePrefix, false);
    }

    public MaterializedInternal(final Materialized<K, V, S> materialized,
                                final InternalNameProvider nameProvider,
                                final String generatedStorePrefix,
                                final boolean forceQueryable) {
        super(materialized);

        // if storeName is not provided, the corresponding KTable would never be queryable;
        // but we still need to provide an internal name for it in case we materialize.
        queryable = forceQueryable || storeName() != null;
        if (storeName() == null && nameProvider != null) {
            storeName = nameProvider.newStoreName(generatedStorePrefix);
        }

        // if store type is not configured during creating Materialized, then try to get the topologyConfigs from nameProvider
        // otherwise, leave it as null so that it resolves when the KafkaStreams application
        // is configured with the main StreamsConfig
        if (dslStoreSuppliers == null) {
            if (nameProvider instanceof InternalStreamsBuilder) {
                final TopologyConfig topologyConfig = ((InternalStreamsBuilder) nameProvider).internalTopologyBuilder.topologyConfigs();
                if (topologyConfig != null) {
                    dslStoreSuppliers = topologyConfig.resolveDslStoreSuppliers().orElse(null);
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static StoreType parse(final String storeType) {
        switch (storeType) {
            case StreamsConfig.IN_MEMORY:
                return StoreType.IN_MEMORY;
            case StreamsConfig.ROCKS_DB:
                return StoreType.ROCKS_DB;
            default:
                throw new IllegalStateException("Unexpected storeType: " + storeType);
        }
    }

    public String queryableStoreName() {
        return queryable ? storeName() : null;
    }

    public String storeName() {
        if (storeSupplier != null) {
            return storeSupplier.name();
        }
        return storeName;
    }

    public Optional<DslStoreSuppliers> dslStoreSuppliers() {
        return Optional.ofNullable(dslStoreSuppliers);
    }

    public StoreSupplier<S> storeSupplier() {
        return storeSupplier;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    public Map<String, String> logConfig() {
        return topicConfig;
    }

    public boolean cachingEnabled() {
        return cachingEnabled;
    }

    Duration retention() {
        return retention;
    }
}
