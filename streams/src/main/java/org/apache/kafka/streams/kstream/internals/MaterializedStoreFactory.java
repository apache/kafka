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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory;

/**
 * {@code MaterializedStoreFactory} is the base class for any {@link StoreFactory} that
 * wraps a {@link MaterializedInternal} instance.
 */
public abstract class MaterializedStoreFactory<K, V, S extends StateStore> implements StoreFactory {
    protected final MaterializedInternal<K, V, S> materialized;
    private final Set<String> connectedProcessorNames = new HashSet<>();
    protected Materialized.StoreType defaultStoreType
            = MaterializedInternal.parse(StreamsConfig.DEFAULT_DSL_STORE);

    public MaterializedStoreFactory(final MaterializedInternal<K, V, S> materialized) {
        this.materialized = materialized;

        // this condition will never be false; in the next PR we will
        // remove the initialization of storeType from MaterializedInternal
        if (materialized.storeType() != null) {
            defaultStoreType = materialized.storeType;
        }
    }

    @Override
    public void configure(final StreamsConfig config) {
        // in a follow-up PR, this will set the defaultStoreType to the configured value
    }

    @Override
    public Set<String> connectedProcessorNames() {
        return connectedProcessorNames;
    }

    @Override
    public boolean loggingEnabled() {
        return materialized.loggingEnabled();
    }

    @Override
    public String name() {
        return materialized.storeName();
    }

    @Override
    public Map<String, String> logConfig() {
        return materialized.logConfig();
    }

    @Override
    public StoreFactory withCachingDisabled() {
        materialized.withCachingDisabled();
        return this;
    }

    @Override
    public StoreFactory withLoggingDisabled() {
        materialized.withLoggingDisabled();
        return this;
    }

    @Override
    public boolean isCompatibleWith(final StoreFactory storeFactory) {
        return (storeFactory instanceof MaterializedStoreFactory)
                && ((MaterializedStoreFactory<?, ?, ?>) storeFactory).materialized.equals(materialized);
    }
}
