/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a wrapper over multiple underlying {@link StateStoreProvider}s
 */
public class WrappingStoreProvider implements StateStoreProvider {

    private final List<StateStoreProvider> storeProviders;

    public WrappingStoreProvider(final List<StateStoreProvider> storeProviders) {
        this.storeProviders = storeProviders;
    }

    /**
     * Provides access to {@link org.apache.kafka.streams.processor.StateStore}s accepted
     * by {@link QueryableStoreType#accepts(StateStore)}
     * @param storeName  name of the store
     * @param type      The {@link QueryableStoreType}
     * @param <T>       The type of the Store, for example, {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore}
     * @return  a List of all the stores with the storeName and are accepted by {@link QueryableStoreType#accepts(StateStore)}
     */
    public <T> List<T> stores(final String storeName, QueryableStoreType<T> type) {
        final List<T> allStores = new ArrayList<>();
        for (StateStoreProvider provider : storeProviders) {
            final List<T> stores =
                provider.stores(storeName, type);
            allStores.addAll(stores);
        }
        if (allStores.isEmpty()) {
            throw new InvalidStateStoreException("the state store, " + storeName + ", may have migrated to another instance.");
        }
        return allStores;
    }
}
