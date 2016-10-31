/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
 * A wrapper over all of the {@link StateStoreProvider}s in a Topology
 */
public class QueryableStoreProvider {

    private final List<StateStoreProvider> storeProviders;

    public QueryableStoreProvider(final List<StateStoreProvider> storeProviders) {
        this.storeProviders = new ArrayList<>(storeProviders);
    }

    /**
     * Get a composite object wrapping the instances of the {@link StateStore} with the provided
     * storeName and {@link QueryableStoreType}
     * @param storeName             name of the store
     * @param queryableStoreType    accept stores passing {@link QueryableStoreType#accepts(StateStore)}
     * @param <T>                   The expected type of the returned store
     * @return A composite object that wraps the store instances.
     */
    public <T> T getStore(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        final List<T> allStores = new ArrayList<>();
        for (StateStoreProvider storeProvider : storeProviders) {
            allStores.addAll(storeProvider.stores(storeName, queryableStoreType));
        }
        if (allStores.isEmpty()) {
            throw new InvalidStateStoreException("the state store, " + storeName + ", may have migrated to another instance.");
        }
        return queryableStoreType.create(
                new WrappingStoreProvider(storeProviders),
                storeName);
    }
}
