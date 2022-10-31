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

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper over all of the {@link StateStoreProvider}s in a Topology
 *
 * The store providers field is a reference
 */
public class QueryableStoreProvider {

    // map of StreamThread.name to StreamThreadStateStoreProvider
    private final Map<String, StreamThreadStateStoreProvider> storeProviders;
    private final GlobalStateStoreProvider globalStoreProvider;

    public QueryableStoreProvider(final GlobalStateStoreProvider globalStateStoreProvider) {
        this.storeProviders = new HashMap<>();
        this.globalStoreProvider = globalStateStoreProvider;
    }

    /**
     * Get a composite object wrapping the instances of the {@link StateStore} with the provided
     * storeName and {@link QueryableStoreType}
     *
     * @param storeQueryParameters       if stateStoresEnabled is used i.e. staleStoresEnabled is true, include standbys and recovering stores;
     *                                        if stateStoresDisabled i.e. staleStoresEnabled is false, only include running actives;
     *                                        if partition is null then it fetches all local partitions on the instance;
     *                                        if partition is set then it fetches a specific partition.
     * @param <T>                The expected type of the returned store
     * @return A composite object that wraps the store instances.
     */
    public <T> T getStore(final StoreQueryParameters<T> storeQueryParameters) {
        final String storeName = storeQueryParameters.storeName();
        final QueryableStoreType<T> queryableStoreType = storeQueryParameters.queryableStoreType();
        final List<T> globalStore = globalStoreProvider.stores(storeName, queryableStoreType);
        if (!globalStore.isEmpty()) {
            return queryableStoreType.create(globalStoreProvider, storeName);
        }
        return queryableStoreType.create(
            new WrappingStoreProvider(storeProviders.values(), storeQueryParameters),
            storeName
        );
    }

    public void addStoreProviderForThread(final String threadName, final StreamThreadStateStoreProvider streamThreadStateStoreProvider) {
        this.storeProviders.put(threadName, streamThreadStateStoreProvider);
    }

    public void removeStoreProviderForThread(final String threadName) {
        this.storeProviders.remove(threadName);
    }
}
