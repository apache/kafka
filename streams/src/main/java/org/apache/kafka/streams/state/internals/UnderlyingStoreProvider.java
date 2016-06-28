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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StateStoreProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides access to {@link org.apache.kafka.streams.processor.StateStore}s accepted
 * by {@link QueryableStoreType#accepts(StateStore)}
 * @param <T>
 */
public class UnderlyingStoreProvider implements StateStoreProvider {

    private final List<StateStoreProvider> storeProviders;

    public UnderlyingStoreProvider(final List<StateStoreProvider> storeProviders) {
        this.storeProviders = storeProviders;
    }

    public <T> List<T> getStores(final String storeName, QueryableStoreType<T> type) {
        final List<T> allStores = new ArrayList<>();
        for (StateStoreProvider provider : storeProviders) {
            final List<T> stores =
                provider.getStores(storeName, type);
            allStores.addAll(stores);
        }
        if (allStores.isEmpty()) {
            throw new InvalidStateStoreException("Store " + storeName + " is currently "
                                                 + "unavailable");
        }
        return allStores;
    }
}
