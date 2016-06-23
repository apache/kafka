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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.state.internals.InvalidStateStoreException;
import org.apache.kafka.streams.state.internals.ReadOnlyStoresProvider;

import java.util.ArrayList;
import java.util.List;

public class UnderlyingStoreProvider<T> {

    private final List<ReadOnlyStoresProvider> storeProviders;
    private final QueryableStoreType<T> queryableStoreType;

    public UnderlyingStoreProvider(final List<ReadOnlyStoresProvider> storeProviders,
                                   final QueryableStoreType<T> queryableStoreType) {

        this.storeProviders = storeProviders;
        this.queryableStoreType = queryableStoreType;
    }

    public List<T> getStores(final String storeName) {
        final List<T> allStores = new ArrayList<>();
        for (ReadOnlyStoresProvider provider : storeProviders) {
            final List<T> stores =
                provider.getStores(storeName, queryableStoreType);
            allStores.addAll(stores);
        }
        if (allStores.isEmpty()) {
            throw new InvalidStateStoreException("Store " + storeName + " is currently "
                                                 + "unavailable");
        }
        return allStores;
    }
}
