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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a wrapper over multiple underlying {@link StateStoreProvider}s
 */
public class WrappingStoreProvider implements StateStoreProvider {

    private final List<StreamThreadStateStoreProvider> storeProviders;
    private StoreQueryParameters storeQueryParameters;

    WrappingStoreProvider(final List<StreamThreadStateStoreProvider> storeProviders,
                          final StoreQueryParameters storeQueryParameters) {
        this.storeProviders = storeProviders;
        this.storeQueryParameters = storeQueryParameters;
    }

    //visible for testing
    public void setStoreQueryParameters(final StoreQueryParameters storeQueryParameters) {
        this.storeQueryParameters = storeQueryParameters;
    }

    @Override
    public <T> List<T> stores(final String storeName,
                              final QueryableStoreType<T> queryableStoreType) {
        final List<T> allStores = new ArrayList<>();
        for (final StreamThreadStateStoreProvider storeProvider : storeProviders) {
            final List<T> stores = storeProvider.stores(storeQueryParameters);
            if (!stores.isEmpty()) {
                allStores.addAll(stores);
                if (storeQueryParameters.partition() != null) {
                    break;
                }
            }
        }
        if (allStores.isEmpty()) {
            if (storeQueryParameters.partition() != null) {
                throw new InvalidStateStoreException(
                        String.format("The specified partition %d for store %s does not exist.",
                                storeQueryParameters.partition(),
                                storeName));
            }
            throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
        }
        return allStores;
    }
}
