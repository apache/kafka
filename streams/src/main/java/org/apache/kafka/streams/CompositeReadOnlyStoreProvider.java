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
package org.apache.kafka.streams;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.ReadOnlyStoreProvider;

import java.util.ArrayList;
import java.util.List;

class CompositeReadOnlyStoreProvider {

    private final List<ReadOnlyStoreProvider> storeProviders;

    CompositeReadOnlyStoreProvider(final List<ReadOnlyStoreProvider> storeProviders) {
        this.storeProviders = new ArrayList<>(storeProviders);
    }

    <K, V> ReadOnlyKeyValueStore<K, V> getStore(final String name) {
        final List<ReadOnlyKeyValueStore<K, V>> allStores = new ArrayList<>();
        for (ReadOnlyStoreProvider storeProvider : storeProviders) {
            final List<ReadOnlyKeyValueStore<K, V>> stores = storeProvider.getStores(name);
            allStores.addAll(stores);
        }
        if (allStores.isEmpty()) {
            return null;
        }

        return new CompositeReadOnlyStore<>(storeProviders, name);
    }

    <K, V> ReadOnlyWindowStore<K, V> getWindowedStore(final String name) {
        final List<ReadOnlyWindowStore<K, V>> allStores = new ArrayList<>();
        for (ReadOnlyStoreProvider storeProvider : storeProviders) {
            final List<ReadOnlyWindowStore<K, V>>
                windowStores =
                storeProvider.getWindowStores(name);
            allStores.addAll(windowStores);
        }

        if (allStores.isEmpty()) {
            return null;
        }

        return new CompositeReadOnlyWindowStore<>(storeProviders, name);
    }


}
