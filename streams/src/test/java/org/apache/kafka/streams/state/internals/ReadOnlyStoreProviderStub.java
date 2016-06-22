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

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadOnlyStoreProviderStub implements ReadOnlyStoreProvider {

    private final Map<String, ReadOnlyKeyValueStore> keyValueStores = new HashMap<>();
    private final Map<String, ReadOnlyWindowStore> windowStores = new HashMap<>();

    @Override
    public <K, V> List<ReadOnlyKeyValueStore<K, V>> getStores(final String storeName) {
        if (keyValueStores.containsKey(storeName)) {
            ReadOnlyKeyValueStore<K, V> store = keyValueStores.get(storeName);
            return Collections.singletonList(store);
        }
        return Collections.emptyList();
    }

    @Override
    public <K, V> List<ReadOnlyWindowStore<K, V>> getWindowStores(final String storeName) {
        if (windowStores.containsKey(storeName)) {
            ReadOnlyWindowStore<K, V> store = windowStores.get(storeName);
            return Collections.singletonList(store);
        }
        return Collections.emptyList();
    }

    public void addKeyValueStore(final String storeName,
                                 final ReadOnlyKeyValueStore store) {
        keyValueStores.put(storeName, store);
    }

    public void addWindowStore(final String storeName,
                               final ReadOnlyWindowStore store) {
        windowStores.put(storeName, store);
    }
}
