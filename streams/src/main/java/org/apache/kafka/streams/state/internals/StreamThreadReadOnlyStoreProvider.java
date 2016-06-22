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
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper over StreamThread that implements ReadOnlyStoreProvider
 */
public class StreamThreadReadOnlyStoreProvider implements ReadOnlyStoreProvider {

    private final StreamThread streamThread;

    public StreamThreadReadOnlyStoreProvider(final StreamThread streamThread) {
        this.streamThread = streamThread;
    }

    @Override
    public <K, V> List<ReadOnlyKeyValueStore<K, V>> getStores(final String storeName) {
        final List<ReadOnlyKeyValueStore<K, V>> stores = new ArrayList<>();
        for (StreamTask task : streamThread.tasks().values()) {
            final StateStore store = task.getStore(storeName);
            if (store instanceof ReadOnlyKeyValueStore) {
                if (!store.isOpen()) {
                    throw new InvalidStoreException("Store: " + storeName + " isn't open");
                }
                stores.add((ReadOnlyKeyValueStore<K, V>) store);
            }
        }
        return stores;
    }

    @Override
    public <K, V> List<ReadOnlyWindowStore<K, V>> getWindowStores(final String storeName) {
        final List<ReadOnlyWindowStore<K, V>> stores = new ArrayList<>();
        for (StreamTask streamTask : streamThread.tasks().values()) {
            final StateStore store = streamTask.getStore(storeName);
            if (store instanceof  ReadOnlyWindowStore) {
                if (!store.isOpen()) {
                    throw new InvalidStoreException("Store: " + storeName + " isn't open");
                }
                stores.add((ReadOnlyWindowStore<K, V>) store);
            }
        }
        return stores;
    }
}
