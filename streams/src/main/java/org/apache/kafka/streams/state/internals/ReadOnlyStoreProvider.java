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

import java.util.List;

/**
 * A ReadOnlyStoreProvider provides access to {@link ReadOnlyKeyValueStore}s
 * and {@link ReadOnlyWindowStore}s available in a topology
 */
public interface ReadOnlyStoreProvider {

    /**
     * Get the instances of {@link ReadOnlyKeyValueStore} with the given storeName.
     * @param storeName  name of the store
     * @param <K>        key type of the store
     * @param <V>        value type of the store
     * @return  List of the instances of the store in this topology. Empty List if not found
     */
    <K, V> List<ReadOnlyKeyValueStore<K, V>> getStores(final String storeName);

    /**
     * Get the instances of {@link ReadOnlyWindowStore} with the given storeName
     * @param storeName     name of the store
     * @param <K>           key type of the store
     * @param <V>           value type of the store
     * @return List of the instances of the store in this topology. Empty List if not found
     */
    <K, V> List<ReadOnlyWindowStore<K, V>> getWindowStores(final String storeName);
}
