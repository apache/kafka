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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateStoreProviderStub implements StateStoreProvider {

    private final Map<String, StateStore> stores = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> getStores(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        if (stores.containsKey(storeName) && queryableStoreType.accepts(stores.get(storeName))) {
            return (List<T>) Collections.singletonList(stores.get(storeName));
        }
        return Collections.emptyList();
    }

    public void addStore(final String storeName,
                         final StateStore store) {
        stores.put(storeName, store);
    }

}
