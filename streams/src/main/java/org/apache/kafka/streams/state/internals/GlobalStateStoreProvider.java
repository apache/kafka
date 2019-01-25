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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KeyValueWithTimestampStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.SessionWindowedKStreamImpl;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GlobalStateStoreProvider implements StateStoreProvider {
    private final Map<String, StateStore> globalStateStores;

    public GlobalStateStoreProvider(final Map<String, StateStore> globalStateStores) {
        this.globalStateStores = globalStateStores;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> stores(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        final StateStore store = globalStateStores.get(storeName);
        if (store == null || !queryableStoreType.accepts(store)) {
            return Collections.emptyList();
        }
        if (!store.isOpen()) {
            throw new InvalidStateStoreException("the state store, " + storeName + ", is not open.");
        }

        if (store instanceof KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade) {
            final KeyValueStore innerStore = ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade) store).inner;
            if (queryableStoreType instanceof QueryableStoreTypes.KeyValueStoreType) {
                return (List<T>) Collections.singletonList(new ReadOnlyKeyValueStoreFacade(innerStore));
            }
            return (List<T>) Collections.singletonList(innerStore);
        }
        if (store instanceof KStreamImpl.WindowStoreFacade) {
            final WindowStore innerStore = ((KStreamImpl.WindowStoreFacade) store).inner;
            if (queryableStoreType instanceof QueryableStoreTypes.WindowStoreType) {
                return (List<T>) Collections.singletonList(new ReadOnlyWindowStoreFacade(innerStore));
            }
            return (List<T>) Collections.singletonList(innerStore);
        }
        if (store instanceof SessionWindowedKStreamImpl.SessionStoreFacade) {
            final SessionStore innerStore = ((SessionWindowedKStreamImpl.SessionStoreFacade) store).inner;
            if (queryableStoreType instanceof QueryableStoreTypes.SessionStoreType) {
                return (List<T>) Collections.singletonList(new ReadOnlySessionStoreFacade(innerStore));
            }
            return (List<T>) Collections.singletonList(innerStore);
        }
        return (List<T>) Collections.singletonList(store);
    }
}
