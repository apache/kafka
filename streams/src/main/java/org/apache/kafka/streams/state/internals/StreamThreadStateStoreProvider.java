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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StreamThreadStateStoreProvider {

    private final StreamThread streamThread;

    public StreamThreadStateStoreProvider(final StreamThread streamThread) {
        this.streamThread = streamThread;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> stores(final StoreQueryParameters storeQueryParams) {
        final StreamThread.State state = streamThread.state();
        if (state == StreamThread.State.DEAD) {
            return Collections.emptyList();
        }

        final String storeName = storeQueryParams.storeName();
        final QueryableStoreType<T> queryableStoreType = storeQueryParams.queryableStoreType();
        final String topologyName = storeQueryParams instanceof NamedTopologyStoreQueryParameters ?
            ((NamedTopologyStoreQueryParameters) storeQueryParams).topologyName() :
            null;

        if (storeQueryParams.staleStoresEnabled() ? state.isAlive() : state == StreamThread.State.RUNNING) {
            final Collection<Task> tasks = storeQueryParams.staleStoresEnabled() ?
                    streamThread.allTasks().values() : streamThread.activeTasks();

            if (storeQueryParams.partition() != null) {
                for (final Task task : tasks) {
                    if (task.id().partition() == storeQueryParams.partition() &&
                        (topologyName == null || topologyName.equals(task.id().topologyName())) &&
                        task.getStore(storeName) != null &&
                        storeName.equals(task.getStore(storeName).name())) {
                        final T typedStore = validateAndCastStores(task.getStore(storeName), queryableStoreType, storeName, task.id());
                        return Collections.singletonList(typedStore);
                    }
                }
                return Collections.emptyList();
            } else {
                final List<T> list = new ArrayList<>();
                for (final Task task : tasks) {
                    final StateStore store = task.getStore(storeName);
                    if (store == null) {
                        // then this task doesn't have that store
                    } else {
                        final T typedStore = validateAndCastStores(store, queryableStoreType, storeName, task.id());
                        list.add(typedStore);
                    }
                }
                return list;
            }
        } else {
            throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                                                    state + ", not RUNNING" +
                                                    (storeQueryParams.staleStoresEnabled() ? " or REBALANCING" : ""));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T validateAndCastStores(final StateStore store,
                                               final QueryableStoreType<T> queryableStoreType,
                                               final String storeName,
                                               final TaskId taskId) {
        if (store == null) {
            throw new NullPointerException("Expected store not to be null at this point.");
        } else if (queryableStoreType.accepts(store)) {
            if (!store.isOpen()) {
                throw new InvalidStateStoreException(
                        "Cannot get state store " + storeName + " for task " + taskId +
                            " because the store is not open. " +
                            "The state store may have migrated to another instance.");
            }
            if (store instanceof TimestampedKeyValueStore && queryableStoreType instanceof QueryableStoreTypes.KeyValueStoreType) {
                return (T) new ReadOnlyKeyValueStoreFacade<>((TimestampedKeyValueStore<Object, Object>) store);
            } else if (store instanceof TimestampedWindowStore && queryableStoreType instanceof QueryableStoreTypes.WindowStoreType) {
                return (T) new ReadOnlyWindowStoreFacade<>((TimestampedWindowStore<Object, Object>) store);
            } else {
                return (T) store;
            }
        } else {
            throw new InvalidStateStoreException(
                "Cannot get state store " + storeName +
                    " because the queryable store type [" + queryableStoreType.getClass() +
                    "] does not accept the actual store type [" + store.getClass() + "]."
            );
        }
    }
}
