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
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;

import java.util.Collections;
import java.util.List;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class StreamThreadStateStoreProvider {

    private final StreamThread streamThread;

    public StreamThreadStateStoreProvider(final StreamThread streamThread) {
        this.streamThread = streamThread;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> stores(final StoreQueryParameters storeQueryParams) {
        final String storeName = storeQueryParams.storeName();
        final QueryableStoreType<T> queryableStoreType = storeQueryParams.queryableStoreType();
        if (streamThread.state() == StreamThread.State.DEAD) {
            return Collections.emptyList();
        }
        final StreamThread.State state = streamThread.state();
        if (storeQueryParams.staleStoresEnabled() ? state.isAlive() : state == StreamThread.State.RUNNING) {
            final Collection<Task> tasks = storeQueryParams.staleStoresEnabled() ?
                    streamThread.allTasks().values() : streamThread.activeTasks();

            if (storeQueryParams.partition() != null) {
                return findStreamTask(tasks, storeName, storeQueryParams.partition()).
                        map(streamTask ->
                                validateAndListStores(streamTask.getStore(storeName), queryableStoreType, storeName, streamTask.id())).
                        map(Collections::singletonList).
                        orElse(Collections.emptyList());
            } else {
                return tasks.stream().
                        map(streamTask ->
                                validateAndListStores(streamTask.getStore(storeName), queryableStoreType, storeName, streamTask.id())).
                        filter(Objects::nonNull).
                        collect(Collectors.toList());
            }
        } else {
            throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                                                    state + ", not RUNNING" +
                                                    (storeQueryParams.staleStoresEnabled() ? " or REBALANCING" : ""));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T validateAndListStores(final StateStore store, final QueryableStoreType<T> queryableStoreType, final String storeName, final TaskId taskId) {
        if (store != null && queryableStoreType.accepts(store)) {
            if (!store.isOpen()) {
                throw new InvalidStateStoreException(
                        "Cannot get state store " + storeName + " for task " + taskId +
                            " because the store is not open. " +
                            "The state store may have migrated to another instances.");
            }
            if (store instanceof TimestampedKeyValueStore && queryableStoreType instanceof QueryableStoreTypes.KeyValueStoreType) {
                return (T) new ReadOnlyKeyValueStoreFacade<>((TimestampedKeyValueStore<Object, Object>) store);
            } else if (store instanceof TimestampedWindowStore && queryableStoreType instanceof QueryableStoreTypes.WindowStoreType) {
                return (T) new ReadOnlyWindowStoreFacade<>((TimestampedWindowStore<Object, Object>) store);
            } else {
                return (T) store;
            }
        } else {
            return null;
        }
    }

    private Optional<Task> findStreamTask(final Collection<Task> tasks, final String storeName, final int partition) {
        return tasks.stream().
                filter(streamTask -> streamTask.id().partition == partition &&
                        streamTask.getStore(storeName) != null &&
                        storeName.equals(streamTask.getStore(storeName).name())).
                findFirst();
    }
}
