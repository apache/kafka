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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper over StreamThread that implements StateStoreProvider
 */
public class StreamThreadStateStoreProvider implements StateStoreProvider {

    private final StreamThread streamThread;

    public StreamThreadStateStoreProvider(final StreamThread streamThread) {
        this.streamThread = streamThread;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> stores(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        if (streamThread.state() == StreamThread.State.DEAD) {
            return Collections.emptyList();
        }
        if (!streamThread.isRunningAndNotRebalancing()) {
            throw new InvalidStateStoreException("the state store, " + storeName + ", may have migrated to another instance.");
        }
        final List<T> stores = new ArrayList<>();
        for (Task streamTask : streamThread.tasks().values()) {
            final StateStore store = streamTask.getStore(storeName);
            if (store != null && queryableStoreType.accepts(store)) {
                if (!store.isOpen()) {
                    throw new InvalidStateStoreException("the state store, " + storeName + ", may have migrated to another instance.");
                }
                stores.add((T) store);
            }
        }
        return stores;
    }

}
