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
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

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
            throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                    streamThread.state() + ", not RUNNING");
        }
        final List<T> stores = new ArrayList<>();
        for (final Task streamTask : streamThread.tasks().values()) {
            final StateStore store = streamTask.getStore(storeName);
            if (store != null && queryableStoreType.accepts(store)) {
                if (!store.isOpen()) {
                    throw new InvalidStateStoreException("Cannot get state store " + storeName + " for task " + streamTask +
                            " because the store is not open. The state store may have migrated to another instances.");
                }
                if (store instanceof KeyValueWithTimestampStoreMaterializer.KeyValueStoreFacade) {
                    final KeyValueStore innerStore = ((KeyValueWithTimestampStoreMaterializer.KeyValueStoreFacade) store).inner;
                    if (queryableStoreType instanceof QueryableStoreTypes.KeyValueStoreType) {
                        stores.add((T) new ReadOnlyKeyValueStoreFacade(innerStore));
                    } else {
                        stores.add((T) innerStore);
                    }
                } else if (store instanceof KStreamImpl.WindowStoreFacade) {
                    final WindowStore innerStore = ((KStreamImpl.WindowStoreFacade) store).inner;
                    if (queryableStoreType instanceof QueryableStoreTypes.WindowStoreType) {
                        stores.add((T) new ReadOnlyWindowStoreFacade(innerStore));
                    } else {
                        stores.add((T) innerStore);
                    }
                } else if (store instanceof SessionWindowedKStreamImpl.SessionStoreFacade) {
                    final SessionStore innerStore = ((SessionWindowedKStreamImpl.SessionStoreFacade) store).inner;
                    if (queryableStoreType instanceof QueryableStoreTypes.SessionStoreType) {
                        stores.add((T) new ReadOnlySessionStoreFacade(innerStore));
                    } else {
                        stores.add((T) innerStore);
                    }
                } else {
                    stores.add((T) store);
                }
            }
        }
        return stores;
    }

}
