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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * A wrapper class for non-windowed key-value stores used within the DSL. All such stores are
 * instances of either {@link TimestampedKeyValueStore} or {@link VersionedKeyValueStore}.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class KeyValueStoreWrapper<K, V> implements StateStore {

    private final StateStore store;
    private final StoreType storeType;

    private enum StoreType {
        TIMESTAMPED,
        VERSIONED;
    }

    public KeyValueStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        store = context.getStateStore(storeName);

        if (store instanceof TimestampedKeyValueStore) {
            storeType = StoreType.TIMESTAMPED;
        } else if (store instanceof VersionedKeyValueStore) {
            storeType = StoreType.VERSIONED;
        } else {
            throw new InvalidStateStoreException("KTable source state store must implement either "
                + "TimestampedKeyValueStore or VersionedKeyValueStore. Got: " + store.getClass().getName());
        }
    }

    public ValueAndTimestamp<V> get(final K key) {
        switch (storeType) {
            case TIMESTAMPED:
                return getTimestampedStore().get(key);
            case VERSIONED:
                final VersionedRecord<V> versionedRecord = getVersionedStore().get(key);
                return versionedRecord == null
                    ? null
                    : ValueAndTimestamp.make(versionedRecord.value(), versionedRecord.timestamp());
            default:
                throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
        }
    }

    public void put(final K key, final V value, final long timestamp) {
        switch (storeType) {
            case TIMESTAMPED:
                getTimestampedStore().put(key, ValueAndTimestamp.make(value, timestamp));
                return;
            case VERSIONED:
                getVersionedStore().put(key, value, timestamp);
                return;
            default:
                throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
        }
    }

    public StateStore getStore() {
        return store;
    }

    @Override
    public String name() {
        return store.name();
    }

    @Deprecated
    @Override
    public void init(final org.apache.kafka.streams.processor.ProcessorContext context, final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void flush() {
        store.flush();
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return store.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    @SuppressWarnings("unchecked")
    private TimestampedKeyValueStore<K, V> getTimestampedStore() {
        return (TimestampedKeyValueStore<K, V>) store;
    }

    @SuppressWarnings("unchecked")
    private VersionedKeyValueStore<K, V> getVersionedStore() {
        return (VersionedKeyValueStore<K, V>) store;
    }
}
