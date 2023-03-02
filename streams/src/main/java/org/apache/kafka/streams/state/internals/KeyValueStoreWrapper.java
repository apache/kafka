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

    private TimestampedKeyValueStore<K, V> timestampedStore = null;
    private VersionedKeyValueStore<K, V> versionedStore = null;

    // same as either timestampedStore or versionedStore above. kept merely as a convenience
    // to simplify implementation for methods which do not depend on store type.
    private StateStore store = null;

    public KeyValueStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        try {
            // first try timestamped store
            timestampedStore = context.getStateStore(storeName);
            store = timestampedStore;
            return;
        } catch (final ClassCastException e) {
            // ignore since could be versioned store instead
        }

        try {
            // next try versioned store
            versionedStore = context.getStateStore(storeName);
            store = versionedStore;
        } catch (final ClassCastException e) {
            store = context.getStateStore(storeName);
            final String storeType = store == null ? "null" : store.getClass().getName();
            throw new InvalidStateStoreException("KTable source state store must implement either "
                + "TimestampedKeyValueStore or VersionedKeyValueStore. Got: " + storeType);
        }
    }

    public ValueAndTimestamp<V> get(final K key) {
        if (timestampedStore != null) {
            return timestampedStore.get(key);
        }
        if (versionedStore != null) {
            final VersionedRecord<V> versionedRecord = versionedStore.get(key);
            return versionedRecord == null
                ? null
                : ValueAndTimestamp.make(versionedRecord.value(), versionedRecord.timestamp());
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    public void put(final K key, final V value, final long timestamp) {
        if (timestampedStore != null) {
            timestampedStore.put(key, ValueAndTimestamp.make(value, timestamp));
            return;
        }
        if (versionedStore != null) {
            versionedStore.put(key, value, timestamp);
            return;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
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
}
