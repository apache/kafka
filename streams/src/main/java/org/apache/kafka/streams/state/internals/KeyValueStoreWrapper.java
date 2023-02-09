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

    public KeyValueStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        try {
            // first try timestamped store
            timestampedStore = context.getStateStore(storeName);
            return;
        } catch (final ClassCastException e) {
            // ignore since could be versioned store instead
        }

        try {
            // next try versioned store
            versionedStore = context.getStateStore(storeName);
        } catch (final ClassCastException e) {
            throw new InvalidStateStoreException("KTable source state store must implement either TimestampedKeyValueStore or VersionedKeyValueStore.");
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
        if (timestampedStore != null) {
            return timestampedStore;
        }
        if (versionedStore != null) {
            return versionedStore;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public String name() {
        if (timestampedStore != null) {
            return timestampedStore.name();
        }
        if (versionedStore != null) {
            return versionedStore.name();
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Deprecated
    @Override
    public void init(org.apache.kafka.streams.processor.ProcessorContext context, StateStore root) {
        if (timestampedStore != null) {
            timestampedStore.init(context, root);
            return;
        }
        if (versionedStore != null) {
            versionedStore.init(context, root);
            return;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        if (timestampedStore != null) {
            timestampedStore.init(context, root);
            return;
        }
        if (versionedStore != null) {
            versionedStore.init(context, root);
            return;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public void flush() {
        if (timestampedStore != null) {
            timestampedStore.flush();
            return;
        }
        if (versionedStore != null) {
            versionedStore.flush();
            return;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public void close() {
        if (timestampedStore != null) {
            timestampedStore.close();
            return;
        }
        if (versionedStore != null) {
            versionedStore.close();
            return;
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public boolean persistent() {
        if (timestampedStore != null) {
            return timestampedStore.persistent();
        }
        if (versionedStore != null) {
            return versionedStore.persistent();
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public boolean isOpen() {
        if (timestampedStore != null) {
            return timestampedStore.isOpen();
        }
        if (versionedStore != null) {
            return versionedStore.isOpen();
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        if (timestampedStore != null) {
            return timestampedStore.query(query, positionBound, config);
        }
        if (versionedStore != null) {
            return versionedStore.query(query, positionBound, config);
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }

    @Override
    public Position getPosition() {
        if (timestampedStore != null) {
            return timestampedStore.getPosition();
        }
        if (versionedStore != null) {
            return versionedStore.getPosition();
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped or versioned store");
    }
}
