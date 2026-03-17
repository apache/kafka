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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

import java.util.Map;

/**
 * A wrapper class for non-windowed key-value stores used within the DSL. All such stores are
 * instances of either {@link TimestampedKeyValueStore}, {@link TimestampedKeyValueStoreWithHeaders},
 * or {@link VersionedKeyValueStore}.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class KeyValueStoreWrapper<K, V> implements StateStore {

    public static final long PUT_RETURN_CODE_IS_LATEST
        = VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;

    private TimestampedKeyValueStoreWithHeaders<K, V> headersStore = null;
    private VersionedKeyValueStore<K, V> versionedStore = null;

    // same as either timestampedStore or versionedStore above. kept merely as a convenience
    // to simplify implementation for methods which do not depend on store type.
    private StateStore store;

    @SuppressWarnings("unchecked")
    public KeyValueStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        final StateStore rawStore = context.getStateStore(storeName);

        // Try headers-aware timestamped store
        try {
            headersStore = (TimestampedKeyValueStoreWithHeaders<K, V>) rawStore;
            store = headersStore;
            return;
        } catch (final ClassCastException e) {
            // not headers store, try versioned
        }

        // Try versioned store
        try {
            versionedStore = (VersionedKeyValueStore<K, V>) rawStore;
            store = versionedStore;
        } catch (final ClassCastException e) {
            final String storeType = rawStore == null ? "null" : rawStore.getClass().getName();
            throw new InvalidStateStoreException("KTable source state store must implement either "
                + "TimestampedKeyValueStore, TimestampedKeyValueStoreWithHeaders, or VersionedKeyValueStore. Got: " + storeType);
        }
    }

    public ValueTimestampHeaders<V> get(final K key) {
        if (headersStore != null) {
            return headersStore.get(key);
        }
        if (versionedStore != null) {
            final VersionedRecord<V> versionedRecord = versionedStore.get(key);
            return versionedRecord == null
                ? null
                : ValueTimestampHeaders.make(versionedRecord.value(), versionedRecord.timestamp(), null);
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped, headers, or versioned store");
    }

    public ValueTimestampHeaders<V> get(final K key, final long asOfTimestamp) {
        if (!isVersionedStore()) {
            throw new UnsupportedOperationException("get(key, timestamp) is only supported for versioned stores");
        }
        final VersionedRecord<V> versionedRecord = versionedStore.get(key, asOfTimestamp);
        return versionedRecord == null ? null : ValueTimestampHeaders.make(versionedRecord.value(), versionedRecord.timestamp(), new RecordHeaders());
    }

    /**
     * @return {@code -1} if the put record is the latest for its key, and {@code Long.MIN_VALUE}
     *         if the put was rejected (i.e., due to grace period having elapsed for a versioned
     *         store). If neither, any other long value may be returned.
     */
    public long put(final K key, final V value, final long timestamp, final Headers headers) {
        if (headersStore != null) {
            headersStore.put(key, ValueTimestampHeaders.make(value, timestamp, headers));
            return PUT_RETURN_CODE_IS_LATEST;
        }
        if (versionedStore != null) {
            return versionedStore.put(key, value, timestamp);
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped, headers, or versioned store");
    }

    public StateStore store() {
        return store;
    }

    public boolean isVersionedStore() {
        return versionedStore != null;
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        store.init(stateStoreContext, root);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        store.commit(changelogOffsets);
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
