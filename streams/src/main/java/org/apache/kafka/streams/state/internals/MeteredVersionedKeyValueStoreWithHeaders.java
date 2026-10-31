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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedRecord;

class MeteredVersionedKeyValueStoreWithHeaders<K, V>
    extends MeteredVersionedKeyValueStore<K, V>
    implements VersionedKeyValueStoreWithHeaders<K, V> {

    MeteredVersionedKeyValueStoreWithHeaders(final VersionedBytesStore inner,
                                             final String metricScope,
                                             final Time time,
                                             final Serde<K> keySerde,
                                             final Serde<V> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }

    @Override
    public long put(final K key, final V value, final long timestamp, final Headers headers) {
        return putWithHeaders(key, value, timestamp, headers);
    }

    @Override
    public VersionedRecord<V> delete(final K key, final long timestamp) {
        return deleteWithHeaders(key, timestamp);
    }

    @Override
    public VersionedRecord<V> get(final K key) {
        return getWithHeaders(key);
    }

    @Override
    public VersionedRecord<V> get(final K key, final long asOfTimestamp) {
        return getWithHeaders(key, asOfTimestamp);
    }

    @Override
    public VersionedKeyValueStoreWithHeaders<K, V> readOnly(final IsolationLevel isolationLevel) {
        return new ReadOnlyView(wrapped().readOnly(isolationLevel));
    }

    private final class ReadOnlyView implements VersionedKeyValueStoreWithHeaders<K, V> {
        private final VersionedBytesStore underlying;

        private ReadOnlyView(final VersionedBytesStore underlying) {
            this.underlying = underlying;
        }

        @Override
        public VersionedRecord<V> get(final K key) {
            return getWithHeaders(underlying, key);
        }

        @Override
        public VersionedRecord<V> get(final K key, final long asOfTimestamp) {
            return getWithHeaders(underlying, key, asOfTimestamp);
        }

        @Override
        public long put(final K key, final V value, final long timestamp) {
            throw new UnsupportedOperationException("put is not supported on a read-only view");
        }

        @Override
        public long put(final K key, final V value, final long timestamp, final Headers headers) {
            throw new UnsupportedOperationException("put is not supported on a read-only view");
        }

        @Override
        public VersionedRecord<V> delete(final K key, final long timestamp) {
            throw new UnsupportedOperationException("delete is not supported on a read-only view");
        }

        @Override
        public String name() {
            return MeteredVersionedKeyValueStoreWithHeaders.this.name();
        }

        @Override
        public void init(final StateStoreContext stateStoreContext, final StateStore root) {
            throw new UnsupportedOperationException("init is not supported on a read-only view");
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("close is not supported on a read-only view");
        }

        @Override
        public boolean persistent() {
            return MeteredVersionedKeyValueStoreWithHeaders.this.persistent();
        }

        @Override
        public boolean isOpen() {
            return MeteredVersionedKeyValueStoreWithHeaders.this.isOpen();
        }
    }
}
