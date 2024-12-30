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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore} and {@link KeyValueStore}.
 * <p>
 * If a user provides a supplier for plain {@code KeyValueStores} via
 * {@link org.apache.kafka.streams.kstream.Materialized#as(KeyValueBytesStoreSupplier)} this adapter is used to
 * translate between old a new {@code byte[]} format of the value.
 *
 * @see KeyValueToTimestampedKeyValueIteratorAdapter
 */

@SuppressWarnings("unchecked")
public class KeyValueToTimestampedKeyValueByteStoreAdapter implements KeyValueStore<Bytes, byte[]> {
    final KeyValueStore<Bytes, byte[]> store;

    KeyValueToTimestampedKeyValueByteStoreAdapter(final KeyValueStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp) {
        store.put(key, rawValue(valueWithTimestamp));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueWithTimestamp) {
        return convertToTimestampedFormat(store.putIfAbsent(
            key,
            rawValue(valueWithTimestamp)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestamp = entry.value;
            store.put(entry.key, rawValue(valueWithTimestamp));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        return convertToTimestampedFormat(store.delete(key));
    }

    @Override
    public String name() {
        return store.name();
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
        return true;
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {



        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        QueryResult<R> result = store.query(query, positionBound, config);

        // this adapter always needs to return a `value-with-timestamp` result to hold up its contract
        // thus, we need to add the dummy `-1` timestamp even for `KeyQuery` and `RangeQuery`
        if (result.isSuccess()) {
            if (query instanceof KeyQuery || query instanceof TimestampedKeyQuery) {
                final byte[] plainValue = (byte[]) result.getResult();
                final byte[] valueWithTimestamp = convertToTimestampedFormat(plainValue);
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(result, valueWithTimestamp);
            } else if (query instanceof RangeQuery || query instanceof TimestampedRangeQuery) {
                final KeyValueToTimestampedKeyValueAdapterIterator wrappedRocksDBRangeIterator = new KeyValueToTimestampedKeyValueAdapterIterator((RocksDbIterator) result.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(result, wrappedRocksDBRangeIterator);
            } else {
                throw new IllegalArgumentException("Unsupported query type: " + query.getClass());
            }
        }

        if (config.isCollectExecutionInfo()) {

            final long end = System.nanoTime();
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (end - start) + "ns"
            );
        }
        return result;
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    @Override
    public byte[] get(final Bytes key) {
        return convertToTimestampedFormat(store.get(key));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                        final Bytes to) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }

    private static class KeyValueToTimestampedKeyValueAdapterIterator implements ManagedKeyValueIterator<Bytes, byte[]> {

        private final RocksDbIterator rocksDbIterator;

        public KeyValueToTimestampedKeyValueAdapterIterator(final RocksDbIterator rocksDbIterator) {
            this.rocksDbIterator = rocksDbIterator;
        }

        @Override
        public void close() {
            rocksDbIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            return rocksDbIterator.peekNextKey();
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            rocksDbIterator.onClose(closeCallback);
        }

        @Override
        public boolean hasNext() {
            return rocksDbIterator.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = rocksDbIterator.next();
            if (next == null) {
                return null;
            }
            return KeyValue.pair(next.key, convertToTimestampedFormat(next.value));
        }
    }
}