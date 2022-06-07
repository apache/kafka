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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Objects;

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
public class KeyValueToTimestampedKeyValueByteStoreAdapter implements KeyValueStore<Bytes, byte[]> {
    final KeyValueStore<Bytes, byte[]> store;
    private static final Logger log = LoggerFactory.getLogger(KeyValueToTimestampedKeyValueByteStoreAdapter.class);


    KeyValueToTimestampedKeyValueByteStoreAdapter(final KeyValueStore<Bytes, byte[]> store) {
        log.error("SOPHIE: creating store adapter with inner store={}", store.getClass());
        System.out.println("SOPHIE: creating store adapter with inner store=" + store.getClass());
        System.out.println("SOPHIE: store class is " + store.getClass());
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp) {
        log.error("SOPHIE: calling put on key={}, value={}", key, valueWithTimestamp == null ? "null" : valueWithTimestamp);
        System.out.println("SOPHIE: calling put on key=" + key + ", value bytes null?=" + valueWithTimestamp.length);
        System.out.println("SOPHIE: store class is " + store.getClass());

        store.put(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueWithTimestamp) {
        log.error("SOPHIE: calling putIfAbsent on key={}, value={}", key, valueWithTimestamp == null ? "null" : valueWithTimestamp);
        System.out.println("SOPHIE: calling put on key=" + key + ", value bytes null?=" + valueWithTimestamp.length);
        System.out.println("SOPHIE: store class is " + store.getClass());
        return convertToTimestampedFormat(store.putIfAbsent(
            key,
            valueWithTimestamp == null ? null : rawValue(valueWithTimestamp)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        log.error("SOPHIE: calling putAll with entries={}", entries);
        System.out.println("SOPHIE: calling putAll with entries="+ entries);
        System.out.println("SOPHIE: store class is " + store.getClass());
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestamp = entry.value;
            store.put(entry.key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        log.error("SOPHIE: attempting to delete key={}", key);
        System.out.println("SOPHIE: attempting to delete key={}" + key);
        return convertToTimestampedFormat(store.delete(key));
    }

    @Override
    public String name() {
        return store.name();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
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
        final QueryResult<R> result = store.query(query, positionBound, config);
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
        log.error("SOPHIE: calling get on key={}", key);
        System.out.println("SOPHIE: calling get on key=" + key);
        Objects.requireNonNull(key);
        return convertToTimestampedFormat(store.get(key));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        log.error("SOPHIE: calling range from={} to={}", from, to);

        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                        final Bytes to) {
        log.error("SOPHIE: calling reverseRange from={} to={}", from, to);

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

}