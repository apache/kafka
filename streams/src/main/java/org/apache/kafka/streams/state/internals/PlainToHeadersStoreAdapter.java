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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertFromPlainToHeaderFormat;
import static org.apache.kafka.streams.state.internals.Utils.rawPlainValue;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link TimestampedKeyValueStoreWithHeaders} and plain {@link KeyValueStore}.
 * <p>
 * If a user provides a supplier for plain {@code KeyValueStore} (without timestamp or headers) via
 * {@link Materialized#as(KeyValueBytesStoreSupplier)} when building
 * a {@code TimestampedKeyValueStoreWithHeaders}, this adapter is used to translate between
 * the plain {@code byte[]} format and the timestamped-with-headers {@code byte[]} format.
 *
 * @see PlainToHeadersIteratorAdapter
 */
@SuppressWarnings("unchecked")
public class PlainToHeadersStoreAdapter implements KeyValueStore<Bytes, byte[]> {
    final KeyValueStore<Bytes, byte[]> store;

    PlainToHeadersStoreAdapter(final KeyValueStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        if (store instanceof TimestampedBytesStore) {
            throw new IllegalArgumentException("Provided store must be a plain (non-timestamped) key value store, but it is timestamped.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestampAndHeaders) {
        store.put(key, rawPlainValue(valueWithTimestampAndHeaders));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueWithTimestampAndHeaders) {
        return convertFromPlainToHeaderFormat(store.putIfAbsent(
            key,
            rawPlainValue(valueWithTimestampAndHeaders)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestampAndHeaders = entry.value;
            store.put(entry.key, rawPlainValue(valueWithTimestampAndHeaders));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        return convertFromPlainToHeaderFormat(store.delete(key));
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
        return true;
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        // Handle KeyQuery: convert byte[] result from plain to headers format
        if (query instanceof KeyQuery) {
            final KeyQuery<Bytes, byte[]> keyQuery = (KeyQuery<Bytes, byte[]>) query;
            final QueryResult<byte[]> rawResult = store.query(keyQuery, positionBound, config);

            if (rawResult.isSuccess()) {
                final byte[] convertedValue = convertFromPlainToHeaderFormat(rawResult.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, convertedValue);
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else if (query instanceof RangeQuery) {
            // Handle RangeQuery: wrap iterator to convert values
            final RangeQuery<Bytes, byte[]> rangeQuery = (RangeQuery<Bytes, byte[]>) query;
            final QueryResult<KeyValueIterator<Bytes, byte[]>> rawResult =
                    store.query(rangeQuery, positionBound, config);

            if (rawResult.isSuccess()) {
                final KeyValueIterator<Bytes, byte[]> convertedIterator =
                        new PlainToHeadersIteratorAdapter<>(rawResult.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, convertedIterator);
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else {
            // For other query types, delegate to the underlying store
            result = store.query(query, positionBound, config);
        }

        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (System.nanoTime() - start) + "ns"
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
        return convertFromPlainToHeaderFormat(store.get(key));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return new PlainToHeadersIteratorAdapter<>(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                        final Bytes to) {
        return new PlainToHeadersIteratorAdapter<>(store.reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new PlainToHeadersIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new PlainToHeadersIteratorAdapter<>(store.reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        return new PlainToHeadersIteratorAdapter<>(store.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }
}