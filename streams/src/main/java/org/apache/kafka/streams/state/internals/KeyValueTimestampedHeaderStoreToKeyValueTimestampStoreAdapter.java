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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Adaptor store for the Kafka Streams DSL to bridge between "headers" store and "ts-store".
 *
 * <p> With KIP-1285 we did rewrite the DLS Processor code to work against "header store" interface to allow users
 * to plugin "header stores", but by default the underlying store is still a "ts-store". To avoid "if-then-else"
 * code across the entire DSL Processor code base, we use this adaptor to wrap a "ts-store" and make it look like
 * a "header store".
 *
 * <p> On any write operation, provided {@link org.apache.kafka.common.header.Headers} will just be dropped,
 * and {@link ValueTimestampHeaders} type is translated into {@link ValueAndTimestamp} type. Similarly for
 * any read operation, the underlying {@link ValueAndTimestamp} type is translated into a {@link ValueTimestampHeaders}
 * type with an empty {@link org.apache.kafka.common.header.Headers} object.
 */
public class KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter<K, V>
    extends WrappedStateStore<TimestampedKeyValueStore<K, V>, K, V>
    implements TimestampedKeyValueStoreWithHeaders<K, V> {

    public KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter(final TimestampedKeyValueStore<K, V> timestampedKeyValueStore) {
        super(timestampedKeyValueStore);
    }

    @Override
    public String name() {
        return wrapped().name();
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        wrapped().init(stateStoreContext, root);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void flush() {
        wrapped().flush();
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        wrapped().commit(changelogOffsets);
    }

    @Override
    public Long committedOffset(final TopicPartition partition) {
        return wrapped().committedOffset(partition);
    }

    @Override
    public void close() {
        wrapped().close();
    }

    @Override
    public boolean persistent() {
        return wrapped().persistent();
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean managesOffsets() {
        return wrapped().managesOffsets();
    }

    @Override
    public boolean isOpen() {
        return wrapped().isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return wrapped().query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return wrapped().getPosition();
    }

    @Override
    public void put(final K key, final ValueTimestampHeaders<V> value) {
        wrapped().put(
            key,
            value == null ? null : ValueAndTimestamp.make(value.value(), value.timestamp())
        );
    }

    @Override
    public ValueTimestampHeaders<V> putIfAbsent(final K key, final ValueTimestampHeaders<V> value) {
        final ValueAndTimestamp<V> oldValueAndTimestamp = wrapped().putIfAbsent(
            key,
            value == null ? null : ValueAndTimestamp.make(value.value(), value.timestamp())
        );

        return oldValueAndTimestamp == null
            ? null
            : ValueTimestampHeaders.make(oldValueAndTimestamp.value(), oldValueAndTimestamp.timestamp(), new RecordHeaders());
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueTimestampHeaders<V>>> entries) {
        wrapped().putAll(
            entries.stream().map(keyValuePair -> KeyValue.pair(
                keyValuePair.key,
                ValueAndTimestamp.make(keyValuePair.value.value(), keyValuePair.value.timestamp()))
            )
            .collect(Collectors.toList())
        );
    }

    @Override
    public ValueTimestampHeaders<V> delete(final K key) {
        final ValueAndTimestamp<V> oldValueAndTimestamp = wrapped().delete(key);

        return oldValueAndTimestamp == null
            ? null
            : ValueTimestampHeaders.make(oldValueAndTimestamp.value(), oldValueAndTimestamp.timestamp(), new RecordHeaders());
    }

    @Override
    public ValueTimestampHeaders<V> get(final K key) {
        final ValueAndTimestamp<V> valueAndTimestamp = wrapped().get(key);

        return valueAndTimestamp == null
            ? null
            : ValueTimestampHeaders.make(valueAndTimestamp.value(), valueAndTimestamp.timestamp(), new RecordHeaders());
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from, final K to) {
        return new KeyValueIteratorAdapter(wrapped().range(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRange(final K from, final K to) {
        return new KeyValueIteratorAdapter(wrapped().reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
        return new KeyValueIteratorAdapter(wrapped().all());
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAll() {
        return new KeyValueIteratorAdapter(wrapped().reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScan(final P prefix, final PS prefixKeySerializer) {
        return new KeyValueIteratorAdapter(wrapped().prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    private final class KeyValueIteratorAdapter implements KeyValueIterator<K, ValueTimestampHeaders<V>> {
        private final KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator;

        private KeyValueIteratorAdapter(final KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public K peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<K, ValueTimestampHeaders<V>> next() {
            final KeyValue<K, ValueAndTimestamp<V>> next = innerIterator.next();
            return KeyValue.pair(next.key, ValueTimestampHeaders.make(next.value.value(), next.value.timestamp(), new RecordHeaders()));
        }
    }
}
