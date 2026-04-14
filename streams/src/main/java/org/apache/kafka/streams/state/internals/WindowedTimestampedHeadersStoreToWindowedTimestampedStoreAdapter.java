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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.Map;

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
public class WindowedTimestampedHeadersStoreToWindowedTimestampedStoreAdapter<K, V>
    extends WrappedStateStore<TimestampedWindowStore<K, V>, K, V>
    implements TimestampedWindowStoreWithHeaders<K, V> {

    public WindowedTimestampedHeadersStoreToWindowedTimestampedStoreAdapter(final TimestampedWindowStore<K, V> timestampedWindowStore) {
        super(timestampedWindowStore);
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
    public void put(final K key, final ValueTimestampHeaders<V> valueTimestampHeaders, final long windowStartTimestamp) {
        wrapped().put(key, valueTimestampHeaders == null ? null : ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp()), windowStartTimestamp);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> fetch(final K key, final long timeFrom, final long timeTo) {
        return new WindowStoreIteratorAdapter(wrapped().fetch(key, timeFrom, timeTo));
    }

    @Override
    public ValueTimestampHeaders<V> fetch(final K key, final long time) {
        final ValueAndTimestamp<V> valueAndTimestamp = wrapped().fetch(key, time);
        return valueAndTimestamp == null ? null : ValueTimestampHeaders.make(valueAndTimestamp.value(), valueAndTimestamp.timestamp(), new RecordHeaders());
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> fetch(final K key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorAdapter(wrapped().fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> backwardFetch(final K key, final long timeFrom, final long timeTo) {
        return new WindowStoreIteratorAdapter(wrapped().backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> backwardFetch(final K key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorAdapter(wrapped().backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetch(final K keyFrom, final K keyTo, final long timeFrom, final long timeTo) {
        return new KeyValueIteratorAdapter(wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetch(final K keyFrom, final K keyTo, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorAdapter(wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetch(final K keyFrom, final K keyTo, final long timeFrom, final long timeTo) {
        return new KeyValueIteratorAdapter(wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetch(final K keyFrom, final K keyTo, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorAdapter(wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> all() {
        return new KeyValueIteratorAdapter(wrapped().all());
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardAll() {
        return new KeyValueIteratorAdapter(wrapped().backwardAll());
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchAll(final long timeFrom, final long timeTo) {
        return new KeyValueIteratorAdapter(wrapped().fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorAdapter(wrapped().fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchAll(final long timeFrom, final long timeTo) {
        return new KeyValueIteratorAdapter(wrapped().backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorAdapter(wrapped().backwardFetchAll(timeFrom, timeTo));
    }

    private final class WindowStoreIteratorAdapter implements WindowStoreIterator<ValueTimestampHeaders<V>> {
        private final WindowStoreIterator<ValueAndTimestamp<V>> innerIterator;

        private WindowStoreIteratorAdapter(final WindowStoreIterator<ValueAndTimestamp<V>> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Long peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Long, ValueTimestampHeaders<V>> next() {
            final KeyValue<Long, ValueAndTimestamp<V>> next = innerIterator.next();
            return KeyValue.pair(next.key, ValueTimestampHeaders.make(next.value.value(), next.value.timestamp(), new RecordHeaders()));
        }
    }

    private final class KeyValueIteratorAdapter implements KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> {
        private final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator;

        private KeyValueIteratorAdapter(final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Windowed<K> peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, ValueTimestampHeaders<V>> next() {
            final KeyValue<Windowed<K>, ValueAndTimestamp<V>> next = innerIterator.next();
            return KeyValue.pair(next.key, ValueTimestampHeaders.make(next.value.value(), next.value.timestamp(), new RecordHeaders()));
        }
    }
}
