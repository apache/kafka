/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate over multiple Segments
 * @param <K>
 * @param <V>
 */
class SegmentIterator<K, V> implements KeyValueIterator<K, V> {

    private final StateSerdes<?, V> serdes;
    private final Iterator<Segment> segments;
    private final KeyExtractor<K> keyExtractor;
    private final HasNextCondition hasNextCondition;
    private final SegmentQuery segmentQuery;
    private KeyValueIterator<Bytes, byte[]> currentIterator;
    private KeyValueStore<Bytes, byte[]> currentSegment;

    SegmentIterator(final StateSerdes<?, V> serdes,
                    final Iterator<Segment> segments,
                    final KeyExtractor<K> keyExtractor,
                    final HasNextCondition hasNextCondition,
                    final SegmentQuery segmentQuery) {
        this.serdes = serdes;
        this.segments = segments;
        this.keyExtractor = keyExtractor;
        this.hasNextCondition = hasNextCondition;
        this.segmentQuery = segmentQuery;
    }

    public void close() {
        if (currentIterator != null) {
            currentIterator.close();
            currentIterator = null;
        }
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return keyExtractor.apply(currentIterator.peekNextKey());
    }

    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        boolean hasNext = false;
        while ((currentIterator == null || !(hasNext = hasNextCondition.hasNext(currentIterator)) || !currentSegment.isOpen())
                && segments.hasNext()) {
            close();
            currentSegment = segments.next();
            try {
                currentIterator = segmentQuery.query(currentSegment);
            } catch (InvalidStateStoreException e) {
                // segment may have been closed so we ignore it.
            }
        }
        return currentIterator != null && hasNext;
    }

    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValue<Bytes, byte[]> kv = currentIterator.next();
        return new KeyValue<>(keyExtractor.apply(kv.key),
                              serdes.valueFrom(kv.value));
    }

    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    interface KeyExtractor<K> {
        K apply(final Bytes data);
    }

    interface SegmentQuery<K, V> {
        KeyValueIterator<K, V> query(final KeyValueStore<Bytes, byte[]> segment);
    }
}
