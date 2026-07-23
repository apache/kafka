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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A merge iterator that overlays staged writes (from a {@link NavigableMap}) on top of
 * a base store iterator. Staging entries take precedence over base entries for the same key.
 * Tombstones ({@link Optional#empty()}) in the staging map cause the corresponding key
 * to be skipped in the merged output.
 * <p>
 * Supports both forward and reverse iteration. When iterating in reverse, the staging
 * map is traversed in descending order and the merge comparison is inverted so that
 * the largest key is emitted first.
 *
 * @param <K> the key type, must be {@link Comparable}
 * @param <V> the value type
 */
class StagedMergeIterator<K extends Comparable<K>, V> implements ManagedKeyValueIterator<K, V> {

    private final Iterator<Map.Entry<K, Optional<V>>> stagingIterator;
    private final KeyValueIterator<K, V> baseIterator;
    private final boolean forward;

    private Map.Entry<K, Optional<V>> nextStaging;
    private KeyValue<K, V> nextBase;
    private KeyValue<K, V> prefetched;
    private boolean closed = false;
    private Runnable closeCallback;

    StagedMergeIterator(final NavigableMap<K, Optional<V>> staging,
                        final KeyValueIterator<K, V> baseIterator) {
        this(staging, baseIterator, true);
    }

    StagedMergeIterator(final NavigableMap<K, Optional<V>> staging,
                        final KeyValueIterator<K, V> baseIterator,
                        final boolean forward) {
        this.forward = forward;
        final NavigableMap<K, Optional<V>> orderedStaging = forward ? staging : staging.descendingMap();
        this.stagingIterator = orderedStaging.entrySet().iterator();
        this.baseIterator = baseIterator;
        advanceStaging();
        advanceBase();
    }

    private void advanceStaging() {
        nextStaging = stagingIterator.hasNext() ? stagingIterator.next() : null;
    }

    private void advanceBase() {
        nextBase = baseIterator.hasNext() ? baseIterator.next() : null;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new InvalidStateStoreException("Iterator has already been closed.");
        }
        if (prefetched != null) {
            return true;
        }
        prefetched = computeNext();
        return prefetched != null;
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<K, V> result = prefetched;
        prefetched = null;
        return result;
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return prefetched.key;
    }

    @Override
    public void onClose(final Runnable closeCallback) {
        this.closeCallback = closeCallback;
    }

    @Override
    public void close() {
        closed = true;
        try {
            baseIterator.close();
        } finally {
            if (closeCallback != null) {
                closeCallback.run();
            }
        }
    }

    private KeyValue<K, V> computeNext() {
        while (nextStaging != null || nextBase != null) {
            if (nextStaging == null) {
                // staging exhausted — emit base
                final KeyValue<K, V> result = nextBase;
                advanceBase();
                return result;
            }
            if (nextBase == null) {
                // base exhausted — skip tombstones, emit non-tombstone staging entries
                final Map.Entry<K, Optional<V>> entry = nextStaging;
                advanceStaging();
                if (entry.getValue().isPresent()) {
                    return new KeyValue<>(entry.getKey(), entry.getValue().get());
                }
                // tombstone — continue loop
                continue;
            }

            // Compare keys; in reverse mode, invert so the largest key is "first"
            final int rawCmp = nextStaging.getKey().compareTo(nextBase.key);
            final int cmp = forward ? rawCmp : Integer.compare(0, rawCmp);
            if (cmp < 0) {
                // staging key comes first — emit it or skip if tombstone
                final Map.Entry<K, Optional<V>> entry = nextStaging;
                advanceStaging();
                if (entry.getValue().isPresent()) {
                    return new KeyValue<>(entry.getKey(), entry.getValue().get());
                }
                // tombstone — continue loop
            } else if (cmp > 0) {
                // base key comes first
                final KeyValue<K, V> result = nextBase;
                advanceBase();
                return result;
            } else {
                // same key — staging takes precedence, skip base
                advanceBase();
                final Map.Entry<K, Optional<V>> entry = nextStaging;
                advanceStaging();
                if (entry.getValue().isPresent()) {
                    return new KeyValue<>(entry.getKey(), entry.getValue().get());
                }
                // tombstone — both skipped, continue loop
            }
        }
        return null;
    }
}
