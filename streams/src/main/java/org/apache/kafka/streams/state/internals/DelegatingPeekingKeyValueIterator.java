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

import java.util.NoSuchElementException;

/**
 * Optimized {@link KeyValueIterator} used when the same element could be peeked multiple times.
 */
class DelegatingPeekingKeyValueIterator<K, V> implements KeyValueIterator<K, V>, PeekingKeyValueIterator<K, V> {
    private final KeyValueIterator<K, V> underlying;
    private final String storeName;
    private KeyValue<K, V> next;

    private volatile boolean open = true;

    public DelegatingPeekingKeyValueIterator(final String storeName, final KeyValueIterator<K, V> underlying) {
        this.storeName = storeName;
        this.underlying = underlying;
    }

    @Override
    public synchronized K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next.key;
    }

    @Override
    public synchronized void close() {
        underlying.close();
        open = false;
    }

    @Override
    public synchronized boolean hasNext() {
        if (!open) {
            throw new InvalidStateStoreException(String.format("Store %s has closed", storeName));
        }
        if (next != null) {
            return true;
        }

        if (!underlying.hasNext()) {
            return false;
        }

        next = underlying.next();
        return true;
    }

    @Override
    public synchronized KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<K, V> result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
    }

    @Override
    public KeyValue<K, V> peekNext() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }
}
