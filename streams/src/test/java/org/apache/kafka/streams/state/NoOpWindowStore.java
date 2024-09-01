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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;

import java.time.Instant;
import java.util.NoSuchElementException;

public class NoOpWindowStore implements ReadOnlyWindowStore, StateStore {

    private static class EmptyWindowStoreIterator implements WindowStoreIterator<KeyValue> {

        @Override
        public void close() {
        }

        @Override
        public Long peekNextKey() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public KeyValue<Long, KeyValue> next() {
            throw new NoSuchElementException();
        }
    }

    private static final WindowStoreIterator<KeyValue> EMPTY_WINDOW_STORE_ITERATOR = new EmptyWindowStoreIterator();

    @Override
    public String name() {
        return "";
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {}

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("Position handling not implemented");
    }

    @Override
    public Object fetch(final Object key, final long time) {
        return null;
    }

    @Override
    public WindowStoreIterator fetch(final Object key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public WindowStoreIterator backwardFetch(final  Object key,
                                             final Instant timeFrom,
                                             final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public KeyValueIterator fetch(final Object keyFrom,
                                  final Object keyTo,
                                  final Instant timeFrom,
                                  final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public KeyValueIterator backwardFetch(final Object from,
                                          final Object keyTo,
                                          final Instant timeFrom,
                                          final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public WindowStoreIterator<KeyValue> all() {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public WindowStoreIterator<KeyValue> backwardAll() {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public KeyValueIterator fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }

    @Override
    public KeyValueIterator backwardFetchAll(final Instant timeFrom,
                                             final Instant timeTo) throws IllegalArgumentException {
        return EMPTY_WINDOW_STORE_ITERATOR;
    }
}
