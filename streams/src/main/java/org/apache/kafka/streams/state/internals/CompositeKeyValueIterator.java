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
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

class CompositeKeyValueIterator<K, V, StoreType> implements KeyValueIterator<K, V> {

    private final Iterator<StoreType> storeIterator;
    private final NextIteratorFunction<K, V, StoreType> nextIteratorFunction;

    private KeyValueIterator<K, V> current;

    CompositeKeyValueIterator(final Iterator<StoreType> underlying,
                              final NextIteratorFunction<K, V, StoreType> nextIteratorFunction) {
        this.storeIterator = underlying;
        this.nextIteratorFunction = nextIteratorFunction;
    }

    @Override
    public void close() {
        if (current != null) {
            current.close();
            current = null;
        }
    }

    @Override
    public K peekNextKey() {
        throw new UnsupportedOperationException("peekNextKey not supported");
    }

    @Override
    public boolean hasNext() {
        while ((current == null || !current.hasNext())
                && storeIterator.hasNext()) {
            close();
            current = nextIteratorFunction.apply(storeIterator.next());
        }
        return current != null && current.hasNext();
    }


    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return current.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }
}
