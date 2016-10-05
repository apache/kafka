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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;

public class KeyValueIteratorStub<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<KeyValue<K, V>> iterator;

    public KeyValueIteratorStub(final Iterator<KeyValue<K, V>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public K peekNextKey() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        return iterator.next();
    }

    @Override
    public void remove() {

    }
}
