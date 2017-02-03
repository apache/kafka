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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.NoSuchElementException;

class SerializedKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<K, V> serdes;

    SerializedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                               final StateSerdes<K, V> serdes) {

        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Bytes bytes = bytesIterator.peekNextKey();
        return serdes.keyFrom(bytes.get());
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
    }
}
