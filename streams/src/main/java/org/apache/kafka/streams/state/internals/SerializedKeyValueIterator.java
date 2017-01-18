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

    private final KeyValueIterator<Bytes, byte[]> underlying;
    private final StateSerdes<K, V> serdes;

    // delegating peeks to optimize for cases when each element is peeked multiple times
    protected KeyValue<Bytes, byte[]> rawNext;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class SerializedKeyValueBytesIterator extends SerializedKeyValueIterator<Bytes, byte[]> {
        SerializedKeyValueBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
            super(underlying, null);
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return rawNext;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return rawNext.key;
        }
    }

    static SerializedKeyValueIterator<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
        return new SerializedKeyValueBytesIterator(underlying);
    }

    SerializedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> underlying, final StateSerdes<K, V> serdes) {
        this.underlying = underlying;
        this.serdes = serdes;

        this.rawNext = null;
    }

    @Override
    public boolean hasNext() {
        if (rawNext == null && underlying.hasNext())
            rawNext = underlying.next();

        return rawNext != null;
    }

    /**
     * @throws NoSuchElementException if no next element exists
     */
    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final K key = serdes.keyFrom(rawNext.key.get());
        final V value = serdes.valueFrom(rawNext.value);
        return KeyValue.pair(key, value);
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return serdes.keyFrom(rawNext.key.get());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in SerializedKeyValueIterator.");
    }

    @Override
    public void close() {
        underlying.close();
    }
}
