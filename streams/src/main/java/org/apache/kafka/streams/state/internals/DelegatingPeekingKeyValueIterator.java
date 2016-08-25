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

import java.util.NoSuchElementException;

class DelegatingPeekingKeyValueIterator implements PeekingKeyValueIterator<Bytes, byte[]> {
    private final KeyValueIterator<Bytes, byte[]> underlying;
    private KeyValue<Bytes, byte[]> next;

    public DelegatingPeekingKeyValueIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
        this.underlying = underlying;
    }

    @Override
    public Bytes peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next.key;
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public boolean hasNext() {
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
    public KeyValue<Bytes, byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<Bytes, byte[]> result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {

    }
}
