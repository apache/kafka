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
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

class ReadOnlyKeyValueStoreFacade implements ReadOnlyKeyValueStore {

    private final KeyValueWithTimestampStore<Object, Object> inner;

    ReadOnlyKeyValueStoreFacade(final KeyValueWithTimestampStore<Object, Object> inner) {
        this.inner = inner;
    }

    @Override
    public Object get(final Object key) {
        final ValueAndTimestamp<Object> valueAndTimestamp = inner.get(key);
        return valueAndTimestamp.value();
    }

    @Override
    public KeyValueIterator<Object, Object> range(final Object from, final Object to) {
        final KeyValueIterator<Object, ValueAndTimestamp<Object>> innerIterator = inner.range(from, to);
        return new KeyValueIteratorFacade(innerIterator);
    }

    @Override
    public KeyValueIterator all() {
        final KeyValueIterator<Object, ValueAndTimestamp<Object>> innerIterator = inner.all();
        return new KeyValueIteratorFacade(innerIterator);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }



    private static class KeyValueIteratorFacade implements KeyValueIterator<Object, Object> {
        private final KeyValueIterator<Object, ValueAndTimestamp<Object>> innerIterator;

        KeyValueIteratorFacade(final KeyValueIterator<Object, ValueAndTimestamp<Object>> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Object peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Object, Object> next() {
            final KeyValue<Object, ValueAndTimestamp<Object>> innerKeyValue = innerIterator.next();
            return KeyValue.pair(innerKeyValue.key, innerKeyValue.value.value());
        }
    }
}
