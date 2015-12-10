/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.WindowSupplier;
import org.apache.kafka.streams.kstream.internals.FilteredIterator;
import org.apache.kafka.streams.processor.internals.Stamped;

import java.util.Iterator;
import java.util.LinkedList;

public class UnlimitedWindowDef<K, V> implements WindowSupplier<K, V> {

    private final String name;

    public UnlimitedWindowDef(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public Window<K, V> get() {
        return new UnlimitedWindow();
    }

    public class UnlimitedWindow implements Window<K, V> {

        private final LinkedList<Stamped<KeyValue<K, V>>> list = new LinkedList<>();

        @Override
        public void init(ProcessorContext context) {
            context.register(this, true, null);
        }

        @Override
        public Iterator<V> find(final K key, long timestamp) {
            return find(key, Long.MIN_VALUE, timestamp);
        }

        @Override
        public Iterator<V> findAfter(final K key, long timestamp) {
            return find(key, timestamp, Long.MAX_VALUE);
        }

        @Override
        public Iterator<V> findBefore(final K key, long timestamp) {
            return find(key, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        private Iterator<V> find(final K key, final long startTime, final long endTime) {
            return new FilteredIterator<V, Stamped<KeyValue<K, V>>>(list.iterator()) {
                protected V filter(Stamped<KeyValue<K, V>> item) {
                    if (item.value.key.equals(key) && startTime <= item.timestamp && item.timestamp <= endTime)
                        return item.value.value;
                    else
                        return null;
                }
            };
        }

        @Override
        public void put(K key, V value, long timestamp) {
            list.add(new Stamped<>(KeyValue.pair(key, value), timestamp));
        }

        @Override
        public String name() {
            return name;
        }

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
    }
}
