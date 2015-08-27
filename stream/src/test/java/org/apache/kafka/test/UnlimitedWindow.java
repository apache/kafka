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

import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.kstream.KeyValue;
import org.apache.kafka.streaming.kstream.WindowDef;
import org.apache.kafka.streaming.kstream.internals.FilteredIterator;
import org.apache.kafka.streaming.processor.internals.Stamped;

import java.util.Iterator;
import java.util.LinkedList;

public class UnlimitedWindow<K, V> implements WindowDef<K, V> {

    private LinkedList<Stamped<KeyValue<K, V>>> list = new LinkedList<>();

    @Override
    public void init(ProcessorContext context) {
        // do nothing
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
        return null;
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
