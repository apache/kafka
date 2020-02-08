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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ReadOnlyKeyValueStoreFacade;

import java.util.List;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KeyValueStoreFacade<K, V> extends ReadOnlyKeyValueStoreFacade<K, V> implements KeyValueStore<K, V> {

    public KeyValueStoreFacade(final TimestampedKeyValueStore<K, V> inner) {
        super(inner);
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void put(final K key,
                    final V value) {
        inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP));
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        return getValueOrNull(inner.putIfAbsent(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP)));
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        for (final KeyValue<K, V> entry : entries) {
            inner.put(entry.key, ValueAndTimestamp.make(entry.value, ConsumerRecord.NO_TIMESTAMP));
        }
    }

    @Override
    public V delete(final K key) {
        return getValueOrNull(inner.delete(key));
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }
}