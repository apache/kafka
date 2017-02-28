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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class NoOpReadOnlyStore<K, V>
        implements ReadOnlyKeyValueStore<K, V>, StateStore {

    private final String name;
    private boolean open = true;
    public boolean initialized;
    public boolean flushed;


    public NoOpReadOnlyStore() {
        this("");
    }

    public NoOpReadOnlyStore(final String name) {
        this.name = name;
    }

    @Override
    public V get(final K key) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0L;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.initialized = true;
    }

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

}
