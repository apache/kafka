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
package org.apache.kafka.test;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.File;

public class NoOpReadOnlyStore<K, V> implements ReadOnlyKeyValueStore<K, V>, StateStore {
    private final String name;
    private final boolean rocksdbStore;
    private boolean open = true;
    public boolean initialized;
    public boolean flushed;

    public NoOpReadOnlyStore() {
        this("", false);
    }

    public NoOpReadOnlyStore(final String name) {
        this(name, false);
    }

    public NoOpReadOnlyStore(final String name,
                             final boolean rocksdbStore) {
        this.name = name;
        this.rocksdbStore = rocksdbStore;
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
        if (rocksdbStore) {
            // cf. RocksDBStore
            new File(context.stateDir() + File.separator + "rocksdb" + File.separator + name).mkdirs();
        } else {
            new File(context.stateDir() + File.separator + name).mkdir();
        }
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
        return rocksdbStore;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

}
