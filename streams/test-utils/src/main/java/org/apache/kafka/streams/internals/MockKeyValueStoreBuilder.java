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

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.MockTime;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;


public class MockKeyValueStoreBuilder extends AbstractStoreBuilder<byte[], byte[], KeyValueStore> {

    private final boolean persistent;
    private final KeyValueStore keyValueStore;

    public MockKeyValueStoreBuilder(final String storeName, final boolean persistent) {
        super(storeName, Serdes.ByteArray(), Serdes.ByteArray(), new MockTime(0));
        this.persistent = persistent;
        this.keyValueStore = new InMemoryKeyValueStore(storeName);

    }

    // The users can plugin their own kv store as the backend of the mock store.
    public MockKeyValueStoreBuilder(final String storeName, KeyValueStore keyValueStore, final boolean persistent) {
        super(storeName, Serdes.ByteArray(), Serdes.ByteArray(), new MockTime(0));
        this.persistent = persistent;
        this.keyValueStore = keyValueStore;
    }


    @Override
    public KeyValueStore build() {
        return new MockKeyValueStore(name, keyValueStore, persistent);
    }
}

