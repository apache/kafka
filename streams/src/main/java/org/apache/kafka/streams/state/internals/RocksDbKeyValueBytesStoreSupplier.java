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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class RocksDbKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final boolean returnTimestampedStore;
    private final int ttlInSeconds;

    public RocksDbKeyValueBytesStoreSupplier(final String name,
                                             final boolean returnTimestampedStore) {
        this.name = name;
        this.returnTimestampedStore = returnTimestampedStore;
        this.ttlInSeconds = 0;
    }

    public RocksDbKeyValueBytesStoreSupplier(final String name,
                                             final int ttlInSeconds) {
        this.name = name;
        this.returnTimestampedStore = false;
        this.ttlInSeconds = ttlInSeconds;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        if (ttlInSeconds > 0) return new RocksDBTTLStore(name, ttlInSeconds);
        else return returnTimestampedStore ? new RocksDBTimestampedStore(name) : new RocksDBStore(name);
    }

    @Override
    public String metricsScope() {
        return "rocksdb-state";
    }
}
