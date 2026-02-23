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

public class RocksDBKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final boolean returnTimestampedStore;
    private final boolean returnHeadersStore;

    public RocksDBKeyValueBytesStoreSupplier(final String name,
                                             final boolean returnTimestampedStore,
                                             final boolean returnHeadersStore) {
        this.name = name;
        this.returnTimestampedStore = returnTimestampedStore;
        this.returnHeadersStore = returnHeadersStore;
        if (returnHeadersStore && !returnTimestampedStore) {
            throw new IllegalStateException(
                "RocksDBKeyValueBytesStoreSupplier cannot return a headers store without also returning a timestamped store!");
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        if (returnHeadersStore && returnTimestampedStore) {
            return new RocksDBTimestampedStoreWithHeaders(name, metricsScope());
        }
        if (returnTimestampedStore) {
            return new RocksDBTimestampedStore(name, metricsScope());
        }
        return new RocksDBStore(name, metricsScope());
    }

    @Override
    public String metricsScope() {
        return "rocksdb";
    }
}
