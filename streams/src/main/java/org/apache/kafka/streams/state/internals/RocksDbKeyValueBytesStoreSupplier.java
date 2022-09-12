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
    private final RocksDBTransactionalMechanism mechanism;

    public RocksDbKeyValueBytesStoreSupplier(final String name,
                                             final boolean returnTimestampedStore,
                                             final RocksDBTransactionalMechanism mechanism) {
        this.name = name;
        this.returnTimestampedStore = returnTimestampedStore;
        this.mechanism = mechanism;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        if (mechanism == null)  {
            return returnTimestampedStore ?
                new RocksDBTimestampedStore(name, metricsScope()) :
                new RocksDBStore(name, metricsScope());
        }

        if (mechanism == RocksDBTransactionalMechanism.SECONDARY_STORE) {
            if (returnTimestampedStore) {
                return new TransactionalKeyValueStore(new RocksDBTimestampedStore(name, metricsScope()), metricsScope());
            } else {
                return new TransactionalKeyValueStore(new RocksDBStore(name, metricsScope()), metricsScope());
            }
        }
        throw new IllegalArgumentException("Unsupported transactional mechanism: " + mechanism);
    }

    @Override
    public String metricsScope() {
        return "rocksdb";
    }
}
