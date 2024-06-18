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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;

import static java.time.Duration.ofMillis;

public abstract class AbstractRocksDBSessionStoreTest extends AbstractSessionBytesStoreTest {

    private static final String STORE_NAME = "rocksDB session store";

    abstract StoreType storeType();

    @Override
    StoreType getStoreType() {
        return storeType();
    }

    @Override
    <K, V> SessionStore<K, V> buildSessionStore(final long retentionPeriod,
                                                final Serde<K> keySerde,
                                                final Serde<V> valueSerde) {
        switch (storeType()) {
            case RocksDBSessionStore: {
                return Stores.sessionStoreBuilder(
                        Stores.persistentSessionStore(
                                STORE_NAME,
                                ofMillis(retentionPeriod)),
                        keySerde,
                        valueSerde).build();
            }
            case RocksDBTimeOrderedSessionStoreWithIndex: {
                return Stores.sessionStoreBuilder(
                        new RocksDbTimeOrderedSessionBytesStoreSupplier(
                                STORE_NAME,
                                retentionPeriod,
                                true
                        ),
                        keySerde,
                        valueSerde
                ).build();
            }
            case RocksDBTimeOrderedSessionStoreWithoutIndex: {
                return Stores.sessionStoreBuilder(
                        new RocksDbTimeOrderedSessionBytesStoreSupplier(
                                STORE_NAME,
                                retentionPeriod,
                                false
                        ),
                        keySerde,
                        valueSerde
                ).build();
            }
            default:
                throw new IllegalStateException("Unknown StoreType: " + storeType());
        }
    }
}
