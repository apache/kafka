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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.state.internals.RocksDbIndexedTimeOrderedWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbTimeOrderedSessionBytesStoreSupplier;

/**
 * Collection of builtin {@link DslStoreSuppliers} for Kafka Streams. Today we
 * support RocksDb and InMemory stores out of the box.
 */
public class BuiltInDslStoreSuppliers {

    public static final DslStoreSuppliers ROCKS_DB = new RocksDBDslStoreSuppliers();
    public static final DslStoreSuppliers IN_MEMORY = new InMemoryDslStoreSuppliers();

    /**
     * A {@link DslStoreSuppliers} that supplies all stores backed by RocksDB
     */
    public static class RocksDBDslStoreSuppliers implements DslStoreSuppliers {

        @Override
        public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params) {
            return params.isTimestamped()
                    ? Stores.persistentTimestampedKeyValueStore(params.name())
                    : Stores.persistentKeyValueStore(params.name());
        }

        @Override
        public WindowBytesStoreSupplier windowStore(final DslWindowParams params) {
            if (params.emitStrategy().type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
                return RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                        params.name(),
                        params.retentionPeriod(),
                        params.windowSize(),
                        params.retainDuplicates(),
                        params.isSlidingWindow());
            }

            if (params.isTimestamped()) {
                return Stores.persistentTimestampedWindowStore(
                        params.name(),
                        params.retentionPeriod(),
                        params.windowSize(),
                        params.retainDuplicates());
            } else {
                return Stores.persistentWindowStore(
                        params.name(),
                        params.retentionPeriod(),
                        params.windowSize(),
                        params.retainDuplicates());
            }
        }

        @Override
        public SessionBytesStoreSupplier sessionStore(final DslSessionParams params) {
            if (params.emitStrategy().type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
                return new RocksDbTimeOrderedSessionBytesStoreSupplier(
                        params.name(),
                        params.retentionPeriod().toMillis(),
                        true);
            }

            return Stores.persistentSessionStore(params.name(), params.retentionPeriod());
        }
    }

    /**
     * A {@link DslStoreSuppliers} that supplies all stores backed by an in-memory map
     */
    public static class InMemoryDslStoreSuppliers implements DslStoreSuppliers {

        @Override
        public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params) {
            return Stores.inMemoryKeyValueStore(params.name());
        }

        @Override
        public WindowBytesStoreSupplier windowStore(final DslWindowParams params) {
            return Stores.inMemoryWindowStore(
                    params.name(),
                    params.retentionPeriod(),
                    params.windowSize(),
                    params.retainDuplicates()
            );
        }

        @Override
        public SessionBytesStoreSupplier sessionStore(final DslSessionParams params) {
            return Stores.inMemorySessionStore(params.name(), params.retentionPeriod());
        }
    }
}