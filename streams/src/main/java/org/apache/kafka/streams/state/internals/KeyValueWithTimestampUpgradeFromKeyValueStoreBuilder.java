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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StoreUpgradeBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class KeyValueWithTimestampUpgradeFromKeyValueStoreBuilder<K, V>
        extends KeyValueWithTimestampStoreBuilder<K, V>
        implements StoreUpgradeBuilder<KeyValueWithTimestampStore<K, V>,
                                                    KeyValueWithTimestampRecordConverterStore<K, V>> {

    private final StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder;

    public KeyValueWithTimestampUpgradeFromKeyValueStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                                                final Serde<K> keySerde,
                                                                final Serde<V> valueSerde,
                                                                final Time time) {
        super(storeSupplier, keySerde, valueSerde, time);
        keyValueStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, keySerde, valueSerde);
    }

    @Override
    public KeyValueWithTimestampStoreToKeyValueStoreProxy<K, V> storeProxy() {
        if (!enableLogging) {
            throw new IllegalStateException("Upgrading stores is only supported of change logging is enabled.");
        }
        keyValueStoreBuilder.withLoggingEnabled(logConfig());

        if (enableCaching) {
            keyValueStoreBuilder.withCachingEnabled();
        }

        return new KeyValueWithTimestampStoreToKeyValueStoreProxy<>(keyValueStoreBuilder.build());
    }

    @Override
    public KeyValueWithTimestampRecordConverterStore<K, V> converterStore() {
        if (!enableLogging) {
            throw new IllegalStateException("Upgrading stores is only supported of change logging is enabled.");
        }

        return new KeyValueWithTimestampRecordConverterStore<>(
            new MeteredKeyValueStore<>(
                new ChangeLoggingKeyValueWithTimestampBytesStore(storeSupplier.get(), true),
                storeSupplier.metricsScope(),
                time,
                Serdes.Bytes(),
                Serdes.ByteArray()),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

}
