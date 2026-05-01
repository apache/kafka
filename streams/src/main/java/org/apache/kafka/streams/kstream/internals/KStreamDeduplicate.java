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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBTimeOrderedKeyValueBuffer;

import java.time.Duration;
import java.util.Set;

class KStreamDeduplicate<K, V, KR> implements ProcessorSupplier<K, V, K, V> {

    private final KeyValueMapper<? super K, ? super V, ? extends KR> idSelector;
    private final Duration deduplicationInterval;
    private final String baseStoreName;
    private final String timeIndexStoreName;
    private final Set<StoreBuilder<?>> stores;
    private final Serde<K> keySerde;
    private final Serde<KR> idSerde;

    KStreamDeduplicate(final KeyValueMapper<? super K, ? super V, ? extends KR> idSelector,
                       final Duration deduplicationInterval,
                       final Serde<K> keySerde,
                       final Serde<KR> idSerde,
                       final Serde<V> valueSerde,
                       final String name,
                       final String baseStoreName) {
        this.idSelector = idSelector;
        this.deduplicationInterval = deduplicationInterval;
        this.baseStoreName = baseStoreName;
        this.timeIndexStoreName = baseStoreName + "-time-index";

        final StoreBuilder<KeyValueStore<Bytes, TimestampAndOffset>> baseStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(baseStoreName),
                Serdes.Bytes(),
                new TimestampAndOffsetSerde()
            );

        final StoreBuilder<?> timeIndexStoreBuilder =
            new RocksDBTimeOrderedKeyValueBuffer.Builder<>(
                timeIndexStoreName,
                Serdes.Bytes(),
                valueSerde,
                deduplicationInterval,
                name
            );

        this.stores = Set.of(baseStoreBuilder, timeIndexStoreBuilder);
        this.keySerde = keySerde;
        this.idSerde = idSerde;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return stores;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new KStreamDeduplicateProcessor<>(idSelector, deduplicationInterval, keySerde, idSerde, baseStoreName, timeIndexStoreName);
    }
}
