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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

public class ListValueStoreBuilder<K, V> extends AbstractStoreBuilder<K, V, KeyValueStore<K, V>> {
    private final KeyValueBytesStoreSupplier storeSupplier;

    public ListValueStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                 final Serde<K> keySerde,
                                 final Serde<V> valueSerde,
                                 final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public KeyValueStore<K, V> build() {
        return new MeteredKeyValueStore<>(
                maybeWrapCaching(maybeWrapLogging(new ListValueStore(storeSupplier.get()))),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner, false);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingListValueBytesStore(inner);
    }
}
