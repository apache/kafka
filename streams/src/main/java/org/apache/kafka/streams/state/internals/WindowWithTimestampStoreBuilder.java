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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class WindowWithTimestampStoreBuilder<K, V> extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, WindowStore<K, ValueAndTimestamp<V>>> {

    private final WindowBytesStoreSupplier storeSupplier;

    public WindowWithTimestampStoreBuilder(final WindowBytesStoreSupplier storeSupplier,
                                           final Serde<K> keySerde,
                                           final Serde<V> valueSerde,
                                           final Time time) {
        super(storeSupplier.name(), keySerde, new KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde<>(valueSerde), time);
        this.storeSupplier = storeSupplier;
    }

    @Override
    public WindowStore<K, ValueAndTimestamp<V>> build() {
        WindowStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store instanceof StoreWithTimestamps) && store.persistent()) {
            store = new WindowToWindowWithTimestampByteProxyStore(store);
        }
        return new MeteredWindowWithTimestampStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(final WindowStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingWindowWithTimestampStore<>(
            inner,
            keySerde,
            valueSerde,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(final WindowStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingWindowWithTimestampBytesStore(inner, storeSupplier.retainDuplicates());
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }
}
