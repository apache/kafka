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
import org.apache.kafka.streams.state.HeaderBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Builder for creating TimestampedWindowStoreWithHeaders instances.
 * This is the KIP-1271 version that supports storing headers along with timestamps.
 */
public class TimestampedWindowStoreWithHeadersBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueTimestampHeaders<V>, TimestampedWindowStoreWithHeaders<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampedWindowStoreWithHeadersBuilder.class);

    private final WindowBytesStoreSupplier storeSupplier;

    public TimestampedWindowStoreWithHeadersBuilder(final WindowBytesStoreSupplier storeSupplier,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Time time) {
        super(
            storeSupplier.name(),
            keySerde,
            valueSerde == null ? null : new ValueTimestampHeadersSerde<>(valueSerde),
            time
        );
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedWindowStoreWithHeaders<K, V> build() {
        WindowStore<Bytes, byte[]> store = storeSupplier.get();

        // Verify the store supports headers
        if (!(store instanceof HeaderBytesStore)) {
            throw new IllegalArgumentException(
                "Store supplier must provide a HeaderBytesStore implementation. " +
                "Use Stores.persistentTimestampedWindowStoreWithHeaders() to create the supplier."
            );
        }

        // Disable caching if retaining duplicates
        if (storeSupplier.retainDuplicates() && enableCaching) {
            LOG.warn("Disabling caching for {} since store was configured to retain duplicates",
                storeSupplier.name());
            enableCaching = false;
        }

        // Wrap with caching and logging if needed
        final WindowStore<Bytes, byte[]> wrappedStore = maybeWrapCaching(maybeWrapLogging(store));

        // Wrap with MeteredTimestampedWindowStoreWithHeaders
        return new MeteredTimestampedWindowStoreWithHeaders<>(
            wrappedStore,
            storeSupplier.windowSize(),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde
        );
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(final WindowStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }

        // For headers-aware stores, we can use the same caching wrappers
        // TODO: Implement time-ordered check if needed
        return new CachingWindowStore(
            inner,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(final WindowStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        // Use the timestamped changelog wrapper (headers are in the value bytes)
        return new ChangeLoggingTimestampedWindowBytesStore(inner, storeSupplier.retainDuplicates());
    }
}
