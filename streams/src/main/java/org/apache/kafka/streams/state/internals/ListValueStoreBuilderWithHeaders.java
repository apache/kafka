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
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.Objects;

/**
 * Headers-aware variant of {@link ListValueStoreBuilder}.
 * <p>
 * The built store has the public type {@code KeyValueStore<K, ValueTimestampHeaders<V>>}.
 * Each list element is serialized via {@link ValueTimestampHeadersSerde} so that the per-element
 * record headers are preserved on disk.
 * <p>
 * Caching is intentionally not supported: the only production caller, the stream-stream
 * outer-join store factory, always disables caching for outer-join list stores.
 * <p>
 * Backwards compatibility: changelog records produced by the plain (non headers-aware)
 * {@link ListValueStoreBuilder} are not byte-compatible with this builder. Switching an
 * existing application from {@code DslStoreFormat.PLAIN} to {@code DslStoreFormat.HEADERS}
 * requires a reset of the affected changelog/state.
 */
public class ListValueStoreBuilderWithHeaders<K, V>
    extends AbstractStoreBuilder<K, ValueTimestampHeaders<V>, KeyValueStore<K, ValueTimestampHeaders<V>>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public ListValueStoreBuilderWithHeaders(final KeyValueBytesStoreSupplier storeSupplier,
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
    public KeyValueStore<K, ValueTimestampHeaders<V>> build() {
        return new MeteredKeyValueStore<>(
            maybeWrapLogging(new ListValueStore(storeSupplier.get())),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde
        );
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingListValueBytesStore(inner);
    }
}
