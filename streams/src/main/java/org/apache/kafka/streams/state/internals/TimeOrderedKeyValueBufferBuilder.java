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
import org.apache.kafka.streams.state.StoreBuilder;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TimeOrderedKeyValueBufferBuilder<K, V> implements StoreBuilder<TimeOrderedKeyValueBuffer<K, V>> {

    private final String storeName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private boolean loggingEnabled = true;
    private Map<String, String> logConfig = new HashMap<>();

    public TimeOrderedKeyValueBufferBuilder(final String storeName, final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * As of 2.1, there's no way for users to directly interact with the buffer,
     * so this method is implemented solely to be called by Streams (which
     * it will do based on the {@code cache.max.bytes.buffering} config.
     * <p>
     * It's currently a no-op.
     */
    @Override
    public StoreBuilder<TimeOrderedKeyValueBuffer<K, V>> withCachingEnabled() {
        return this;
    }

    /**
     * As of 2.1, there's no way for users to directly interact with the buffer,
     * so this method is implemented solely to be called by Streams (which
     * it will do based on the {@code cache.max.bytes.buffering} config.
     * <p>
     * It's currently a no-op.
     */
    @Override
    public StoreBuilder<TimeOrderedKeyValueBuffer<K, V>> withCachingDisabled() {
        return this;
    }

    @Override
    public StoreBuilder<TimeOrderedKeyValueBuffer<K, V>> withLoggingEnabled(final Map<String, String> config) {
        logConfig = config;
        return this;
    }

    @Override
    public StoreBuilder<TimeOrderedKeyValueBuffer<K, V>> withLoggingDisabled() {
        loggingEnabled = false;
        return this;
    }

    @Override
    public TimeOrderedKeyValueBuffer<K, V> build() {
        final InMemoryTimeOrderedKeyValueBuffer inner = new InMemoryTimeOrderedKeyValueBuffer(storeName, loggingEnabled);
        return new MeteredTimeOrderedKeyValueBuffer<>(inner, "in-memory-suppression", keySerde, valueSerde);
    }

    @Override
    public Map<String, String> logConfig() {
        return loggingEnabled() ? Collections.unmodifiableMap(logConfig) : Collections.emptyMap();
    }

    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    @Override
    public String name() {
        return storeName;
    }
}
