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
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

public class SubscriptionStoreFactory<K> extends AbstractConfigurableStoreFactory {
    private final String name;
    private final Serde<SubscriptionWrapper<K>> subscriptionWrapperSerde;
    private final Map<String, String> logConfig = new HashMap<>();
    private boolean loggingEnabled = true;

    public SubscriptionStoreFactory(
        final String name,
        final Serde<SubscriptionWrapper<K>> subscriptionWrapperSerde
    ) {
        super(null);
        this.name = name;
        this.subscriptionWrapperSerde = subscriptionWrapperSerde;
    }

    @Override
    public StoreBuilder<?> builder() {
        StoreBuilder<?> builder;
        builder = Stores.timestampedKeyValueStoreBuilder(
            dslStoreSuppliers().keyValueStore(new DslKeyValueParams(name, true)),
            new Serdes.BytesSerde(),
            subscriptionWrapperSerde
        );
        if (loggingEnabled) {
            builder = builder.withLoggingEnabled(logConfig);
        } else {
            builder = builder.withLoggingDisabled();
        }
        builder = builder.withCachingDisabled();
        return builder;
    }

    @Override
    public long retentionPeriod() {
        throw new IllegalStateException("retentionPeriod is not supported when not a window store");
    }

    @Override
    public long historyRetention() {
        throw new IllegalStateException(
            "historyRetention is not supported when not a versioned store");
    }

    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    @Override
    public String storeName() {
        return name;
    }

    @Override
    public boolean isWindowStore() {
        return false;
    }

    @Override
    public boolean isVersionedStore() {
        return false;
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public StoreFactory withCachingDisabled() {
        // caching is always disabled
        return this;
    }

    @Override
    public StoreFactory withLoggingDisabled() {
        loggingEnabled = false;
        return this;
    }

    @Override
    public boolean isCompatibleWith(final StoreFactory other) {
        return other instanceof SubscriptionStoreFactory
            && ((SubscriptionStoreFactory<?>) other).name.equals(name);
    }
}