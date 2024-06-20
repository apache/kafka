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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.DslWindowParams;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class StreamJoinedStoreFactory<K, V1, V2> extends AbstractConfigurableStoreFactory {

    private final String name;
    private final JoinWindows windows;
    private final Serde<?> valueSerde;
    private final WindowBytesStoreSupplier storeSupplier;
    private final StreamJoinedInternal<K, V1, V2> joinedInternal;

    private boolean loggingEnabled;
    private final Map<String, String> logConfig;

    public enum Type {
        THIS,
        OTHER
    }

    public StreamJoinedStoreFactory(
            final String name,
            final JoinWindows windows,
            final StreamJoinedInternal<K, V1, V2> joinedInternal,
            final Type type
    ) {
        super(joinedInternal.dslStoreSuppliers());
        this.name = name + "-store";
        this.joinedInternal = joinedInternal;
        this.windows = windows;
        this.loggingEnabled = joinedInternal.loggingEnabled();
        this.logConfig = new HashMap<>(joinedInternal.logConfig());

        // since this store is configured to retain duplicates, we should
        // not compact, so we override the configuration to make sure that
        // it's just delete (window stores are configured to compact,delete)
        this.logConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

        switch (type) {
            case THIS:
                this.valueSerde = joinedInternal.valueSerde();
                this.storeSupplier = joinedInternal.thisStoreSupplier();
                break;
            case OTHER:
                this.valueSerde = joinedInternal.otherValueSerde();
                this.storeSupplier = joinedInternal.otherStoreSupplier();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    @Override
    public StateStore build() {
        final WindowBytesStoreSupplier supplier = storeSupplier == null
                ? dslStoreSuppliers().windowStore(new DslWindowParams(
                        this.name,
                        Duration.ofMillis(retentionPeriod()),
                        Duration.ofMillis(windows.size()),
                        true,
                        EmitStrategy.onWindowUpdate(),
                        false,
                        false
                ))
                : storeSupplier;

        final StoreBuilder<? extends WindowStore<K, ?>> builder = Stores.windowStoreBuilder(
                supplier,
                joinedInternal.keySerde(),
                valueSerde
        );

        if (joinedInternal.loggingEnabled()) {
            builder.withLoggingEnabled(logConfig);
        } else {
            builder.withLoggingDisabled();
        }

        return builder.build();
    }

    @Override
    public long retentionPeriod() {
        return windows.size() + windows.gracePeriodMs();
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
    public String name() {
        return name;
    }

    @Override
    public boolean isWindowStore() {
        return true;
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
        // caching is never enabled for these stores
        return this;
    }

    @Override
    public StoreFactory withLoggingDisabled() {
        loggingEnabled = false;
        return this;
    }

    @Override
    public boolean isCompatibleWith(final StoreFactory storeFactory) {
        return storeFactory instanceof StreamJoinedStoreFactory
                && ((StreamJoinedStoreFactory<?, ?, ?>) storeFactory).joinedInternal.equals(joinedInternal);
    }
}
