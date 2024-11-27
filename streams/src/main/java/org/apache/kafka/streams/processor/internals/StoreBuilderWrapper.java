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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link StoreFactory} which wraps an instantiated
 * {@link StoreBuilder}. This should be used in situations where the user
 * directly builds and supplies the topology with a {@code StoreBuilder}
 * instead of providing a {@link org.apache.kafka.streams.state.StoreSupplier}.
 */
public class StoreBuilderWrapper implements StoreFactory {

    private final StoreBuilder<?> builder;
    private final Set<String> connectedProcessorNames = new HashSet<>();

    public static StoreFactory wrapStoreBuilder(final StoreBuilder<?> builder) {
        if (builder instanceof FactoryWrappingStoreBuilder) {
            return ((FactoryWrappingStoreBuilder<?>) builder).storeFactory();
        } else {
            return new StoreBuilderWrapper(builder);
        }
    }

    private StoreBuilderWrapper(final StoreBuilder<?> builder) {
        this.builder = builder;
    }

    @Override
    public StateStore build() {
        return builder.build();
    }

    @Override
    public long retentionPeriod() {
        if (builder instanceof WindowStoreBuilder) {
            return ((WindowStoreBuilder<?, ?>) builder).retentionPeriod();
        } else if (builder instanceof TimestampedWindowStoreBuilder) {
            return ((TimestampedWindowStoreBuilder<?, ?>) builder).retentionPeriod();
        } else if (builder instanceof SessionStoreBuilder) {
            return ((SessionStoreBuilder<?, ?>) builder).retentionPeriod();
        } else {
            throw new IllegalStateException(
                    "retentionPeriod is not supported when not a window store");
        }
    }

    @Override
    public long historyRetention() {
        if (builder instanceof VersionedKeyValueStoreBuilder) {
            return ((VersionedKeyValueStoreBuilder<?, ?>) builder).historyRetention();
        } else {
            throw new IllegalStateException(
                    "historyRetention is not supported when not a versioned store");
        }
    }

    @Override
    public Set<String> connectedProcessorNames() {
        return connectedProcessorNames;
    }

    @Override
    public boolean loggingEnabled() {
        return builder.loggingEnabled();
    }

    @Override
    public String name() {
        return builder.name();
    }

    @Override
    public boolean isWindowStore() {
        return builder instanceof WindowStoreBuilder
                || builder instanceof TimestampedWindowStoreBuilder
                || builder instanceof SessionStoreBuilder;
    }

    @Override
    public boolean isVersionedStore() {
        return builder instanceof VersionedKeyValueStoreBuilder;
    }

    @Override
    public Map<String, String> logConfig() {
        return builder.logConfig();
    }

    @Override
    public StoreFactory withCachingDisabled() {
        builder.withCachingDisabled();
        return this;
    }

    @Override
    public StoreFactory withLoggingDisabled() {
        builder.withLoggingDisabled();
        return this;
    }

    @Override
    public boolean isCompatibleWith(final StoreFactory storeFactory) {
        return storeFactory instanceof StoreBuilderWrapper
                && builder.equals(((StoreBuilderWrapper) storeFactory).builder);
    }
}
