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

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.LeftOrRightValueSerde;
import org.apache.kafka.streams.state.internals.ListValueStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSideSerde;

public class OuterStreamJoinStoreFactory<K, V1, V2> extends AbstractConfigurableStoreFactory {

    private final String name;
    private final StreamJoinedInternal<K, V1, V2> streamJoined;
    private final JoinWindows windows;
    private boolean loggingEnabled;

    public enum Type {
        RIGHT,
        LEFT
    }

    public OuterStreamJoinStoreFactory(
            final String name,
            final StreamJoinedInternal<K, V1, V2> streamJoined,
            final JoinWindows windows,
            final Type type
    ) {
        super(streamJoined.dslStoreSuppliers());
        this.name = buildOuterJoinWindowStoreName(streamJoined, name, type) + "-store";
        this.streamJoined = streamJoined;
        this.windows = windows;
        this.loggingEnabled = streamJoined.loggingEnabled();
    }

    @Override
    public StateStore build() {
        final Duration retentionPeriod = Duration.ofMillis(retentionPeriod());
        final Duration windowSize = Duration.ofMillis(windows.size());
        final String rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        final String wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = validateMillisecondDuration(windowSize, wsMsgPrefix);

        if (retentionMs < 0L) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        if (windowSizeMs < 0L) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }
        if (windowSizeMs > retentionMs) {
            throw new IllegalArgumentException("The retention period of the window store "
                    + name + " must be no smaller than its window size. Got size=["
                    + windowSizeMs + "], retention=[" + retentionMs + "]");
        }

        final TimestampedKeyAndJoinSideSerde<K> timestampedKeyAndJoinSideSerde = new TimestampedKeyAndJoinSideSerde<>(streamJoined.keySerde());
        final LeftOrRightValueSerde<V1, V2> leftOrRightValueSerde = new LeftOrRightValueSerde<>(streamJoined.valueSerde(), streamJoined.otherValueSerde());

        // TODO: we should allow for configuration of this store explicitly instead of assuming that it should
        // share the same type of store as thisStoreSupplier
        final boolean useInMemoryStore = streamJoined.thisStoreSupplier() != null
                && streamJoined.thisStoreSupplier() instanceof InMemoryWindowBytesStoreSupplier;
        final KeyValueBytesStoreSupplier supplier = useInMemoryStore
                ? Stores.inMemoryKeyValueStore(name)
                : dslStoreSuppliers().keyValueStore(new DslKeyValueParams(name));

        final StoreBuilder<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<V1, V2>>>
                builder =
                new ListValueStoreBuilder<>(
                        supplier,
                        timestampedKeyAndJoinSideSerde,
                        leftOrRightValueSerde,
                        Time.SYSTEM
                );

        if (loggingEnabled) {
            builder.withLoggingEnabled(streamJoined.logConfig());
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
        return false;
    }

    @Override
    public boolean isVersionedStore() {
        return false;
    }

    @Override
    public Map<String, String> logConfig() {
        return streamJoined.logConfig();
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
    public boolean isCompatibleWith(final StoreFactory storeFactory) {
        return (storeFactory instanceof OuterStreamJoinStoreFactory)
                && ((OuterStreamJoinStoreFactory<?, ?, ?>) storeFactory).streamJoined.equals(streamJoined);
    }

    private static <K, V1, V2> String buildOuterJoinWindowStoreName(
            final StreamJoinedInternal<K, V1, V2> streamJoinedInternal,
            final String joinThisGeneratedName,
            final Type type
    ) {
        final String outerJoinSuffix = (type == Type.RIGHT) ? "-outer-shared-join" : "-left-shared-join";

        if (streamJoinedInternal.thisStoreSupplier() != null && !streamJoinedInternal.thisStoreSupplier().name().isEmpty()) {
            return streamJoinedInternal.thisStoreSupplier().name() + outerJoinSuffix;
        } else if (streamJoinedInternal.storeName() != null) {
            return streamJoinedInternal.storeName() + outerJoinSuffix;
        } else {
            return KStreamImpl.OUTERSHARED_NAME
                    + joinThisGeneratedName.substring(
                    type == Type.RIGHT
                            ? KStreamImpl.OUTERTHIS_NAME.length()
                            : KStreamImpl.JOINTHIS_NAME.length());
        }
    }
}
