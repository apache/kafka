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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.DslSessionParams;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class SessionStoreMaterializer<K, V> extends MaterializedStoreFactory<K, V, SessionStore<Bytes, byte[]>> {

    private final MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materialized;
    private final SessionWindows sessionWindows;
    private final EmitStrategy emitStrategy;
    private final long retentionPeriod;

    public SessionStoreMaterializer(
            final MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materialized,
            final SessionWindows sessionWindows,
            final EmitStrategy emitStrategy
    ) {
        super(materialized);
        this.materialized = materialized;
        this.sessionWindows = sessionWindows;
        this.emitStrategy = emitStrategy;

        retentionPeriod = retentionPeriod();
        if ((sessionWindows.inactivityGap() + sessionWindows.gracePeriodMs()) > retentionPeriod) {
            throw new IllegalArgumentException("The retention period of the session store "
                    + materialized.storeName()
                    + " must be no smaller than the session inactivity gap plus the"
                    + " grace period."
                    + " Got gap=[" + sessionWindows.inactivityGap() + "],"
                    + " grace=[" + sessionWindows.gracePeriodMs() + "],"
                    + " retention=[" + retentionPeriod + "]");
        }
    }

    @Override
    public StateStore build() {
        final SessionBytesStoreSupplier supplier = materialized.storeSupplier() == null
                ? dslStoreSuppliers().sessionStore(new DslSessionParams(
                        materialized.storeName(),
                        Duration.ofMillis(retentionPeriod),
                        emitStrategy))
                : (SessionBytesStoreSupplier) materialized.storeSupplier();

        final StoreBuilder<SessionStore<K, V>> builder = Stores.sessionStoreBuilder(
                supplier,
                materialized.keySerde(),
                materialized.valueSerde()
        );

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        // do not enable cache if the emit final strategy is used
        if (materialized.cachingEnabled() && emitStrategy.type() != EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
            builder.withCachingEnabled();
        } else {
            builder.withCachingDisabled();
        }

        return builder.build();
    }

    @Override
    public final long retentionPeriod() {
        return materialized.retention() != null
                ? materialized.retention().toMillis()
                : sessionWindows.inactivityGap() + sessionWindows.gracePeriodMs();
    }

    @Override
    public long historyRetention() {
        throw new IllegalStateException(
                "historyRetention is not supported when not a versioned store");
    }

    @Override
    public boolean isWindowStore() {
        return true;
    }

    @Override
    public boolean isVersionedStore() {
        return false;
    }

}
