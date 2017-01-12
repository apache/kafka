/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBSessionStoreSupplierTest {

    @Test
    public void shouldRegisterWithLoggingEnabledWhenStoreLogged() throws Exception {
        final SessionStore sessionStore = createStore(true, false);
        sessionStore.init(new MockProcessorContext(new StateSerdes<>("", Serdes.String(), Serdes.String()), new NoOpRecordCollector()) {
            @Override
            public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
                assertTrue("store should be registering as loggingEnabled", loggingEnabled);
            }
        }, sessionStore);
    }

    @Test
    public void shouldRegisterWithLoggingEnabledWhenStoreLoggedAndCached() throws Exception {
        final SessionStore sessionStore = createStore(true, true);
        sessionStore.init(new MockProcessorContext(new StateSerdes<>("", Serdes.String(), Serdes.String()), new NoOpRecordCollector()) {
            @Override
            public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
                assertTrue("store should be registering as loggingEnabled", loggingEnabled);
            }

            @Override
            public ThreadCache getCache() {
                return new ThreadCache("name", 0, new MockStreamsMetrics(new Metrics()));
            }
        }, sessionStore);
    }

    @Test
    public void shouldRegisterWithLoggingDisabledWhenStoreNotLogged() throws Exception {
        final SessionStore sessionStore = createStore(false, false);
        sessionStore.init(new MockProcessorContext(new StateSerdes<>("", Serdes.String(), Serdes.String()), new NoOpRecordCollector()) {
            @Override
            public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
                assertFalse("store should not be registering as loggingEnabled", loggingEnabled);
            }
        }, sessionStore);
    }

    private SessionStore createStore(final boolean logged, final boolean cached) {
        return new RocksDBSessionStoreSupplier<>("name",
                                                10L,
                                                Serdes.String(),
                                                Serdes.String(),
                                                logged,
                                                Collections.<String, String>emptyMap(),
                                                cached).get();
    }

}