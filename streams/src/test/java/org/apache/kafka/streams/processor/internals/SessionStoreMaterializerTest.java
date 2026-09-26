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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.internals.InternalNameProvider;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.SessionStoreMaterializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.CachingSessionStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingSessionBytesStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingSessionBytesStoreWithHeaders;
import org.apache.kafka.streams.state.internals.InMemorySessionStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionStoreMaterializerTest {

    private static final String STORE_PREFIX = "prefix";
    private static final String STORE_NAME = "name";
    private static final long GAP_SIZE_MS = 10000L;

    @Mock
    private InternalNameProvider nameProvider;
    @Mock
    private StreamsConfig streamsConfig;

    private final SessionStore<Bytes, byte[]> innerSessionStore =
        new InMemorySessionStore(STORE_NAME, 60000L, "metricScope");

    private SessionWindows windows;
    private EmitStrategy emitStrategy;

    @BeforeEach
    public void setUp() {
        windows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(GAP_SIZE_MS));
        emitStrategy = EmitStrategy.onWindowUpdate();

        doReturn(emptyMap())
            .when(streamsConfig).originals();
        doReturn(new BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers())
                .when(streamsConfig).getConfiguredInstance(
                    StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG,
                    DslStoreSuppliers.class,
                    emptyMap()
            );
        lenient().doReturn("timestamped")
                .when(streamsConfig).getString(StreamsConfig.DSL_STORE_FORMAT_CONFIG);
    }

    private final class HeadersStoreSupplier implements SessionBytesStoreSupplier, HeadersBytesStoreSupplier {
        @Override
        public String name() {
            return STORE_NAME;
        }

        @Override
        public SessionStore<Bytes, byte[]> get() {
            return innerSessionStore;
        }

        @Override
        public String metricsScope() {
            return "metricScope";
        }

        @Override
        public long segmentIntervalMs() {
            return 0;
        }

        @Override
        public long retentionPeriod() {
            return 0;
        }
    }

    @Test
    public void shouldCreateHeadersBuilderWithCachingAndLoggingEnabledByDefault() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as("store"), nameProvider, STORE_PREFIX);

        final SessionStore<String, String> store = getSessionStore(materialized);
        final WrappedStateStore<?, ?, ?> caching = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        final StateStore logging = caching.wrapped();

        assertFalse(store instanceof SessionStoreWithHeaders);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, logging);
    }

    @Test
    public void shouldCreateHeadersBuilderWithCachingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store").withCachingDisabled(), nameProvider, STORE_PREFIX
        );

        final SessionStore<String, String> store = getSessionStore(materialized);

        final WrappedStateStore<?, ?, ?> logging = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertFalse(store instanceof SessionStoreWithHeaders);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, logging);
    }

    @Test
    public void shouldCreateHeadersBuilderWithLoggingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store").withLoggingDisabled(), nameProvider, STORE_PREFIX
        );

        final SessionStore<String, String> store = getSessionStore(materialized);

        final WrappedStateStore<?, ?, ?> caching = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertFalse(store instanceof SessionStoreWithHeaders);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertFalse(caching.wrapped() instanceof ChangeLoggingSessionBytesStore);
    }

    @Test
    public void shouldCreateHeadersBuilderWithCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store").withCachingDisabled().withLoggingDisabled(), nameProvider, STORE_PREFIX
        );

        final SessionStore<String, String> store = getSessionStore(materialized);

        final StateStore wrapped = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertFalse(store instanceof SessionStoreWithHeaders);
        assertFalse(wrapped instanceof CachingSessionStore);
        assertFalse(wrapped instanceof ChangeLoggingSessionBytesStore);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierAndCachingAndLoggingEnabledByDefault() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(new HeadersStoreSupplier()), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final WrappedStateStore<?, ?, ?> caching = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        final StateStore logging = caching.wrapped();
        assertEquals(innerSessionStore.name(), store.name());
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, logging);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierAndCachingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(new HeadersStoreSupplier()).withCachingDisabled(), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final WrappedStateStore<?, ?, ?> logging = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertEquals(innerSessionStore.name(), store.name());
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, logging);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierAndLoggingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(new HeadersStoreSupplier()).withLoggingDisabled(), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final WrappedStateStore<?, ?, ?> caching = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertEquals(innerSessionStore.name(), store.name());
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertFalse(caching.wrapped() instanceof ChangeLoggingSessionBytesStoreWithHeaders);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierAndCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(new HeadersStoreSupplier()).withCachingDisabled().withLoggingDisabled(), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final StateStore wrapped = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertEquals(innerSessionStore.name(), store.name());
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertFalse(wrapped instanceof CachingSessionStore);
        assertFalse(wrapped instanceof ChangeLoggingSessionBytesStoreWithHeaders);
    }

    @Test
    public void shouldCreateSessionStoreWithOnWindowCloseByDefault() {
        emitStrategy = EmitStrategy.onWindowClose();

        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store")
                .withCachingDisabled(), nameProvider, STORE_PREFIX);

        final SessionStore<String, String> store = getSessionStore(materialized);

        final WrappedStateStore<?, ?, ?> logging = (WrappedStateStore<?, ?, ?>) ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertFalse(store instanceof SessionStoreWithHeaders);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, logging);
    }

    @Test
    public void shouldCreateSessionStoreWithOnWindowCloseAndAutoDisableCaching() {
        emitStrategy = EmitStrategy.onWindowClose();

        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as("store"), nameProvider, STORE_PREFIX);

        final SessionStore<String, String> store = getSessionStore(materialized);

        final StateStore wrapped = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertFalse(store instanceof SessionStoreWithHeaders);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, wrapped);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierOnWindowClose() {
        emitStrategy = EmitStrategy.onWindowClose();

        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(new HeadersStoreSupplier())
                .withCachingDisabled(), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final StateStore wrapped = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, wrapped);
    }

    @Test
    public void shouldCreateHeadersStoreWithProvidedSupplierOnWindowCloseAndAutoDisableCaching() {
        emitStrategy = EmitStrategy.onWindowClose();

        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(new HeadersStoreSupplier()), nameProvider, STORE_PREFIX);

        final SessionStoreWithHeaders<String, String> store = getHeadersStore(materialized);

        final StateStore wrapped = ((WrappedStateStore<?, ?, ?>) store).wrapped();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, wrapped);
    }

    @SuppressWarnings("unchecked")
    private SessionStore<String, String> getSessionStore(
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized
    ) {
        final SessionStoreMaterializer<String, String> materializer =
            new SessionStoreMaterializer<>(materialized, windows, emitStrategy);
        materializer.configure(streamsConfig);
        return (SessionStore<String, String>) materializer.builder().build();
    }

    @SuppressWarnings("unchecked")
    private SessionStoreWithHeaders<String, String> getHeadersStore(
        final MaterializedInternal<String, String, SessionStore<Bytes, byte[]>> materialized
    ) {
        final SessionStoreMaterializer<String, String> materializer =
            new SessionStoreMaterializer<>(materialized, windows, emitStrategy);
        materializer.configure(streamsConfig);
        return (SessionStoreWithHeaders<String, String>) materializer.builder().build();
    }
}
