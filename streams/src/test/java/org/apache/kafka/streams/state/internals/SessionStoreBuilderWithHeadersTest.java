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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionStoreBuilderWithHeadersTest {

    @Mock
    private SessionBytesStoreSupplier supplier;
    @Mock
    private RocksDBSessionStoreWithHeaders inner;
    private SessionStoreBuilderWithHeaders<String, String> builder;

    private void setUpWithoutInner() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new SessionStoreBuilderWithHeaders<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    private void setUp() {
        when(supplier.get()).thenReturn(inner);
        setUpWithoutInner();
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, next);
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertSame(inner, next);
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        when(supplier.segmentIntervalMs()).thenReturn(30_000L);
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(CachingSessionStore.class, wrapped);
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, wrapped);
        assertSame(inner, ((WrappedStateStore) wrapped).wrapped());
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        when(supplier.segmentIntervalMs()).thenReturn(30_000L);
        setUp();
        final SessionStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .withCachingEnabled()
            .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, changeLogging);
        assertSame(inner, changeLogging.wrapped());
    }

    @Test
    public void shouldNotWrapHeadersByteStore() {
        setUp();
        // inner already implements HeadersBytesStore, so no adapter wrapping is needed
        final SessionStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertSame(inner, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldWrapNonHeadersSessionStoreWithAdapter() {
        final SessionStore<Bytes, byte[]> plainSessionStore =
            new RocksDBSessionStore(new RocksDBSegmentedBytesStore(
                "name", "metric-scope", 60_000L, 30_000L, new SessionKeySchema()));

        when(supplier.get()).thenReturn(plainSessionStore);
        setUpWithoutInner();

        final SessionStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(SessionToHeadersStoreAdapter.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldWrapInMemorySessionStoreWithMarker() {
        final InMemorySessionStore inMemoryStore = new InMemorySessionStore("name", 60_000L, "metricScope");
        when(supplier.get()).thenReturn(inMemoryStore);
        setUpWithoutInner();

        final SessionStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(HeadersBytesStore.class, wrapped);
    }

    @Test
    public void shouldThrowNullPointerIfStoreSupplierIsNull() {
        final Exception e = assertThrows(NullPointerException.class,
            () -> new SessionStoreBuilderWithHeaders<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        assertTrue(e.getMessage().contains("storeSupplier cannot be null"));
    }

    @Test
    public void shouldNotThrowNullPointerIfKeySerdeIsNull() {
        setUpWithoutInner();
        new SessionStoreBuilderWithHeaders<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test
    public void shouldNotThrowNullPointerIfValueSerdeIsNull() {
        setUpWithoutInner();
        new SessionStoreBuilderWithHeaders<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        assertThrows(NullPointerException.class,
            () -> new SessionStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn(null);

        final Exception e = assertThrows(NullPointerException.class,
            () -> new SessionStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertTrue(e.getMessage().contains("storeSupplier's metricsScope can't be null"));
    }
}
