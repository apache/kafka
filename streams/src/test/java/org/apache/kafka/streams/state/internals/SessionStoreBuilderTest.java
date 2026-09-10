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
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionStoreBuilderTest {

    @Mock
    private SessionBytesStoreSupplier supplier;
    @Mock
    private SessionStore<Bytes, byte[]> inner;
    private SessionStoreBuilder<String, String> builder;

    public void setUp() {
        when(supplier.get()).thenReturn(inner);
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new SessionStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    public void setUpWithoutInner() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new SessionStoreBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        setUp();
        final SessionStore<String, String> store = builder.build();
        assertInstanceOf(MeteredSessionStore.class, store);
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final SessionStore<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, next);
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final SessionStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertEquals(inner, next);
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        setUp();
        final SessionStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredSessionStore.class, store);
        assertInstanceOf(CachingSessionStore.class, wrapped);
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final SessionStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredSessionStore.class, store);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, wrapped);
        assertEquals(inner, ((WrappedStateStore) wrapped).wrapped());
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        setUp();
        final SessionStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertInstanceOf(MeteredSessionStore.class, store);
        assertInstanceOf(CachingSessionStore.class, caching);
        assertInstanceOf(ChangeLoggingSessionBytesStore.class, changeLogging);
        assertEquals(inner, changeLogging.wrapped());
    }

    @Test
    public void shouldThrowNullPointerIfStoreSupplierIsNull() {
        setUpWithoutInner();
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        assertEquals("storeSupplier cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfNameIsNull() {
        setUpWithoutInner();
        when(supplier.name()).thenReturn(null);
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        setUpWithoutInner();
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
        assertEquals("time cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        setUpWithoutInner();
        when(supplier.metricsScope()).thenReturn(null);
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertEquals("storeSupplier's metricsScope can't be null", e.getMessage());
    }

}