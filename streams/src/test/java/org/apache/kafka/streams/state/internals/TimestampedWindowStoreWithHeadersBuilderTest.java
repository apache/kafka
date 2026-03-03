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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedWindowStoreWithHeadersBuilderTest {
    private static final String STORE_NAME = "name";
    private static final String METRICS_SCOPE = "metricsScope";

    @Mock
    private WindowBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedWindowStoreWithHeaders timestampedStoreWithHeaders;

    private TimestampedWindowStoreWithHeadersBuilder<String, String> builder;

    public void setUp() {
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
        when(supplier.get()).thenReturn(timestampedStoreWithHeaders);

        builder = new TimestampedWindowStoreWithHeadersBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, next);
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertSame(timestampedStoreWithHeaders, next);
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
        assertInstanceOf(CachingWindowStore.class, wrapped);
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, wrapped);
        assertSame(timestampedStoreWithHeaders, ((WrappedStateStore) wrapped).wrapped());
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        setUp();
        final TimestampedWindowStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .withCachingEnabled()
            .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
        assertInstanceOf(CachingWindowStore.class, caching);
        assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, changeLogging);
        assertSame(timestampedStoreWithHeaders, changeLogging.wrapped());
    }

    @Test
    public void shouldNotWrapHeadersByteStore() {
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
        when(supplier.get()).thenReturn(new RocksDBTimestampedWindowStoreWithHeaders(
            new RocksDBTimestampedSegmentedBytesStoreWithHeaders(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));

        builder = new TimestampedWindowStoreWithHeadersBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());

        final TimestampedWindowStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(RocksDBTimestampedWindowStoreWithHeaders.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldWrapTimestampedStoreAsHeadersStore() {
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
        when(supplier.get()).thenReturn(new RocksDBTimestampedWindowStore(
            new RocksDBTimestampedSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));

        builder = new TimestampedWindowStoreWithHeadersBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());

        final TimestampedWindowStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(TimestampedToHeadersWindowStoreAdapter.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldDisableCachingWithRetainDuplicates() {
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
        when(supplier.retainDuplicates()).thenReturn(true);
        when(supplier.get()).thenReturn(timestampedStoreWithHeaders);

        builder = new TimestampedWindowStoreWithHeadersBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());

        final TimestampedWindowStoreWithHeaders<String, String> store = builder
            .withCachingEnabled()
            .withLoggingDisabled()
            .build();

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        // Caching should be automatically disabled when retainDuplicates is true
        assertSame(timestampedStoreWithHeaders, wrapped);
    }

    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreWithHeadersBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }
}
