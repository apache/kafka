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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedWindowStoreBuilderTest {
    private static final String TIMESTAMP_STORE_NAME = "Timestamped Store";
    private static final String TIMEORDERED_STORE_NAME = "TimeOrdered Store";
    private static final String STORE_NAME = "name";
    private static final String METRICS_SCOPE = "metricsScope";

    @Mock
    private WindowBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedWindowStore timestampedStore;
    @Mock
    private RocksDBTimeOrderedWindowStore timeOrderedStore;
    private TimestampedWindowStoreBuilder<String, String> builder;
    private boolean isTimeOrderedStore;
    private WindowStore inner;

    public void setUpWithoutInner(final String storeName) {
        isTimeOrderedStore = TIMEORDERED_STORE_NAME.equals(storeName);
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);

        builder = new TimestampedWindowStoreBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());
    }

    @SuppressWarnings("unchecked")
    public void setUp(final String storeName) {
        isTimeOrderedStore = TIMEORDERED_STORE_NAME.equals(storeName);
        inner = isTimeOrderedStore ? timeOrderedStore : timestampedStore;
        when(supplier.get()).thenReturn(inner);
        setUpWithoutInner(storeName);
    }
    
    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldHaveMeteredStoreAsOuterStore(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldHaveChangeLoggingStoreByDefault(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldNotHaveChangeLoggingStoreWhenDisabled(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.equalTo(inner));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldHaveCachingStoreWhenEnabled(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
        if (isTimeOrderedStore) {
            assertThat(wrapped, instanceOf(TimeOrderedCachingWindowStore.class));
        } else {
            assertThat(wrapped, instanceOf(CachingWindowStore.class));
        }
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled(final String storeName) {
        setUp(storeName);
        final TimestampedWindowStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
        if (isTimeOrderedStore) {
            assertThat(caching, instanceOf(TimeOrderedCachingWindowStore.class));
        } else {
            assertThat(caching, instanceOf(CachingWindowStore.class));
        }
        assertThat(changeLogging, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldNotWrapTimestampedByteStore(final String storeName) {
        setUp(storeName);
        when(supplier.get()).thenReturn(new RocksDBTimestampedWindowStore(
            new RocksDBTimestampedSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));

        final TimestampedWindowStore<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(RocksDBTimestampedWindowStore.class));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldWrapPlainKeyValueStoreAsTimestampStore(final String storeName) {
        setUp(storeName);
        when(supplier.get()).thenReturn(new RocksDBWindowStore(
            new RocksDBSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));

        final TimestampedWindowStore<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(WindowToTimestampedWindowByteStoreAdapter.class));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @SuppressWarnings("unchecked")
    @ParameterizedTest
    public void shouldDisableCachingWithRetainDuplicates(final String storeName) {
        setUpWithoutInner(storeName);
        supplier = Stores.persistentTimestampedWindowStore("name", Duration.ofMillis(10L), Duration.ofMillis(10L), true);
        final StoreBuilder<TimestampedWindowStore<String, String>> builder = new TimestampedWindowStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        ).withCachingEnabled();

        builder.build();

        assertFalse(((AbstractStoreBuilder<String, String, TimestampedWindowStore<String, String>>) builder).enableCaching);
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @SuppressWarnings("all")
    @ParameterizedTest
    public void shouldThrowNullPointerIfInnerIsNull(final String storeName) {
        setUpWithoutInner(storeName);
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldThrowNullPointerIfTimeIsNull(final String storeName) {
        setUpWithoutInner(storeName);
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @ValueSource(strings = {TIMESTAMP_STORE_NAME, TIMEORDERED_STORE_NAME})
    @ParameterizedTest
    public void shouldThrowNullPointerIfMetricsScopeIsNull(final String storeName) {
        setUpWithoutInner(storeName);
        when(supplier.metricsScope()).thenReturn(null);
        final Exception e = assertThrows(NullPointerException.class,
            () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertEquals(e.getMessage(), "storeSupplier's metricsScope can't be null");
    }

}
