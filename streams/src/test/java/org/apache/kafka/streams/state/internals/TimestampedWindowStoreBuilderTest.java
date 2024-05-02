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

import java.time.Duration;
import java.util.Collection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@SuppressWarnings("this-escape")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TimestampedWindowStoreBuilderTest {
    private static final String TIMESTAMP_STORE_NAME = "Timestamped Store";
    private static final String TIMEORDERED_STORE_NAME = "TimeOrdered Store";

    @Mock
    private WindowBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedWindowStore timestampedStore;
    @Mock
    private RocksDBTimeOrderedWindowStore timeOrderedStore;
    private TimestampedWindowStoreBuilder<String, String> builder;
    private boolean isTimeOrderedStore;
    private WindowStore inner;

    @Parameter
    public String storeName;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {TIMESTAMP_STORE_NAME},
            {TIMEORDERED_STORE_NAME}
        });
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        isTimeOrderedStore = TIMEORDERED_STORE_NAME.equals(storeName);
        inner = isTimeOrderedStore ? timeOrderedStore : timestampedStore;
        when(supplier.get()).thenReturn(inner);
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new TimestampedWindowStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final TimestampedWindowStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final TimestampedWindowStore<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final TimestampedWindowStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final TimestampedWindowStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
        if (isTimeOrderedStore) {
            assertThat(wrapped, instanceOf(TimeOrderedCachingWindowStore.class));
        } else {
            assertThat(wrapped, instanceOf(CachingWindowStore.class));
        }
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final TimestampedWindowStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedWindowStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
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

    @Test
    public void shouldNotWrapTimestampedByteStore() {
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

    @Test
    public void shouldWrapPlainKeyValueStoreAsTimestampStore() {
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

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDisableCachingWithRetainDuplicates() {
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

    @SuppressWarnings("all")
    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        when(supplier.metricsScope()).thenReturn(null);
        final Exception e = assertThrows(NullPointerException.class,
            () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertEquals(e.getMessage(), "storeSupplier's metricsScope can't be null");
    }

}
