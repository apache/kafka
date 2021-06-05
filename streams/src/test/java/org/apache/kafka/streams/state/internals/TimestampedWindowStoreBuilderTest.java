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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

@RunWith(EasyMockRunner.class)
public class TimestampedWindowStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private WindowBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private RocksDBTimestampedWindowStore inner;
    private TimestampedWindowStoreBuilder<String, String> builder;

    @Before
    public void setUp() {
        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        expect(supplier.metricsScope()).andReturn("metricScope");
        expect(inner.persistent()).andReturn(true).anyTimes();
        replay(supplier, inner);

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
        assertThat(wrapped, instanceOf(CachingWindowStore.class));
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
        assertThat(caching, instanceOf(CachingWindowStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingTimestampedWindowBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldNotWrapTimestampedByteStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBTimestampedWindowStore(
            new RocksDBTimestampedSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        final TimestampedWindowStore<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(RocksDBTimestampedWindowStore.class));
    }

    @Test
    public void shouldWrapPlainKeyValueStoreAsTimestampStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBWindowStore(
            new RocksDBSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

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
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime()));
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBTimestampedWindowStore(
            new RocksDBTimestampedSegmentedBytesStore(
                "name",
                null,
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));
        expect(supplier.name()).andReturn("name");
        replay(supplier);
        final Exception e = assertThrows(NullPointerException.class,
            () -> new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("storeSupplier's metricsScope can't be null"));
    }

}