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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TimestampedKeyValueStoreBuilderTest {

    @Mock
    private KeyValueBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedStore inner;
    private TimestampedKeyValueStoreBuilder<String, String> builder;

    @Before
    public void setUp() {
        when(supplier.get()).thenReturn(inner);
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new TimestampedKeyValueStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final TimestampedKeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final TimestampedKeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final TimestampedKeyValueStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final TimestampedKeyValueStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(wrapped, instanceOf(CachingKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final TimestampedKeyValueStore<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        final TimestampedKeyValueStore<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .withCachingEnabled()
            .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldNotWrapTimestampedByteStore() {
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("name", "metrics-scope"));

        final TimestampedKeyValueStore<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(RocksDBTimestampedStore.class));
    }

    @Test
    public void shouldWrapPlainKeyValueStoreAsTimestampStore() {
        when(supplier.get()).thenReturn(new RocksDBStore("name", "metrics-scope"));

        final TimestampedKeyValueStore<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(KeyValueToTimestampedKeyValueByteStoreAdapter.class));
    }

    @SuppressWarnings("all")
    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedKeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldNotThrowNullPointerIfKeySerdeIsNull() {
        // does not throw
        new TimestampedKeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test
    public void shouldNotThrowNullPointerIfValueSerdeIsNull() {
        // does not throw
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        assertThrows(NullPointerException.class, () -> new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        when(supplier.metricsScope()).thenReturn(null);

        final Exception e = assertThrows(NullPointerException.class,
            () -> new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("storeSupplier's metricsScope can't be null"));
    }

}