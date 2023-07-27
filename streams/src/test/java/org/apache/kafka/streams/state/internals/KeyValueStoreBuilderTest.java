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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
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
public class KeyValueStoreBuilderTest {

    @Mock
    private KeyValueBytesStoreSupplier supplier;
    @Mock
    private KeyValueStore<Bytes, byte[]> inner;
    private KeyValueStoreBuilder<String, String> builder;

    @Before
    public void setUp() {
        when(supplier.get()).thenReturn(inner);
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");
        builder = new KeyValueStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final KeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final KeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredKeyValueStore.class));
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final KeyValueStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final KeyValueStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredKeyValueStore.class));
        assertThat(wrapped, instanceOf(CachingKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final KeyValueStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredKeyValueStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingKeyValueBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        final KeyValueStore<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertThat(store, instanceOf(MeteredKeyValueStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    @SuppressWarnings("all")
    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        assertThrows(NullPointerException.class, () -> new KeyValueStoreBuilder<>(null, Serdes.String(),
            Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        assertThrows(NullPointerException.class, () -> new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn(null);

        final Exception e = assertThrows(NullPointerException.class,
            () -> new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("storeSupplier's metricsScope can't be null"));
    }

}