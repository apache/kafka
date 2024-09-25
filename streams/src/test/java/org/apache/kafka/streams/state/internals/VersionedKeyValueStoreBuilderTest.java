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
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class VersionedKeyValueStoreBuilderTest {

    private static final String STORE_NAME = "versioned-store";
    private static final String METRICS_SCOPE = "metrics-scope";

    @Mock
    private VersionedBytesStoreSupplier supplier;
    @Mock
    private VersionedBytesStore inner;

    private VersionedKeyValueStoreBuilder<String, String> builder;

    private void setUpWithoutInner() {
        when(supplier.name()).thenReturn(STORE_NAME);
        when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);

        builder = new VersionedKeyValueStoreBuilder<>(
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
        final VersionedKeyValueStore<String, String> store = builder.build();

        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final VersionedKeyValueStore<String, String> store = builder.build();

        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingVersionedKeyValueBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final VersionedKeyValueStore<String, String> store = builder
            .withLoggingDisabled()
            .build();

        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, equalTo(inner));

    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final VersionedKeyValueStore<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .build();

        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingVersionedKeyValueBytesStore.class));
        assertThat(((WrappedStateStore) next).wrapped(), equalTo(inner));
    }

    @Test
    public void shouldThrowWhenCachingEnabled() {
        setUpWithoutInner();
        assertThrows(IllegalStateException.class, () -> builder.withCachingEnabled());
    }

    @SuppressWarnings("all")
    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () -> new VersionedKeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldNotThrowNullPointerIfKeySerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new VersionedKeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test
    public void shouldNotThrowNullPointerIfValueSerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new VersionedKeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () -> new VersionedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfNameIsNull() {
        setUpWithoutInner();
        when(supplier.name()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> new VersionedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        setUpWithoutInner();
        when(supplier.metricsScope()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> new VersionedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
    }
}