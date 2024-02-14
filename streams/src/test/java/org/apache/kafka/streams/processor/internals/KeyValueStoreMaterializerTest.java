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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.InternalNameProvider;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingTimestampedKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingVersionedKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredVersionedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KeyValueStoreMaterializerTest {

    private static final String STORE_PREFIX = "prefix";
    private static final String STORE_NAME = "name";
    private static final String METRICS_SCOPE = "metricScope";

    @Mock
    private InternalNameProvider nameProvider;
    @Mock
    private KeyValueBytesStoreSupplier keyValueStoreSupplier;
    @Mock
    private VersionedBytesStoreSupplier versionedStoreSupplier;
    private final KeyValueStore<Bytes, byte[]> innerKeyValueStore = new InMemoryKeyValueStore(STORE_NAME);
    @Mock
    private VersionedBytesStore innerVersionedStore;
    @Mock
    private StreamsConfig streamsConfig;

    @Before
    public void setUp() {
        when(keyValueStoreSupplier.get()).thenReturn(innerKeyValueStore);
        when(keyValueStoreSupplier.name()).thenReturn(STORE_NAME);
        when(keyValueStoreSupplier.metricsScope()).thenReturn(METRICS_SCOPE);

        when(innerVersionedStore.name()).thenReturn(STORE_NAME);
        when(versionedStoreSupplier.get()).thenReturn(innerVersionedStore);
        when(versionedStoreSupplier.name()).thenReturn(STORE_NAME);
        when(versionedStoreSupplier.metricsScope()).thenReturn(METRICS_SCOPE);

        doReturn(BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class)
                .when(streamsConfig).getClass(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG);
    }

    @Test
    public void shouldCreateTimestampedBuilderWithCachingAndLoggingEnabledByDefault() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as("store"), nameProvider, STORE_PREFIX);

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final StateStore logging = caching.wrapped();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(logging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateDefaultTimestampedBuilderWithCachingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled(), nameProvider, STORE_PREFIX
        );

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(logging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateDefaultTimestampedBuilderWithLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withLoggingDisabled(), nameProvider, STORE_PREFIX
        );

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(caching.wrapped(), not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateDefaultTimestampedBuilderWithCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled().withLoggingDisabled(), nameProvider, STORE_PREFIX
        );

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(wrapped, not(instanceOf(CachingKeyValueStore.class)));
        assertThat(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateTimestampedStoreWithProvidedSupplierAndCachingAndLoggingEnabledByDefault() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(keyValueStoreSupplier), nameProvider, STORE_PREFIX);

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final StateStore logging = caching.wrapped();
        assertThat(innerKeyValueStore.name(), equalTo(store.name()));
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(logging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateTimestampedStoreWithProvidedSupplierAndCachingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(keyValueStoreSupplier).withCachingDisabled(), nameProvider, STORE_PREFIX);

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(innerKeyValueStore.name(), equalTo(store.name()));
        assertThat(logging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateTimestampedStoreWithProvidedSupplierAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(keyValueStoreSupplier).withLoggingDisabled(), nameProvider, STORE_PREFIX);

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(innerKeyValueStore.name(), equalTo(store.name()));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(caching.wrapped(), not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateTimestampedStoreWithProvidedSupplierAndCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(keyValueStoreSupplier).withCachingDisabled().withLoggingDisabled(), nameProvider, STORE_PREFIX);

        final TimestampedKeyValueStore<String, String> store = getTimestampedStore(materialized);

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(innerKeyValueStore.name(), equalTo(store.name()));
        assertThat(wrapped, not(instanceOf(CachingKeyValueStore.class)));
        assertThat(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateVersionedStoreWithProvidedSupplierAndLoggingEnabledByDefault() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(versionedStoreSupplier), nameProvider, STORE_PREFIX);

        final VersionedKeyValueStore<String, String> store = getVersionedStore(materialized);

        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final StateStore inner = logging.wrapped();
        assertThat(innerVersionedStore.name(), equalTo(store.name()));
        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        assertThat(logging, instanceOf(ChangeLoggingVersionedKeyValueBytesStore.class));
        assertThat(innerVersionedStore, equalTo(inner));
    }

    @Test
    public void shouldCreateVersionedStoreWithProvidedSupplierAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(versionedStoreSupplier).withLoggingDisabled(), nameProvider, STORE_PREFIX);

        final VersionedKeyValueStore<String, String> store = getVersionedStore(materialized);

        final StateStore inner = ((WrappedStateStore) store).wrapped();
        assertThat(innerVersionedStore.name(), equalTo(store.name()));
        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        assertThat(innerVersionedStore, equalTo(inner));
    }

    @Test
    public void shouldNotBuildVersionedStoreWithCachingEvenIfExplicitlySet() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.<String, String>as(versionedStoreSupplier).withCachingEnabled(), nameProvider, STORE_PREFIX);

        final VersionedKeyValueStore<String, String> store = getVersionedStore(materialized);

        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final StateStore inner = logging.wrapped();
        assertThat(innerVersionedStore.name(), equalTo(store.name()));
        assertThat(store, instanceOf(MeteredVersionedKeyValueStore.class));
        assertThat(logging, instanceOf(ChangeLoggingVersionedKeyValueBytesStore.class));
        assertThat(innerVersionedStore, equalTo(inner));
    }

    @SuppressWarnings("unchecked")
    private TimestampedKeyValueStore<String, String> getTimestampedStore(
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized) {
        final KeyValueStoreMaterializer<String, String> materializer = new KeyValueStoreMaterializer<>(materialized);
        materializer.configure(streamsConfig);
        return (TimestampedKeyValueStore<String, String>) ((StoreFactory) materializer).build();
    }

    @SuppressWarnings("unchecked")
    private VersionedKeyValueStore<String, String> getVersionedStore(
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized) {
        final KeyValueStoreMaterializer<String, String> materializer = new KeyValueStoreMaterializer<>(materialized);
        materializer.configure(streamsConfig);
        return (VersionedKeyValueStore<String, String>) ((StoreFactory) materializer).build();
    }
}