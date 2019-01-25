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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.KeyValueToKeyValueWithUnknownTimestampByteStore;
import org.apache.kafka.streams.state.internals.MeteredKeyValueWithTimestampStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

@RunWith(EasyMockRunner.class)
public class KeyValueWithTimestampStoreMaterializerTest {

    private final String storePrefix = "prefix";
    @Mock(type = MockType.NICE)
    private InternalNameProvider nameProvider;

    @Test
    public void shouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as("store"), nameProvider, storePrefix);

        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> store =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
        final StateStore logging = caching.wrappedStore();
        assertThat(store, instanceOf(MeteredKeyValueWithTimestampStore.class));
        assertThat(caching, instanceOf(CachedStateStore.class));
        assertThat(logging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithCachingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled(), nameProvider, storePrefix
        );
        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> store =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
        assertThat(logging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withLoggingDisabled(), nameProvider, storePrefix
        );
        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> store =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
        assertThat(caching, instanceOf(CachedStateStore.class));
        assertThat(caching.wrappedStore(), not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled().withLoggingDisabled(), nameProvider, storePrefix
        );
        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> store =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final StateStore wrapped = ((WrappedStateStore) store).wrappedStore();
        assertThat(wrapped, not(instanceOf(CachedStateStore.class)));
        assertThat(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateKeyValueStoreWithProvidedInMemoryInnerStore() {
        final KeyValueBytesStoreSupplier supplier = EasyMock.createNiceMock(KeyValueBytesStoreSupplier.class);
        final KeyValueStore<Bytes, byte[]> store = EasyMock.createNiceMock(KeyValueStore.class);
        EasyMock.expect(supplier.name()).andReturn("name").anyTimes();
        EasyMock.expect(supplier.get()).andReturn(store).anyTimes();
        EasyMock.expect(store.persistent()).andReturn(false);
        EasyMock.replay(supplier, store);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), nameProvider, storePrefix);
        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> built =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final StateStore inner = ((WrappedStateStore) built).inner();

        assertThat(inner, CoreMatchers.equalTo(store));
    }

    @Test
    public void shouldCreateKeyValueStoreProxyWithProvidedPersistentInnerStore() {
        final KeyValueBytesStoreSupplier supplier = EasyMock.createNiceMock(KeyValueBytesStoreSupplier.class);
        final KeyValueStore<Bytes, byte[]> store = EasyMock.createNiceMock(KeyValueStore.class);
        EasyMock.expect(supplier.name()).andReturn("name").anyTimes();
        EasyMock.expect(supplier.get()).andReturn(store).anyTimes();
        EasyMock.expect(store.persistent()).andReturn(true);
        EasyMock.replay(supplier, store);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), nameProvider, storePrefix);
        final KeyValueWithTimestampStoreMaterializer<String, String> materializer = new KeyValueWithTimestampStoreMaterializer<>(materialized);
        final StoreBuilder<KeyValueStore<String, String>> builder = materializer.materialize();
        final KeyValueStore<String, ValueAndTimestamp<String>> built =
            ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade<String, String>) builder.build()).inner;
        final StateStore inner = ((WrappedStateStore) built).inner();

        assertThat(inner, instanceOf(KeyValueToKeyValueWithUnknownTimestampByteStore.class));
    }
}
