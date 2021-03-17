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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.InternalNameProvider;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.TimestampedKeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingTimestampedKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
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
public class TimestampedKeyValueStoreMaterializerTest {

    private final String storePrefix = "prefix";
    @Mock(type = MockType.NICE)
    private InternalNameProvider nameProvider;

    @Test
    public void shouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as("store"), nameProvider, storePrefix);

        final TimestampedKeyValueStoreMaterializer<String, String> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        final StoreBuilder<TimestampedKeyValueStore<String, String>> builder = materializer.materialize();
        final TimestampedKeyValueStore<String, String> store = builder.build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final StateStore logging = caching.wrapped();
        assertThat(store, instanceOf(MeteredTimestampedKeyValueStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(logging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithCachingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled(), nameProvider, storePrefix
        );
        final TimestampedKeyValueStoreMaterializer<String, String> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        final StoreBuilder<TimestampedKeyValueStore<String, String>> builder = materializer.materialize();
        final TimestampedKeyValueStore<String, String> store = builder.build();
        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(logging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withLoggingDisabled(), nameProvider, storePrefix
        );
        final TimestampedKeyValueStoreMaterializer<String, String> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        final StoreBuilder<TimestampedKeyValueStore<String, String>> builder = materializer.materialize();
        final TimestampedKeyValueStore<String, String> store = builder.build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(caching.wrapped(), not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled().withLoggingDisabled(), nameProvider, storePrefix
        );
        final TimestampedKeyValueStoreMaterializer<String, String> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        final StoreBuilder<TimestampedKeyValueStore<String, String>> builder = materializer.materialize();
        final TimestampedKeyValueStore<String, String> store = builder.build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(wrapped, not(instanceOf(CachingKeyValueStore.class)));
        assertThat(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore.class)));
    }

    @Test
    public void shouldCreateKeyValueStoreWithTheProvidedInnerStore() {
        final KeyValueBytesStoreSupplier supplier = EasyMock.createNiceMock(KeyValueBytesStoreSupplier.class);
        final InMemoryKeyValueStore store = new InMemoryKeyValueStore("name");
        EasyMock.expect(supplier.name()).andReturn("name").anyTimes();
        EasyMock.expect(supplier.get()).andReturn(store);
        EasyMock.expect(supplier.metricsScope()).andReturn("metricScope");
        EasyMock.replay(supplier);

        final MaterializedInternal<String, Integer, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), nameProvider, storePrefix);
        final TimestampedKeyValueStoreMaterializer<String, Integer> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        final StoreBuilder<TimestampedKeyValueStore<String, Integer>> builder = materializer.materialize();
        final TimestampedKeyValueStore<String, Integer> built = builder.build();

        assertThat(store.name(), CoreMatchers.equalTo(built.name()));
    }

}