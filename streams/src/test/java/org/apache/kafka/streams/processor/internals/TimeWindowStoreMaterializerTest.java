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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.TimeWindowStoreMaterializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingWindowBytesStore;
import org.apache.kafka.streams.state.internals.MeteredWindowStore;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class TimeWindowStoreMaterializerTest {

    @Mock(type = MockType.NICE)
    private Materialized<String, String, WindowStore<Bytes, byte[]>> materializedExternal;
    private TimeWindows timeWindows = TimeWindows.of(10L);

    @Before
    public void setUp() {
        WindowBytesStoreSupplier supplier = Stores.persistentWindowStore("store", 10L, 3, 5L, false);
        materializedExternal = Materialized.<String, String>as(supplier).withKeySerde(Serdes.String()).withValueSerde(Serdes.String());
    }

//    @Test
//    public void shouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled() {
//        final MaterializedInternal<String, String, WindowStore<Bytes, byte[]>> materialized
//                = new MaterializedInternal<>(materializedExternal);
//        final TimeWindowStoreMaterializer<String, String>
//            materializer = new TimeWindowStoreMaterializer<>(materialized, timeWindows);
//        final StoreBuilder<WindowStore<String, String>> builder = materializer.materialize();
//        final WindowStore<String, String> store = builder.build();
//        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
//        final StateStore logging = caching.wrappedStore();
//        assertThat(store, instanceOf(MeteredWindowStore.class));
//        assertThat(caching, instanceOf(CachedStateStore.class));
//        assertThat(logging, instanceOf(ChangeLoggingWindowBytesStore.class));
//    }
//
//    @Test
//    public void shouldCreateBuilderThatBuildsStoreWithCachingDisabled() {
//        final MaterializedInternal<String, String, WindowStore<Bytes, byte[]>> materialized
//                = new MaterializedInternal<>(materializedExternal
//                .withCachingDisabled());
//        final TimeWindowStoreMaterializer<String, String>
//            materializer = new TimeWindowStoreMaterializer<>(materialized, timeWindows);
//        final StoreBuilder<WindowStore<String, String>> builder = materializer.materialize();
//        final WindowStore<String, String> store = builder.build();
//        final WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
//        assertThat(logging, instanceOf(ChangeLoggingWindowBytesStore.class));
//    }
//
//    @Test
//    public void shouldCreateBuilderThatBuildsStoreWithLoggingDisabled() {
//        final MaterializedInternal<String, String, WindowStore<Bytes, byte[]>> materialized
//                = new MaterializedInternal<>(materializedExternal
//                .withLoggingDisabled());
//        final TimeWindowStoreMaterializer<String, String>
//            materializer = new TimeWindowStoreMaterializer<>(materialized, timeWindows);
//        final StoreBuilder<WindowStore<String, String>> builder = materializer.materialize();
//        final WindowStore<String, String> store = builder.build();
//        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
//        assertThat(caching, instanceOf(CachedStateStore.class));
//        assertThat(caching.wrappedStore(), not(instanceOf(ChangeLoggingWindowBytesStore.class)));
//    }
//
//    @Test
//    public void shouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled() {
//        final MaterializedInternal<String, String, WindowStore<Bytes, byte[]>> materialized
//                = new MaterializedInternal<>(materializedExternal
//                .withCachingDisabled()
//                .withLoggingDisabled());
//        final TimeWindowStoreMaterializer<String, String>
//            materializer = new TimeWindowStoreMaterializer<>(materialized, timeWindows);
//        final StoreBuilder<WindowStore<String, String>> builder = materializer.materialize();
//        final WindowStore<String, String> store = builder.build();
//        final StateStore wrapped = ((WrappedStateStore) store).wrappedStore();
//        assertThat(wrapped, not(instanceOf(CachedStateStore.class)));
//        assertThat(wrapped, not(instanceOf(ChangeLoggingWindowBytesStore.class)));
//    }
//
//    @Test
//    public void shouldCreateBuilderThatBuildsStoreWithoutSupplier() {
//        // remove supplier to use the time window given
//        materializedExternal = Materialized.as("mock_materializer");
//        final MaterializedInternal<String, String, WindowStore<Bytes, byte[]>> materialized
//            = new MaterializedInternal<>(materializedExternal);
//        final TimeWindowStoreMaterializer<String, String> materializer
//            = new TimeWindowStoreMaterializer<>(materialized, timeWindows);
//        final StoreBuilder<WindowStore<String, String>> builder = materializer.materialize();
//        final WindowStore<String, String> store = builder.build();
//        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
//        assertThat(caching, instanceOf(CachedStateStore.class));
//        final WindowStoreBuilder windowStoreBuilder = (WindowStoreBuilder) builder;
//        assertEquals(24 * 60 * 60 * 1000L, windowStoreBuilder.retentionPeriod());
//    }
}
