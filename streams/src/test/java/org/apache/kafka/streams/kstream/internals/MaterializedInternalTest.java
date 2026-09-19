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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.DslSessionParams;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.DslWindowParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MaterializedInternalTest {

    private InternalNameProvider nameProvider = mock(InternalNameProvider.class);
    private KeyValueBytesStoreSupplier supplier = mock(KeyValueBytesStoreSupplier.class);
    private final String prefix = "prefix";

    @Test
    public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull() {
        final String generatedName = prefix + "-store";
        when(nameProvider.newStoreName(prefix)).thenReturn(generatedName);

        final MaterializedInternal<Object, Object, StateStore> materialized =
            new MaterializedInternal<>(Materialized.with(null, null), nameProvider, prefix);

        assertEquals(generatedName, materialized.storeName());
    }

    @Test
    public void shouldUseProvidedStoreNameWhenSet() {
        final String storeName = "store-name";
        final MaterializedInternal<Object, Object, StateStore> materialized =
            new MaterializedInternal<>(Materialized.as(storeName), nameProvider, prefix);
        assertEquals(storeName, materialized.storeName());
    }

    @Test
    public void shouldUseStoreNameOfSupplierWhenProvided() {
        final String storeName = "other-store-name";
        when(supplier.name()).thenReturn(storeName);
        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), nameProvider, prefix);
        assertEquals(storeName, materialized.storeName());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseStoreTypeWhenProvidedViaTopologyConfig() {
        final Properties topologyOverrides = new Properties();
        topologyOverrides.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig());

        final InternalTopologyBuilder topologyBuilder = new InternalTopologyBuilder(
            new TopologyConfig("my-topology", config, topologyOverrides));

        final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(topologyBuilder, false);

        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), internalStreamsBuilder, prefix);
        assertEquals(Optional.of(Materialized.StoreType.IN_MEMORY), materialized.dslStoreSuppliers());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldPreferStoreSupplierWhenProvidedWithStoreTypeViaTopologyConfig() {
        final Properties topologyOverrides = new Properties();
        topologyOverrides.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.ROCKS_DB);
        topologyOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, TestStoreSupplier.class);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig());

        final InternalTopologyBuilder topologyBuilder = new InternalTopologyBuilder(
                new TopologyConfig("my-topology", config, topologyOverrides));

        final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(topologyBuilder, false);

        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<>(Materialized.as(supplier), internalStreamsBuilder, prefix);
        assertTrue(materialized.dslStoreSuppliers().isPresent());
        assertInstanceOf(TestStoreSupplier.class, materialized.dslStoreSuppliers().get());
    }

    @Test
    public void shouldReturnEmptyWhenOriginalsAndOverridesDontHaveSuppliersSpecified() {
        final Properties topologyOverrides = new Properties();
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig());

        final InternalTopologyBuilder topologyBuilder = new InternalTopologyBuilder(
                new TopologyConfig("my-topology", config, topologyOverrides));

        final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(topologyBuilder, false);

        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<>(Materialized.as(supplier), internalStreamsBuilder, prefix);
        assertFalse(materialized.dslStoreSuppliers().isPresent());
    }

    public static class TestStoreSupplier implements DslStoreSuppliers {

        @Override
        public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params) {
            return null;
        }

        @Override
        public WindowBytesStoreSupplier windowStore(final DslWindowParams params) {
            return null;
        }

        @Override
        public SessionBytesStoreSupplier sessionStore(final DslSessionParams params) {
            return null;
        }
    }
}