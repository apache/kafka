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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MaterializedInternalTest {

    @Mock
    private InternalNameProvider nameProvider;
    @Mock
    private KeyValueBytesStoreSupplier supplier;
    private final String prefix = "prefix";

    @Test
    public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull() {
        final String generatedName = prefix + "-store";
        when(nameProvider.newStoreName(prefix)).thenReturn(generatedName);

        final MaterializedInternal<Object, Object, StateStore> materialized =
            new MaterializedInternal<>(Materialized.with(null, null), nameProvider, prefix);

        assertThat(materialized.storeName(), equalTo(generatedName));
    }

    @Test
    public void shouldUseProvidedStoreNameWhenSet() {
        final String storeName = "store-name";
        final MaterializedInternal<Object, Object, StateStore> materialized =
            new MaterializedInternal<>(Materialized.as(storeName), nameProvider, prefix);
        assertThat(materialized.storeName(), equalTo(storeName));
    }

    @Test
    public void shouldUseStoreNameOfSupplierWhenProvided() {
        final String storeName = "other-store-name";
        when(supplier.name()).thenReturn(storeName);
        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), nameProvider, prefix);
        assertThat(materialized.storeName(), equalTo(storeName));
    }

    @Test
    public void shouldUseStoreTypeWhenProvidedViaTopologyConfig() {
        final Properties topologyOverrides = new Properties();
        topologyOverrides.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig());

        final InternalTopologyBuilder topologyBuilder = new InternalTopologyBuilder(
            new TopologyConfig("my-topology", config, topologyOverrides));

        final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(topologyBuilder);

        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier), internalStreamsBuilder, prefix);
        assertThat(materialized.storeType(), equalTo(Materialized.StoreType.IN_MEMORY));
    }
}