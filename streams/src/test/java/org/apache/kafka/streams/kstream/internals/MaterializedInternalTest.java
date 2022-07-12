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
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(EasyMockRunner.class)
public class MaterializedInternalTest {

    @Mock(type = MockType.NICE)
    private InternalNameProvider nameProvider;

    @Mock(type = MockType.NICE)
    private KeyValueBytesStoreSupplier supplier;
    private final String prefix = "prefix";

    @Test
    public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull() {
        final String generatedName = prefix + "-store";
        EasyMock.expect(nameProvider.newStoreName(prefix)).andReturn(generatedName);

        EasyMock.replay(nameProvider);

        final MaterializedInternal<Object, Object, StateStore> materialized =
            new MaterializedInternal<>(Materialized.with(null, null), nameProvider, prefix);

        assertThat(materialized.storeName(), equalTo(generatedName));
        EasyMock.verify(nameProvider);
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
        EasyMock.expect(supplier.name()).andReturn(storeName).anyTimes();
        EasyMock.replay(supplier);
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