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
package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.graph.TableSourceNode.TableSourceNodeBuilder;
import org.apache.kafka.streams.processor.api.ProcessorWrapper;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TableSourceNodeTest {

    private static final String STORE_NAME = "store-name";
    private static final String TOPIC = "input-topic";
    private static final String SOURCE_NAME = "source-name";
    private static final String PROCESSOR_NAME = "processor-name";

    private InternalTopologyBuilder topologyBuilder = mock(InternalTopologyBuilder.class);

    @BeforeEach
    public void before() {
        when(topologyBuilder.wrapProcessorSupplier(any(), any()))
                .thenAnswer(iom -> ProcessorWrapper.asWrapped(iom.getArgument(1)));
    }

    @Test
    public void shouldConnectStateStoreToInputTopicIfInputTopicIsUsedAsChangelog() {
        final boolean shouldReuseSourceTopicForChangelog = true;
        buildTableSourceNode(shouldReuseSourceTopicForChangelog);
        verify(topologyBuilder).addReadOnlyStateStore(
                any(),
                argThat(store -> STORE_NAME.equals(store.name())),
                eq(SOURCE_NAME),
                any(),
                any(),
                any(),
                eq(TOPIC),
                eq(PROCESSOR_NAME),
                any()
        );
        verify(topologyBuilder, never()).connectSourceStoreAndTopic(any(), any());
    }

    @Test
    public void shouldConnectStateStoreToChangelogTopic() {
        final boolean shouldReuseSourceTopicForChangelog = false;
        buildTableSourceNode(shouldReuseSourceTopicForChangelog);
        verify(topologyBuilder, never()).connectSourceStoreAndTopic(STORE_NAME, TOPIC);
        verify(topologyBuilder, never()).addReadOnlyStateStore(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private void buildTableSourceNode(final boolean shouldReuseSourceTopicForChangelog) {
        final TableSourceNodeBuilder<String, String> tableSourceNodeBuilder = TableSourceNode.tableSourceNodeBuilder();
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>>
                materializedInternal = new MaterializedInternal<>(Materialized.as(STORE_NAME));
        final TableSourceNode<String, String> tableSourceNode = tableSourceNodeBuilder
            .withTopic(TOPIC)
            .withSourceName(SOURCE_NAME)
            .withConsumedInternal(new ConsumedInternal<>(Consumed.as("node-name")))
            .withProcessorParameters(
                    new ProcessorParameters<>(new KTableSource<>(materializedInternal), PROCESSOR_NAME))
            .build();
        tableSourceNode.reuseSourceTopicForChangeLog(shouldReuseSourceTopicForChangelog);

        tableSourceNode.writeToTopology(topologyBuilder);
    }
}
