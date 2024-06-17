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

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.graph.TableSourceNode.TableSourceNodeBuilder;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TableSourceNodeTest {

    private static final String STORE_NAME = "store-name";
    private static final String TOPIC = "input-topic";

    @Mock
    private InternalTopologyBuilder topologyBuilder;

    @Test
    public void shouldConnectStateStoreToInputTopicIfInputTopicIsUsedAsChangelog() {
        final boolean shouldReuseSourceTopicForChangelog = true;
        buildTableSourceNode(shouldReuseSourceTopicForChangelog);
        verify(topologyBuilder).connectSourceStoreAndTopic(STORE_NAME, TOPIC);
    }

    @Test
    public void shouldConnectStateStoreToChangelogTopic() {
        final boolean shouldReuseSourceTopicForChangelog = false;
        buildTableSourceNode(shouldReuseSourceTopicForChangelog);
        verify(topologyBuilder, never()).connectSourceStoreAndTopic(STORE_NAME, TOPIC);
    }

    private void buildTableSourceNode(final boolean shouldReuseSourceTopicForChangelog) {
        final TableSourceNodeBuilder<String, String> tableSourceNodeBuilder = TableSourceNode.tableSourceNodeBuilder();
        final TableSourceNode<String, String> tableSourceNode = tableSourceNodeBuilder
            .withTopic(TOPIC)
            .withMaterializedInternal(new MaterializedInternal<>(Materialized.as(STORE_NAME)))
            .withConsumedInternal(new ConsumedInternal<>(Consumed.as("node-name")))
            .withProcessorParameters(
                new ProcessorParameters<>(new KTableSource<>(STORE_NAME, STORE_NAME), null))
            .build();
        tableSourceNode.reuseSourceTopicForChangeLog(shouldReuseSourceTopicForChangelog);

        tableSourceNode.writeToTopology(topologyBuilder);
    }
}
