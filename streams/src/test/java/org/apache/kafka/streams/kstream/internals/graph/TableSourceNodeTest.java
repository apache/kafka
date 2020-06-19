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
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({InternalTopologyBuilder.class})
public class TableSourceNodeTest {

    @Test
    public void shouldConnectStateStoreToInputTopicIfInputTopicIsUsedAsChangelog() {
        final String storeName = "store-name";
        final String topic = "input-topic";
        final InternalTopologyBuilder topologyBuilder = PowerMock.createNiceMock(InternalTopologyBuilder.class);
        topologyBuilder.connectSourceStoreAndTopic(storeName, topic);
        EasyMock.replay(topologyBuilder);
        final TableSourceNodeBuilder<String, String> tableSourceNodeBuilder = TableSourceNode.tableSourceNodeBuilder();
        final TableSourceNode<String, String> tableSourceNode = tableSourceNodeBuilder
            .withTopic(topic)
            .withMaterializedInternal(new MaterializedInternal<>(Materialized.as(storeName)))
            .withConsumedInternal(new ConsumedInternal<>(Consumed.as("node-name")))
            .withProcessorParameters(
                new ProcessorParameters<>(new KTableSource<>(storeName, storeName), null))
            .build();
        tableSourceNode.reuseSourceTopicForChangeLog(true);

        tableSourceNode.writeToTopology(topologyBuilder);

        EasyMock.verify(topologyBuilder);
    }
}