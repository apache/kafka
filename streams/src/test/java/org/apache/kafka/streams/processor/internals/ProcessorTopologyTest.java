/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.test.MockProcessorDef;
import org.junit.Test;

public class ProcessorTopologyTest {

    @Test
    public void testTopologyMetadata() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addSource("source-2", "topic-2", "topic-3");
        builder.addProcessor("processor-1", new MockProcessorDef(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorDef(), "source-1", "source-2");
        builder.addSink("sink-1", "topic-3", "processor-1");
        builder.addSink("sink-2", "topic-4", "processor-1", "processor-2");

        final ProcessorTopology topology = builder.build();

        assertEquals(6, topology.processors().size());

        assertEquals(2, topology.sources().size());

        assertEquals(3, topology.sourceTopics().size());

        assertNotNull(topology.source("topic-1"));

        assertNotNull(topology.source("topic-2"));

        assertNotNull(topology.source("topic-3"));

        assertEquals(topology.source("topic-2"), topology.source("topic-3"));
    }
}
