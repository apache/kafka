/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.GlobalStateManagerStub;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalStateTaskTest {

    private Map<TopicPartition, Long> offsets;
    private GlobalStateUpdateTask globalStateTask;
    private GlobalStateManagerStub stateMgr;
    private List<ProcessorNode> processorNodes;
    private NoOpProcessorContext context;
    private TopicPartition t1;
    private TopicPartition t2;
    private MockSourceNode sourceOne;
    private MockSourceNode sourceTwo;

    @Before
    public void before() {
        sourceOne = new MockSourceNode<>(new String[]{"t1"},
                                         new StringDeserializer(),
                                         new StringDeserializer());
        sourceTwo = new MockSourceNode<>(new String[]{"t2"},
                                         new IntegerDeserializer(),
                                         new IntegerDeserializer());
        processorNodes = Arrays.asList(sourceOne, sourceTwo, new MockProcessorNode<>(-1), new MockProcessorNode<>(-1));
        final Set<String> storeNames = Utils.mkSet("t1-store", "t2-store");
        final Map<String, SourceNode> sourceByTopics = new HashMap<>();
        sourceByTopics.put("t1", sourceOne);
        sourceByTopics.put("t2", sourceTwo);
        final Map<String, String> storeToTopic = new HashMap<>();
        storeToTopic.put("t1-store", "t1");
        storeToTopic.put("t2-store", "t2");
        final ProcessorTopology topology = new ProcessorTopology(processorNodes,
                                                                 sourceByTopics,
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>emptyList(),
                                                                 storeToTopic,
                                                                 Collections.<StateStore>emptyList());
        context = new NoOpProcessorContext();

        t1 = new TopicPartition("t1", 1);
        t2 = new TopicPartition("t2", 1);
        offsets = new HashMap<>();
        offsets.put(t1, 50L);
        offsets.put(t2, 100L);
        stateMgr = new GlobalStateManagerStub(storeNames, offsets);
        globalStateTask = new GlobalStateUpdateTask(topology, context, stateMgr);
    }

    @Test
    public void shouldInitializeStateManager() throws Exception {
        final Map<TopicPartition, Long> startingOffsets = globalStateTask.initialize();
        assertTrue(stateMgr.initialized);
        assertEquals(offsets, startingOffsets);
    }

    @Test
    public void shouldInitializeContext() throws Exception {
        globalStateTask.initialize();
        assertTrue(context.initialized);
    }

    @Test
    public void shouldInitializeProcessorTopology() throws Exception {
        globalStateTask.initialize();
        for (ProcessorNode processorNode : processorNodes) {
            if (processorNode instanceof  MockProcessorNode) {
                assertTrue(((MockProcessorNode) processorNode).initialized);
            } else {
                assertTrue(((MockSourceNode) processorNode).initialized);
            }
        }
    }

    @Test
    public void shouldProcessRecordsForTopic() throws Exception {
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t1", 1, 1, "foo".getBytes(), "bar".getBytes()));
        assertEquals(1, sourceOne.numReceived);
        assertEquals(0, sourceTwo.numReceived);
    }

    @Test
    public void shouldProcessRecordsForOtherTopic() throws Exception {
        final byte[] integerBytes = new IntegerSerializer().serialize("foo", 1);
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t2", 1, 1, integerBytes, integerBytes));
        assertEquals(1, sourceTwo.numReceived);
        assertEquals(0, sourceOne.numReceived);
    }


    @Test
    public void shouldCloseStateManagerWithOffsets() throws Exception {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 52L);
        expectedOffsets.put(t2, 100L);
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t1", 1, 51, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.close();
        assertEquals(expectedOffsets, stateMgr.checkpointedOffsets());
        assertTrue(stateMgr.closed);
    }
}