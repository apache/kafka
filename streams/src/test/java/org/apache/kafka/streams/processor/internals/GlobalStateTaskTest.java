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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.GlobalStateManagerStub;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStateTaskTest {

    private final LogContext logContext = new LogContext();
    private Map<TopicPartition, Long> offsets;
    private GlobalStateUpdateTask globalStateTask;
    private GlobalStateManagerStub stateMgr;
    private List<ProcessorNode> processorNodes;
    private NoOpProcessorContext context;
    private TopicPartition t1;
    private TopicPartition t2;
    private MockSourceNode sourceOne;
    private MockSourceNode sourceTwo;
    private ProcessorTopology topology;

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
        topology = new ProcessorTopology(processorNodes,
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
        globalStateTask = new GlobalStateUpdateTask(topology, context, stateMgr, new LogAndFailExceptionHandler(), logContext);
    }

    @Test
    public void shouldInitializeStateManager() {
        final Map<TopicPartition, Long> startingOffsets = globalStateTask.initialize();
        assertTrue(stateMgr.initialized);
        assertEquals(offsets, startingOffsets);
    }

    @Test
    public void shouldInitializeContext() {
        globalStateTask.initialize();
        assertTrue(context.initialized);
    }

    @Test
    public void shouldInitializeProcessorTopology() {
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
    public void shouldProcessRecordsForTopic() {
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t1", 1, 1, "foo".getBytes(), "bar".getBytes()));
        assertEquals(1, sourceOne.numReceived);
        assertEquals(0, sourceTwo.numReceived);
    }

    @Test
    public void shouldProcessRecordsForOtherTopic() {
        final byte[] integerBytes = new IntegerSerializer().serialize("foo", 1);
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t2", 1, 1, integerBytes, integerBytes));
        assertEquals(1, sourceTwo.numReceived);
        assertEquals(0, sourceOne.numReceived);
    }

    private void maybeDeserialize(final GlobalStateUpdateTask globalStateTask,
                                  final byte[] key,
                                  final byte[] recordValue,
                                  boolean failExpected) {
        final ConsumerRecord record = new ConsumerRecord<>("t2", 1, 1,
                0L, TimestampType.CREATE_TIME, 0L, 0, 0,
                key, recordValue);
        globalStateTask.initialize();
        try {
            globalStateTask.update(record);
            if (failExpected) {
                fail("Should have failed to deserialize.");
            }
        } catch (StreamsException e) {
            if (!failExpected) {
                fail("Shouldn't have failed to deserialize.");
            }
        }
    }


    @Test
    public void shouldThrowStreamsExceptionWhenKeyDeserializationFails() throws Exception {
        final byte[] key = new LongSerializer().serialize("t2", 1L);
        final byte[] recordValue = new IntegerSerializer().serialize("t2", 10);
        maybeDeserialize(globalStateTask, key, recordValue, true);
    }


    @Test
    public void shouldThrowStreamsExceptionWhenValueDeserializationFails() throws Exception {
        final byte[] key = new IntegerSerializer().serialize("t2", 1);
        final byte[] recordValue = new LongSerializer().serialize("t2", 10L);
        maybeDeserialize(globalStateTask, key, recordValue, true);
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler() throws Exception {
        final GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
            topology,
            context,
            stateMgr,
            new LogAndContinueExceptionHandler(),
            logContext);
        final byte[] key = new LongSerializer().serialize("t2", 1L);
        final byte[] recordValue = new IntegerSerializer().serialize("t2", 10);

        maybeDeserialize(globalStateTask2, key, recordValue, false);
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenValueDeserializationFails() throws Exception {
        final GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
            topology,
            context,
            stateMgr,
            new LogAndContinueExceptionHandler(),
            logContext);
        final byte[] key = new IntegerSerializer().serialize("t2", 1);
        final byte[] recordValue = new LongSerializer().serialize("t2", 10L);

        maybeDeserialize(globalStateTask2, key, recordValue, false);
    }


    @Test
    public void shouldCloseStateManagerWithOffsets() throws IOException {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 52L);
        expectedOffsets.put(t2, 100L);
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t1", 1, 51, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.close();
        assertEquals(expectedOffsets, stateMgr.checkpointed());
        assertTrue(stateMgr.closed);
    }

    @Test
    public void shouldCheckpointOffsetsWhenStateIsFlushed() {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 102L);
        expectedOffsets.put(t2, 100L);
        globalStateTask.initialize();
        globalStateTask.update(new ConsumerRecord<>("t1", 1, 101, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.flushState();
        assertThat(stateMgr.checkpointed(), equalTo(expectedOffsets));
    }

}