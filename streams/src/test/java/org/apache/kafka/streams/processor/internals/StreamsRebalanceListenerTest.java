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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class StreamsRebalanceListenerTest {

    private final TaskManager taskManager = mock(TaskManager.class);
    private final StreamThread streamThread = mock(StreamThread.class);
    private final AtomicInteger assignmentErrorCode = new AtomicInteger();
    private final MockTime time = new MockTime();
    private final StreamsRebalanceListener streamsRebalanceListener = new StreamsRebalanceListener(
        time,
        taskManager,
        streamThread,
        LoggerFactory.getLogger(StreamsRebalanceListenerTest.class),
        assignmentErrorCode
    );

    @Before
    public void before() {
        expect(streamThread.state()).andStubReturn(null);
        expect(taskManager.activeTaskIds()).andStubReturn(null);
        expect(taskManager.standbyTaskIds()).andStubReturn(null);
    }

    @Test
    public void shouldThrowMissingSourceTopicException() {
        taskManager.handleRebalanceComplete();
        expectLastCall();
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code());

        final MissingSourceTopicException exception = assertThrows(
            MissingSourceTopicException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("One or more source topics were missing during rebalance"));
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldSwallowVersionProbingError() {
        expect(streamThread.setState(State.PARTITIONS_ASSIGNED)).andStubReturn(State.PARTITIONS_REVOKED);
        streamThread.setPartitionAssignedTime(time.milliseconds());
        taskManager.handleRebalanceComplete();
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.VERSION_PROBING.code());
        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldSendShutdown() {
        streamThread.shutdownToError();
        EasyMock.expectLastCall();
        taskManager.handleRebalanceComplete();
        EasyMock.expectLastCall();
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.SHUTDOWN_REQUESTED.code());
        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldThrowTaskAssignmentException() {
        taskManager.handleRebalanceComplete();
        expectLastCall();
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.ASSIGNMENT_ERROR.code());

        final TaskAssignmentException exception = assertThrows(
            TaskAssignmentException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("Hit an unexpected exception during task assignment phase of rebalance"));
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldThrowTaskAssignmentExceptionOnUnrecognizedErrorCode() {
        replay(taskManager, streamThread);
        assignmentErrorCode.set(Integer.MAX_VALUE);

        final TaskAssignmentException exception = assertThrows(
            TaskAssignmentException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("Hit an unrecognized exception during rebalance"));
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldHandleAssignedPartitions() {
        taskManager.handleRebalanceComplete();
        expect(streamThread.setState(State.PARTITIONS_ASSIGNED)).andReturn(State.RUNNING);
        streamThread.setPartitionAssignedTime(time.milliseconds());

        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.NONE.code());

        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());

        verify(taskManager, streamThread);
    }

    @Test
    public void shouldHandleRevokedPartitions() {
        final Collection<TopicPartition> partitions = Collections.singletonList(new TopicPartition("topic", 0));
        expect(streamThread.setState(State.PARTITIONS_REVOKED)).andReturn(State.RUNNING);
        taskManager.handleRevocation(partitions);
        replay(streamThread, taskManager);

        streamsRebalanceListener.onPartitionsRevoked(partitions);

        verify(taskManager, streamThread);
    }

    @Test
    public void shouldNotHandleRevokedPartitionsIfStateCannotTransitToPartitionRevoked() {
        expect(streamThread.setState(State.PARTITIONS_REVOKED)).andReturn(null);
        replay(streamThread, taskManager);

        streamsRebalanceListener.onPartitionsRevoked(Collections.singletonList(new TopicPartition("topic", 0)));

        verify(taskManager, streamThread);
    }

    @Test
    public void shouldNotHandleEmptySetOfRevokedPartitions() {
        expect(streamThread.setState(State.PARTITIONS_REVOKED)).andReturn(State.RUNNING);
        replay(streamThread, taskManager);

        streamsRebalanceListener.onPartitionsRevoked(Collections.emptyList());

        verify(taskManager, streamThread);
    }

    @Test
    public void shouldHandleLostPartitions() {
        taskManager.handleLostAll();
        replay(streamThread, taskManager);

        streamsRebalanceListener.onPartitionsLost(Collections.singletonList(new TopicPartition("topic", 0)));

        verify(taskManager, streamThread);
    }
}