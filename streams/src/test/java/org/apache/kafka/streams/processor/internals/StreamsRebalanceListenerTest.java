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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class StreamsRebalanceListenerTest {

    @Mock
    private TaskManager taskManager;
    @Mock
    private StreamThread streamThread;
    private final AtomicInteger assignmentErrorCode = new AtomicInteger();
    private final MockTime time = new MockTime();
    private StreamsRebalanceListener streamsRebalanceListener;

    @Before
    public void setup() {
        streamsRebalanceListener = new StreamsRebalanceListener(time,
                taskManager,
                streamThread,
                LoggerFactory.getLogger(StreamsRebalanceListenerTest.class),
                assignmentErrorCode
        );
    }

    @Test
    public void shouldThrowMissingSourceTopicException() {
        assignmentErrorCode.set(AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code());

        final MissingSourceTopicException exception = assertThrows(
            MissingSourceTopicException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("One or more source topics were missing during rebalance"));
        verify(taskManager).handleRebalanceComplete();
    }

    @Test
    public void shouldSwallowVersionProbingError() {
        assignmentErrorCode.set(AssignorError.VERSION_PROBING.code());
        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());
        verify(streamThread).setState(State.PARTITIONS_ASSIGNED);
        verify(streamThread).setPartitionAssignedTime(time.milliseconds());
        verify(taskManager).handleRebalanceComplete();
    }

    @Test
    public void shouldSendShutdown() {
        assignmentErrorCode.set(AssignorError.SHUTDOWN_REQUESTED.code());
        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());
        verify(taskManager).handleRebalanceComplete();
        verify(streamThread).shutdownToError();
    }

    @Test
    public void shouldThrowTaskAssignmentException() {
        assignmentErrorCode.set(AssignorError.ASSIGNMENT_ERROR.code());

        final TaskAssignmentException exception = assertThrows(
            TaskAssignmentException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("Hit an unexpected exception during task assignment phase of rebalance"));

        verify(taskManager).handleRebalanceComplete();
    }

    @Test
    public void shouldThrowTaskAssignmentExceptionOnUnrecognizedErrorCode() {
        assignmentErrorCode.set(Integer.MAX_VALUE);

        final TaskAssignmentException exception = assertThrows(
            TaskAssignmentException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("Hit an unrecognized exception during rebalance"));
    }

    @Test
    public void shouldHandleAssignedPartitions() {
        assignmentErrorCode.set(AssignorError.NONE.code());

        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());

        verify(streamThread).setState(State.PARTITIONS_ASSIGNED);
        verify(streamThread).setPartitionAssignedTime(time.milliseconds());
        verify(taskManager).handleRebalanceComplete();
    }

    @Test
    public void shouldHandleRevokedPartitions() {
        final Collection<TopicPartition> partitions = Collections.singletonList(new TopicPartition("topic", 0));
        when(streamThread.setState(State.PARTITIONS_REVOKED)).thenReturn(State.RUNNING);

        streamsRebalanceListener.onPartitionsRevoked(partitions);

        verify(taskManager).handleRevocation(partitions);
    }

    @Test
    public void shouldNotHandleRevokedPartitionsIfStateCannotTransitToPartitionRevoked() {
        when(streamThread.setState(State.PARTITIONS_REVOKED)).thenReturn(null);

        streamsRebalanceListener.onPartitionsRevoked(Collections.singletonList(new TopicPartition("topic", 0)));

        verify(taskManager, never()).handleRevocation(any());
    }

    @Test
    public void shouldNotHandleEmptySetOfRevokedPartitions() {
        when(streamThread.setState(State.PARTITIONS_REVOKED)).thenReturn(State.RUNNING);

        streamsRebalanceListener.onPartitionsRevoked(Collections.emptyList());

        verify(taskManager, never()).handleRevocation(any());
    }

    @Test
    public void shouldHandleLostPartitions() {
        streamsRebalanceListener.onPartitionsLost(Collections.singletonList(new TopicPartition("topic", 0)));

        verify(taskManager).handleLostAll();
    }
}