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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.common.KafkaException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamsRebalanceEventsProcessorTest {

    private static final String SUBTOPOLOGY_0 = "subtopology-0";
    private static final String SUBTOPOLOGY_1 = "subtopology-1";

    @Mock
    private StreamsGroupRebalanceCallbacks rebalanceCallbacks;

    @Mock
    private ApplicationEventHandler applicationEventHandler;

    private StreamsRebalanceData rebalanceData;

    @BeforeEach
    public void setup() {
        final UUID processId = UUID.randomUUID();
        final Optional<StreamsRebalanceData.HostInfo> endpoint = Optional.of(new StreamsRebalanceData.HostInfo("localhost", 9090));
        final Map<String, StreamsRebalanceData.Subtopology> subtopologies = new HashMap<>();
        final Map<String, String> clientTags = Map.of("clientTag1", "clientTagValue1");
        rebalanceData = new StreamsRebalanceData(
            processId,
            endpoint,
            subtopologies,
            clientTags
        );
    }

    @Test
    public void shouldInvokeOnTasksAssignedCallback() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 2),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 3)
        );
        StreamsRebalanceData.Assignment assignment =
            new StreamsRebalanceData.Assignment(activeTasks, standbyTasks, warmupTasks);
        when(rebalanceCallbacks.onTasksAssigned(assignment)).thenReturn(Optional.empty());

        final CompletableFuture<Void> onTasksAssignedExecuted = rebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(assignment);

        assertFalse(onTasksAssignedExecuted.isDone());
        rebalanceEventsProcessor.process();
        ArgumentCaptor<StreamsOnTasksAssignedCallbackCompletedEvent> streamsOnTasksAssignedCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnTasksAssignedCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnTasksAssignedCallbackCompletedCaptor.capture());
        StreamsOnTasksAssignedCallbackCompletedEvent streamsOnTasksAssignedCallbackCompletedEvent =
            streamsOnTasksAssignedCallbackCompletedCaptor.getValue();
        assertFalse(streamsOnTasksAssignedCallbackCompletedEvent.future().isDone());
        assertTrue(streamsOnTasksAssignedCallbackCompletedEvent.error().isEmpty());
        assertEquals(assignment, rebalanceData.reconciledAssignment());
    }

    @Test
    public void shouldReThrowErrorFromOnTasksAssignedCallbackAndPassErrorToBackground() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 2),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 3)
        );
        StreamsRebalanceData.Assignment assignment =
            new StreamsRebalanceData.Assignment(activeTasks, standbyTasks, warmupTasks);
        final Exception exception = new RuntimeException("Nobody expects the Spanish inquisition.");
        when(rebalanceCallbacks.onTasksAssigned(assignment)).thenReturn(Optional.of(exception));

        final CompletableFuture<Void> onTasksAssignedExecuted = rebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(assignment);

        assertFalse(onTasksAssignedExecuted.isDone());
        final Exception actualException = assertThrows(KafkaException.class, rebalanceEventsProcessor::process);
        assertEquals("Task assignment callback throws an error", actualException.getMessage());
        ArgumentCaptor<StreamsOnTasksAssignedCallbackCompletedEvent> streamsOnTasksAssignedCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnTasksAssignedCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnTasksAssignedCallbackCompletedCaptor.capture());
        StreamsOnTasksAssignedCallbackCompletedEvent streamsOnTasksAssignedCallbackCompletedEvent =
            streamsOnTasksAssignedCallbackCompletedCaptor.getValue();
        assertFalse(streamsOnTasksAssignedCallbackCompletedEvent.future().isDone());
        assertTrue(streamsOnTasksAssignedCallbackCompletedEvent.error().isPresent());
        assertEquals(exception, streamsOnTasksAssignedCallbackCompletedEvent.error().get().getCause());
        assertEquals(exception, actualException.getCause());
        assertEquals(StreamsRebalanceData.Assignment.EMPTY, rebalanceData.reconciledAssignment());
    }

    @Test
    public void shouldInvokeOnTasksRevokedCallback() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        when(rebalanceCallbacks.onTasksRevoked(activeTasks)).thenReturn(Optional.empty());

        final CompletableFuture<Void> onTasksRevokedExecuted = rebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasks);

        assertFalse(onTasksRevokedExecuted.isDone());
        rebalanceEventsProcessor.process();
        ArgumentCaptor<StreamsOnTasksRevokedCallbackCompletedEvent> streamsOnTasksRevokedCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnTasksRevokedCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnTasksRevokedCallbackCompletedCaptor.capture());
        StreamsOnTasksRevokedCallbackCompletedEvent streamsOnTasksRevokedCallbackCompletedEvent =
            streamsOnTasksRevokedCallbackCompletedCaptor.getValue();
        assertFalse(streamsOnTasksRevokedCallbackCompletedEvent.future().isDone());
        assertTrue(streamsOnTasksRevokedCallbackCompletedEvent.error().isEmpty());
    }

    @Test
    public void shouldReThrowErrorFromOnTasksRevokedCallbackAndPassErrorToBackground() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        final Exception exception = new RuntimeException("Nobody expects the Spanish inquisition.");
        when(rebalanceCallbacks.onTasksRevoked(activeTasks)).thenReturn(Optional.of(exception));

        final CompletableFuture<Void> onTasksRevokedExecuted = rebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasks);

        assertFalse(onTasksRevokedExecuted.isDone());
        final Exception actualException = assertThrows(KafkaException.class, rebalanceEventsProcessor::process);
        assertEquals("Task revocation callback throws an error", actualException.getMessage());
        ArgumentCaptor<StreamsOnTasksRevokedCallbackCompletedEvent> streamsOnTasksRevokedCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnTasksRevokedCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnTasksRevokedCallbackCompletedCaptor.capture());
        StreamsOnTasksRevokedCallbackCompletedEvent streamsOnTasksRevokedCallbackCompletedEvent =
            streamsOnTasksRevokedCallbackCompletedCaptor.getValue();
        assertTrue(streamsOnTasksRevokedCallbackCompletedEvent.error().isPresent());
        assertEquals(exception, streamsOnTasksRevokedCallbackCompletedEvent.error().get().getCause());
        assertEquals(exception, actualException.getCause());
    }

    @Test
    public void shouldInvokeOnAllTasksLostCallback() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 2),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 3)
        );
        StreamsRebalanceData.Assignment assignment =
            new StreamsRebalanceData.Assignment(activeTasks, standbyTasks, warmupTasks);
        when(rebalanceCallbacks.onTasksAssigned(assignment)).thenReturn(Optional.empty());
        rebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(assignment);
        rebalanceEventsProcessor.process();
        assertEquals(assignment, rebalanceData.reconciledAssignment());
        when(rebalanceCallbacks.onAllTasksLost()).thenReturn(Optional.empty());

        final CompletableFuture<Void> onAllTasksLostExecuted = rebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();

        assertFalse(onAllTasksLostExecuted.isDone());
        rebalanceEventsProcessor.process();
        ArgumentCaptor<StreamsOnAllTasksLostCallbackCompletedEvent> streamsOnAllTasksLostCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnAllTasksLostCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnAllTasksLostCallbackCompletedCaptor.capture());
        StreamsOnAllTasksLostCallbackCompletedEvent streamsOnAllTasksLostCallbackCompletedEvent =
            streamsOnAllTasksLostCallbackCompletedCaptor.getValue();
        assertFalse(streamsOnAllTasksLostCallbackCompletedEvent.future().isDone());
        assertTrue(streamsOnAllTasksLostCallbackCompletedEvent.error().isEmpty());
        assertEquals(StreamsRebalanceData.Assignment.EMPTY, rebalanceData.reconciledAssignment());
    }

    @Test
    public void shouldReThrowErrorFromOnAllTasksLostCallbackAndPassErrorToBackground() {
        final StreamsRebalanceEventsProcessor rebalanceEventsProcessor =
            new StreamsRebalanceEventsProcessor(rebalanceData, rebalanceCallbacks);
        rebalanceEventsProcessor.setApplicationEventHandler(applicationEventHandler);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 1),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_1, 2),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_0, 3)
        );
        StreamsRebalanceData.Assignment assignment =
            new StreamsRebalanceData.Assignment(activeTasks, standbyTasks, warmupTasks);
        when(rebalanceCallbacks.onTasksAssigned(assignment)).thenReturn(Optional.empty());
        rebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(assignment);
        rebalanceEventsProcessor.process();
        assertEquals(assignment, rebalanceData.reconciledAssignment());
        final Exception exception = new RuntimeException("Nobody expects the Spanish inquisition.");
        when(rebalanceCallbacks.onAllTasksLost()).thenReturn(Optional.of(exception));

        final CompletableFuture<Void> onAllTasksLostExecuted = rebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();

        assertFalse(onAllTasksLostExecuted.isDone());
        final Exception actualException = assertThrows(KafkaException.class, rebalanceEventsProcessor::process);
        assertEquals("All tasks lost callback throws an error", actualException.getMessage());
        ArgumentCaptor<StreamsOnAllTasksLostCallbackCompletedEvent> streamsOnAllTasksLostCallbackCompletedCaptor =
            ArgumentCaptor.forClass(StreamsOnAllTasksLostCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(streamsOnAllTasksLostCallbackCompletedCaptor.capture());
        StreamsOnAllTasksLostCallbackCompletedEvent streamsOnAllTasksLostCallbackCompletedEvent =
            streamsOnAllTasksLostCallbackCompletedCaptor.getValue();
        assertFalse(streamsOnAllTasksLostCallbackCompletedEvent.future().isDone());
        assertTrue(streamsOnAllTasksLostCallbackCompletedEvent.error().isPresent());
        assertEquals(exception, streamsOnAllTasksLostCallbackCompletedEvent.error().get().getCause());
        assertEquals(exception, actualException.getCause());
        assertEquals(assignment, rebalanceData.reconciledAssignment());
    }
}