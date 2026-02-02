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

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.RebalanceListenerMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DefaultStreamsRebalanceListenerTest {
    private static final String THREAD_ID = "test-thread-id";
    private final TaskManager taskManager = mock(TaskManager.class);
    private final StreamThread streamThread = mock(StreamThread.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Sensor tasksRevokedSensor = mock(Sensor.class);
    private final Sensor tasksAssignedSensor = mock(Sensor.class);
    private final Sensor tasksLostSensor = mock(Sensor.class);
    private MockTime mockTime;
    private DefaultStreamsRebalanceListener defaultStreamsRebalanceListener;

    @BeforeEach
    public void setUp() {
        mockTime = new MockTime();
    }

    private void createRebalanceListenerWithRebalanceData(final StreamsRebalanceData streamsRebalanceData) {
        try (MockedStatic<RebalanceListenerMetrics> rebalanceMetricsMock = mockStatic(RebalanceListenerMetrics.class)) {
            rebalanceMetricsMock.when(() -> RebalanceListenerMetrics.tasksRevokedSensor(anyString(), any(StreamsMetricsImpl.class)))
                .thenReturn(tasksRevokedSensor);
            rebalanceMetricsMock.when(() -> RebalanceListenerMetrics.tasksAssignedSensor(anyString(), any(StreamsMetricsImpl.class)))
                .thenReturn(tasksAssignedSensor);
            rebalanceMetricsMock.when(() -> RebalanceListenerMetrics.tasksLostSensor(anyString(), any(StreamsMetricsImpl.class)))
                .thenReturn(tasksLostSensor);

            defaultStreamsRebalanceListener = new DefaultStreamsRebalanceListener(
                LoggerFactory.getLogger(DefaultStreamsRebalanceListener.class),
                mockTime,
                streamsRebalanceData,
                streamThread,
                taskManager,
                streamsMetrics,
                THREAD_ID
            );
        }
    }

    @ParameterizedTest
    @EnumSource(StreamThread.State.class)
    void testOnTasksRevoked(final StreamThread.State state) {
        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));
        when(streamThread.state()).thenReturn(state);

        assertDoesNotThrow(() -> defaultStreamsRebalanceListener.onTasksRevoked(
            Set.of(new StreamsRebalanceData.TaskId("1", 0))
        ));

        final InOrder inOrder = inOrder(taskManager, streamThread);
        inOrder.verify(taskManager).handleRevocation(
            Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))
        );
        inOrder.verify(streamThread).state();
        if (state != StreamThread.State.PENDING_SHUTDOWN) {
            inOrder.verify(streamThread).setState(StreamThread.State.PARTITIONS_REVOKED);
        } else {
            inOrder.verify(streamThread, never()).setState(StreamThread.State.PARTITIONS_REVOKED);
        }
    }

    @Test
    void testOnTasksRevokedWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleRevocation(any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));

        final Exception actualException = assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onTasksRevoked(Set.of()));

        assertEquals(actualException, exception);
        verify(taskManager).handleRevocation(any());
        verify(streamThread, never()).setState(any());
    }

    @Test
    void testOnTasksAssigned() {
        final StreamsRebalanceData streamsRebalanceData = mock(StreamsRebalanceData.class);
        when(streamsRebalanceData.subtopologies()).thenReturn(Map.of(
            "1",
            new StreamsRebalanceData.Subtopology(
                Set.of("source1"),
                Set.of(),
                Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                Map.of(),
                Set.of()
            ),
            "2",
            new StreamsRebalanceData.Subtopology(
                Set.of("source2"),
                Set.of(),
                Map.of("repartition2", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                Map.of(),
                Set.of()
            ),
            "3",
            new StreamsRebalanceData.Subtopology(
                Set.of("source3"),
                Set.of(),
                Map.of("repartition3", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                Map.of(),
                Set.of()
            )
        ));
        createRebalanceListenerWithRebalanceData(streamsRebalanceData);

        final StreamsRebalanceData.Assignment assignment = new StreamsRebalanceData.Assignment(
            Set.of(new StreamsRebalanceData.TaskId("1", 0)),
            Set.of(new StreamsRebalanceData.TaskId("2", 0)),
            Set.of(new StreamsRebalanceData.TaskId("3", 0)),
            false
        );

        assertDoesNotThrow(() -> defaultStreamsRebalanceListener.onTasksAssigned(assignment));

        final InOrder inOrder = inOrder(taskManager, streamThread, streamsRebalanceData);
        inOrder.verify(streamThread).setStreamsGroupReady(false);
        inOrder.verify(taskManager).handleAssignment(
            Map.of(new TaskId(1, 0), Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))),
            Map.of(
                new TaskId(2, 0), Set.of(new TopicPartition("source2", 0), new TopicPartition("repartition2", 0)),
                new TaskId(3, 0), Set.of(new TopicPartition("source3", 0), new TopicPartition("repartition3", 0))
            )
        );
        inOrder.verify(streamThread).setState(StreamThread.State.PARTITIONS_ASSIGNED);
        inOrder.verify(taskManager).handleRebalanceComplete();
        inOrder.verify(streamsRebalanceData).setReconciledAssignment(assignment);
    }

    @Test
    void testOnTasksAssignedWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleAssignment(any(), any());

        final StreamsRebalanceData streamsRebalanceData = mock(StreamsRebalanceData.class);
        when(streamsRebalanceData.subtopologies()).thenReturn(Map.of());
        createRebalanceListenerWithRebalanceData(streamsRebalanceData);

        final Exception actualException = assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onTasksAssigned(
            new StreamsRebalanceData.Assignment(Set.of(), Set.of(), Set.of(), false)
        ));

        assertEquals(exception, actualException);
        verify(streamThread).setStreamsGroupReady(false);
        verify(taskManager).handleAssignment(any(), any());
        verify(streamThread, never()).setState(StreamThread.State.PARTITIONS_ASSIGNED);
        verify(taskManager, never()).handleRebalanceComplete();
        verify(streamsRebalanceData, never()).setReconciledAssignment(any());
    }

    @Test
    void testOnAllTasksLost() {
        final StreamsRebalanceData streamsRebalanceData = mock(StreamsRebalanceData.class);
        when(streamsRebalanceData.subtopologies()).thenReturn(Map.of());
        createRebalanceListenerWithRebalanceData(streamsRebalanceData);
        
        assertDoesNotThrow(() -> defaultStreamsRebalanceListener.onAllTasksLost());
        
        final InOrder inOrder = inOrder(taskManager, streamsRebalanceData);
        inOrder.verify(taskManager).handleLostAll();
        inOrder.verify(streamsRebalanceData).setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
    }

    @Test
    void testOnAllTasksLostWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleLostAll();

        final StreamsRebalanceData streamsRebalanceData = mock(StreamsRebalanceData.class);
        when(streamsRebalanceData.subtopologies()).thenReturn(Map.of());
        createRebalanceListenerWithRebalanceData(streamsRebalanceData);

        final Exception actualException = assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onAllTasksLost());

        assertEquals(exception, actualException);
        verify(taskManager).handleLostAll();
        verify(streamsRebalanceData, never()).setReconciledAssignment(any());
    }

    @Test
    void testOnTasksRevokedRecordsMetrics() {
        // Mock handleRevocation to simulate time passing
        doAnswer(invocation -> {
            mockTime.sleep(100); // Simulate task revocation taking 100ms
            return null;
        }).when(taskManager).handleRevocation(any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));

        defaultStreamsRebalanceListener.onTasksRevoked(
            Set.of(new StreamsRebalanceData.TaskId("1", 0))
        );

        verify(tasksRevokedSensor).record(100L);
        verify(taskManager).handleRevocation(
            Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))
        );
    }

    @Test
    void testOnTasksAssignedRecordsMetrics() {
        // Mock handleAssignment to simulate time passing
        doAnswer(invocation -> {
            mockTime.sleep(150); // Simulate task assignment taking 150ms
            return null;
        }).when(taskManager).handleAssignment(any(), any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));

        defaultStreamsRebalanceListener.onTasksAssigned(
            new StreamsRebalanceData.Assignment(
                Set.of(new StreamsRebalanceData.TaskId("1", 0)),
                Set.of(),
                Set.of(),
                true
            )
        );

        verify(tasksAssignedSensor).record(150L);
        verify(streamThread).setStreamsGroupReady(true);
        verify(taskManager).handleAssignment(
            Map.of(new TaskId(1, 0), Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))),
            Map.of()
        );
        verify(streamThread).setState(StreamThread.State.PARTITIONS_ASSIGNED);
        verify(taskManager).handleRebalanceComplete();
    }

    @Test
    void testOnAllTasksLostRecordsMetrics() {
        // Mock handleLostAll to simulate time passing
        doAnswer(invocation -> {
            mockTime.sleep(200); // Simulate task lost handling taking 200ms
            return null;
        }).when(taskManager).handleLostAll();

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));

        defaultStreamsRebalanceListener.onAllTasksLost();

        verify(tasksLostSensor).record(200L);
        verify(taskManager).handleLostAll();
    }

    @Test
    void testOnTasksRevokedRecordsMetricsEvenWithException() {
        final Exception exception = new RuntimeException("sample exception");
        // Mock handleRevocation to first advance time, then throw exception
        doAnswer(invocation -> {
            mockTime.sleep(50); // Simulate some work before exception
            throw exception;
        }).when(taskManager).handleRevocation(any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));

        assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onTasksRevoked(
            Set.of(new StreamsRebalanceData.TaskId("1", 0))
        ));

        verify(tasksRevokedSensor).record(50L);
        verify(taskManager).handleRevocation(any());
    }

    @Test
    void testOnTasksAssignedRecordsMetricsEvenWithException() {
        final Exception exception = new RuntimeException("sample exception");
        // Mock handleAssignment to first advance time, then throw exception
        doAnswer(invocation -> {
            mockTime.sleep(75); // Simulate some work before exception
            throw exception;
        }).when(taskManager).handleAssignment(any(), any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));

        assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onTasksAssigned(
            new StreamsRebalanceData.Assignment(Set.of(), Set.of(), Set.of(), false)
        ));

        verify(tasksAssignedSensor).record(75L);
        verify(streamThread).setStreamsGroupReady(false);
        verify(taskManager).handleAssignment(any(), any());
    }

    @Test
    void testOnAllTasksLostRecordsMetricsEvenWithException() {
        final Exception exception = new RuntimeException("sample exception");
        // Mock handleLostAll to first advance time, then throw exception
        doAnswer(invocation -> {
            mockTime.sleep(125); // Simulate some work before exception
            throw exception;
        }).when(taskManager).handleLostAll();

        final StreamsRebalanceData streamsRebalanceData = mock(StreamsRebalanceData.class);
        when(streamsRebalanceData.subtopologies()).thenReturn(Map.of());
        createRebalanceListenerWithRebalanceData(streamsRebalanceData);

        final Exception actualException = assertThrows(RuntimeException.class, () -> defaultStreamsRebalanceListener.onAllTasksLost());

        assertEquals(exception, actualException);
        verify(tasksLostSensor).record(125L);
        verify(taskManager).handleLostAll();
        verify(streamsRebalanceData, never()).setReconciledAssignment(any());
    }
}
