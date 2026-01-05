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

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StreamsRebalanceListenerInvokerTest {

    @Mock
    private StreamsRebalanceListener mockListener;

    @Mock
    private StreamsRebalanceData streamsRebalanceData;

    private StreamsRebalanceListenerInvoker invoker;
    private final LogContext logContext = new LogContext();

    @BeforeEach
    public void setup() {
        invoker = new StreamsRebalanceListenerInvoker(logContext, streamsRebalanceData);
    }

    @Test
    public void testSetRebalanceListenerWithNull() {
        NullPointerException exception = assertThrows(NullPointerException.class,
            () -> invoker.setRebalanceListener(null));
        assertEquals("StreamsRebalanceListener cannot be null", exception.getMessage());
    }

    @Test
    public void testSetRebalanceListenerOverwritesExisting() {
        StreamsRebalanceListener firstListener = org.mockito.Mockito.mock(StreamsRebalanceListener.class);
        StreamsRebalanceListener secondListener = org.mockito.Mockito.mock(StreamsRebalanceListener.class);

        StreamsRebalanceData.Assignment mockAssignment = createMockAssignment();
        when(streamsRebalanceData.reconciledAssignment()).thenReturn(mockAssignment);

        // Set first listener
        invoker.setRebalanceListener(firstListener);

        // Overwrite with second listener
        invoker.setRebalanceListener(secondListener);

        // Should use second listener
        invoker.invokeAllTasksRevoked();
        verify(firstListener, never()).onTasksRevoked(any());
        verify(secondListener).onTasksRevoked(eq(mockAssignment.activeTasks()));
    }

    @Test
    public void testInvokeMethodsWithNoListener() {
        assertNull(invoker.invokeAllTasksRevoked());
        assertNull(invoker.invokeTasksAssigned(createMockAssignment()));
        assertNull(invoker.invokeTasksRevoked(createMockTasks()));
        assertNull(invoker.invokeAllTasksLost());
    }

    @Test
    public void testInvokeAllTasksRevokedWithListener() {
        invoker.setRebalanceListener(mockListener);

        StreamsRebalanceData.Assignment mockAssignment = createMockAssignment();
        when(streamsRebalanceData.reconciledAssignment()).thenReturn(mockAssignment);

        Exception result = invoker.invokeAllTasksRevoked();

        assertNull(result);
        verify(mockListener).onTasksRevoked(eq(mockAssignment.activeTasks()));
    }

    @Test
    public void testInvokeTasksAssignedWithListener() {
        invoker.setRebalanceListener(mockListener);
        StreamsRebalanceData.Assignment assignment = createMockAssignment();

        Exception result = invoker.invokeTasksAssigned(assignment);

        assertNull(result);
        verify(mockListener).onTasksAssigned(eq(assignment));
    }

    @Test
    public void testInvokeTasksAssignedWithWakeupException() {
        invoker.setRebalanceListener(mockListener);
        StreamsRebalanceData.Assignment assignment = createMockAssignment();
        WakeupException wakeupException = new WakeupException();
        doThrow(wakeupException).when(mockListener).onTasksAssigned(assignment);

        WakeupException thrownException = assertThrows(WakeupException.class,
            () -> invoker.invokeTasksAssigned(assignment));

        assertEquals(wakeupException, thrownException);
        verify(mockListener).onTasksAssigned(eq(assignment));
    }

    @Test
    public void testInvokeTasksAssignedWithInterruptException() {
        invoker.setRebalanceListener(mockListener);
        StreamsRebalanceData.Assignment assignment = createMockAssignment();
        InterruptException interruptException = new InterruptException("Test interrupt");
        doThrow(interruptException).when(mockListener).onTasksAssigned(assignment);

        InterruptException thrownException = assertThrows(InterruptException.class,
            () -> invoker.invokeTasksAssigned(assignment));

        assertEquals(interruptException, thrownException);
        verify(mockListener).onTasksAssigned(eq(assignment));
    }

    @Test
    public void testInvokeTasksAssignedWithOtherException() {
        invoker.setRebalanceListener(mockListener);
        StreamsRebalanceData.Assignment assignment = createMockAssignment();
        RuntimeException runtimeException = new RuntimeException("Test exception");
        doThrow(runtimeException).when(mockListener).onTasksAssigned(assignment);

        Exception result = invoker.invokeTasksAssigned(assignment);

        assertEquals(runtimeException, result);
        verify(mockListener).onTasksAssigned(eq(assignment));
    }

    @Test
    public void testInvokeTasksRevokedWithListener() {
        invoker.setRebalanceListener(mockListener);
        Set<StreamsRebalanceData.TaskId> tasks = createMockTasks();

        Exception result = invoker.invokeTasksRevoked(tasks);

        assertNull(result);
        verify(mockListener).onTasksRevoked(eq(tasks));
    }

    @Test
    public void testInvokeTasksRevokedWithWakeupException() {
        invoker.setRebalanceListener(mockListener);
        Set<StreamsRebalanceData.TaskId> tasks = createMockTasks();
        WakeupException wakeupException = new WakeupException();
        doThrow(wakeupException).when(mockListener).onTasksRevoked(tasks);

        WakeupException thrownException = assertThrows(WakeupException.class,
            () -> invoker.invokeTasksRevoked(tasks));

        assertEquals(wakeupException, thrownException);
        verify(mockListener).onTasksRevoked(eq(tasks));
    }

    @Test
    public void testInvokeTasksRevokedWithInterruptException() {
        invoker.setRebalanceListener(mockListener);
        Set<StreamsRebalanceData.TaskId> tasks = createMockTasks();
        InterruptException interruptException = new InterruptException("Test interrupt");
        doThrow(interruptException).when(mockListener).onTasksRevoked(tasks);

        InterruptException thrownException = assertThrows(InterruptException.class,
            () -> invoker.invokeTasksRevoked(tasks));

        assertEquals(interruptException, thrownException);
        verify(mockListener).onTasksRevoked(eq(tasks));
    }

    @Test
    public void testInvokeTasksRevokedWithOtherException() {
        invoker.setRebalanceListener(mockListener);
        Set<StreamsRebalanceData.TaskId> tasks = createMockTasks();
        RuntimeException runtimeException = new RuntimeException("Test exception");
        doThrow(runtimeException).when(mockListener).onTasksRevoked(tasks);

        Exception result = invoker.invokeTasksRevoked(tasks);

        assertEquals(runtimeException, result);
        verify(mockListener).onTasksRevoked(eq(tasks));
    }

    @Test
    public void testInvokeAllTasksLostWithListener() {
        invoker.setRebalanceListener(mockListener);

        Exception result = invoker.invokeAllTasksLost();

        assertNull(result);
        verify(mockListener).onAllTasksLost();
    }

    @Test
    public void testInvokeAllTasksLostWithWakeupException() {
        invoker.setRebalanceListener(mockListener);
        WakeupException wakeupException = new WakeupException();
        doThrow(wakeupException).when(mockListener).onAllTasksLost();

        WakeupException thrownException = assertThrows(WakeupException.class,
            () -> invoker.invokeAllTasksLost());

        assertEquals(wakeupException, thrownException);
        verify(mockListener).onAllTasksLost();
    }

    @Test
    public void testInvokeAllTasksLostWithInterruptException() {
        invoker.setRebalanceListener(mockListener);
        InterruptException interruptException = new InterruptException("Test interrupt");
        doThrow(interruptException).when(mockListener).onAllTasksLost();

        InterruptException thrownException = assertThrows(InterruptException.class,
            () -> invoker.invokeAllTasksLost());

        assertEquals(interruptException, thrownException);
        verify(mockListener).onAllTasksLost();
    }

    @Test
    public void testInvokeAllTasksLostWithOtherException() {
        invoker.setRebalanceListener(mockListener);
        RuntimeException runtimeException = new RuntimeException("Test exception");
        doThrow(runtimeException).when(mockListener).onAllTasksLost();

        Exception result = invoker.invokeAllTasksLost();

        assertEquals(runtimeException, result);
        verify(mockListener).onAllTasksLost();
    }

    private StreamsRebalanceData.Assignment createMockAssignment() {
        Set<StreamsRebalanceData.TaskId> activeTasks = createMockTasks();
        Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of();
        Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of();

        return new StreamsRebalanceData.Assignment(activeTasks, standbyTasks, warmupTasks, true);
    }

    private Set<StreamsRebalanceData.TaskId> createMockTasks() {
        return Set.of(
            new StreamsRebalanceData.TaskId("subtopology1", 0),
            new StreamsRebalanceData.TaskId("subtopology1", 1)
        );
    }

}
