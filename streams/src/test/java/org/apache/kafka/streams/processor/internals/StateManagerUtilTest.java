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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StateManagerUtilTest {

    @Mock
    private ProcessorStateManager stateManager;

    @Mock
    private StateDirectory stateDirectory;

    @Mock
    private ProcessorTopology topology;

    @Mock
    private InternalProcessorContext processorContext;

    private final Logger logger = new LogContext("test").logger(AbstractTask.class);

    private final TaskId taskId = new TaskId(0, 0);

    @Test
    public void testRegisterStateStoreWhenTopologyEmpty() {
        when(topology.stateStores()).thenReturn(emptyList());

        StateManagerUtil.registerStateStores(logger,
            "logPrefix:", topology, stateManager, stateDirectory, processorContext);
    }

    @Test
    public void testRegisterStateStoreFailToLockStateDirectory() {
        when(topology.stateStores()).thenReturn(singletonList(new MockKeyValueStore("store", false)));
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(false);

        final LockException thrown = assertThrows(LockException.class,
            () -> StateManagerUtil.registerStateStores(logger, "logPrefix:",
                topology, stateManager, stateDirectory, processorContext));

        assertEquals("logPrefix:Failed to lock the state directory for task 0_0", thrown.getMessage());
    }

    @Test
    public void testRegisterStateStores() {
        final MockKeyValueStore store1 = new MockKeyValueStore("store1", false);
        final MockKeyValueStore store2 = new MockKeyValueStore("store2", false);
        final List<StateStore> stateStores = Arrays.asList(store1, store2);
        final InOrder inOrder = inOrder(stateManager);
        when(topology.stateStores()).thenReturn(stateStores);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        when(stateDirectory.directoryForTaskIsEmpty(taskId)).thenReturn(true);
        when(topology.stateStores()).thenReturn(stateStores);

        StateManagerUtil.registerStateStores(logger, "logPrefix:",
            topology, stateManager, stateDirectory, processorContext);

        inOrder.verify(stateManager).registerStateStores(stateStores, processorContext);
        inOrder.verify(stateManager).initializeStoreOffsetsFromCheckpoint(true);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    public void testCloseStateManagerClean() {
        final InOrder inOrder = inOrder(stateManager, stateDirectory);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);

        StateManagerUtil.closeStateManager(logger,
            "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE);

        inOrder.verify(stateManager).close();
        inOrder.verify(stateDirectory).unlock(taskId);
        verifyNoMoreInteractions(stateManager, stateDirectory);
    }

    @Test
    public void testCloseStateManagerThrowsExceptionWhenClean() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        doThrow(new ProcessorStateException("state manager failed to close")).when(stateManager).close();

        final ProcessorStateException thrown = assertThrows(
            ProcessorStateException.class, () -> StateManagerUtil.closeStateManager(logger,
                "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE));

        // Thrown stateMgr exception will not be wrapped.
        assertEquals("state manager failed to close", thrown.getMessage());

        // The unlock logic should still be executed.
        verify(stateDirectory).unlock(taskId);
    }

    @Test
    public void testCloseStateManagerThrowsExceptionWhenDirty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        doThrow(new ProcessorStateException("state manager failed to close")).when(stateManager).close();

        assertThrows(
            ProcessorStateException.class,
            () -> StateManagerUtil.closeStateManager(
                logger, "logPrefix:", false, false, stateManager, stateDirectory, TaskType.ACTIVE));

        verify(stateDirectory).unlock(taskId);
    }

    @Test
    public void testCloseStateManagerWithStateStoreWipeOut() {
        final InOrder inOrder = inOrder(stateManager, stateDirectory);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        // The `baseDir` will be accessed when attempting to delete the state store.
        when(stateManager.baseDir()).thenReturn(TestUtils.tempDirectory("state_store"));

        StateManagerUtil.closeStateManager(logger,
            "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE);

        inOrder.verify(stateManager).close();
        inOrder.verify(stateDirectory).unlock(taskId);
        verifyNoMoreInteractions(stateManager, stateDirectory);
    }

    @Test
    public void  shouldStillWipeStateStoresIfCloseThrowsException() {
        final File randomFile = new File("/random/path");

        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        doThrow(new ProcessorStateException("Close failed")).when(stateManager).close();
        when(stateManager.baseDir()).thenReturn(randomFile);

        try (MockedStatic<Utils> utils = mockStatic(Utils.class)) {
            assertThrows(ProcessorStateException.class, () ->
                    StateManagerUtil.closeStateManager(logger, "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE));
        }

        verify(stateDirectory).unlock(taskId);
    }

    @Test
    public void testCloseStateManagerWithStateStoreWipeOutRethrowWrappedIOException() {
        final File unknownFile = new File("/unknown/path");
        final InOrder inOrder = inOrder(stateManager, stateDirectory);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        when(stateManager.baseDir()).thenReturn(unknownFile);

        try (MockedStatic<Utils> utils = mockStatic(Utils.class)) {
            utils.when(() -> Utils.delete(unknownFile)).thenThrow(new IOException("Deletion failed"));

            final ProcessorStateException thrown = assertThrows(
                    ProcessorStateException.class, () -> StateManagerUtil.closeStateManager(logger,
                            "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE));

            assertEquals(IOException.class, thrown.getCause().getClass());
        }

        inOrder.verify(stateManager).close();
        inOrder.verify(stateDirectory).unlock(taskId);
        verifyNoMoreInteractions(stateManager, stateDirectory);
    }

    @Test
    public void shouldNotCloseStateManagerIfUnableToLockTaskDirectory() {
        final InOrder inOrder = inOrder(stateManager, stateDirectory);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(false);

        StateManagerUtil.closeStateManager(
                logger, "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE);

        inOrder.verify(stateManager).taskId();
        inOrder.verify(stateDirectory).lock(taskId);
        verify(stateManager, never()).close();
        verify(stateManager, never()).baseDir();
        verify(stateDirectory, never()).unlock(taskId);
        verifyNoMoreInteractions(stateManager, stateDirectory);
    }

    @Test
    public void shouldNotWipeStateStoresIfUnableToLockTaskDirectory() {
        final InOrder inOrder = inOrder(stateManager, stateDirectory);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateDirectory.lock(taskId)).thenReturn(false);

        StateManagerUtil.closeStateManager(
                logger, "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE);

        inOrder.verify(stateManager).taskId();
        inOrder.verify(stateDirectory).lock(taskId);
        verify(stateManager, never()).close();
        verify(stateManager, never()).baseDir();
        verify(stateDirectory, never()).unlock(taskId);
        verifyNoMoreInteractions(stateManager, stateDirectory);
    }
}
