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

import java.util.List;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.easymock.IMocksControl;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.createStrictControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Utils.class)
public class StateManagerUtilTest {

    @Mock(type = MockType.NICE)
    private ProcessorStateManager stateManager;

    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;

    @Mock(type = MockType.NICE)
    private ProcessorTopology topology;

    @Mock(type = MockType.NICE)
    private InternalProcessorContext processorContext;

    private IMocksControl ctrl;

    private Logger logger = new LogContext("test").logger(AbstractTask.class);

    private final TaskId taskId = new TaskId(0, 0);

    @Before
    public void setup() {
        ctrl = createStrictControl();
        topology = ctrl.createMock(ProcessorTopology.class);
        processorContext = ctrl.createMock(InternalProcessorContext.class);

        stateManager = ctrl.createMock(ProcessorStateManager.class);
        stateDirectory = ctrl.createMock(StateDirectory.class);
    }

    @Test
    public void testRegisterStateStoreWhenTopologyEmpty() {
        expect(topology.stateStores()).andReturn(emptyList());

        ctrl.checkOrder(true);
        ctrl.replay();

        StateManagerUtil.registerStateStores(logger,
            "logPrefix:", topology, stateManager, stateDirectory, processorContext);

        ctrl.verify();
    }

    @Test
    public void testRegisterStateStoreFailToLockStateDirectory() {
        expect(topology.stateStores()).andReturn(singletonList(new MockKeyValueStore("store", false)));

        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(false);

        ctrl.checkOrder(true);
        ctrl.replay();

        final LockException thrown = assertThrows(LockException.class,
            () -> StateManagerUtil.registerStateStores(logger, "logPrefix:",
                topology, stateManager, stateDirectory, processorContext));

        assertEquals("logPrefix:Failed to lock the state directory for task 0_0", thrown.getMessage());

        ctrl.verify();
    }

    @Test
    public void testRegisterStateStores() {
        final MockKeyValueStore store1 = new MockKeyValueStore("store1", false);
        final MockKeyValueStore store2 = new MockKeyValueStore("store2", false);
        final List<StateStore> stateStores = Arrays.asList(store1, store2);

        expect(topology.stateStores()).andReturn(stateStores);

        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(true);
        expect(stateDirectory.directoryForTaskIsEmpty(taskId)).andReturn(true);

        expect(topology.stateStores()).andReturn(stateStores);

        stateManager.registerStateStores(stateStores, processorContext);

        stateManager.initializeStoreOffsetsFromCheckpoint(true);
        expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        StateManagerUtil.registerStateStores(logger, "logPrefix:",
            topology, stateManager, stateDirectory, processorContext);

        ctrl.verify();
    }

    @Test
    public void testCloseStateManagerClean() {
        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall();

        stateDirectory.unlock(taskId);
        expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        StateManagerUtil.closeStateManager(logger,
            "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE);

        ctrl.verify();
    }

    @Test
    public void testCloseStateManagerThrowsExceptionWhenClean() {
        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall().andThrow(new ProcessorStateException("state manager failed to close"));

        // The unlock logic should still be executed.
        stateDirectory.unlock(taskId);

        ctrl.checkOrder(true);
        ctrl.replay();

        final ProcessorStateException thrown = assertThrows(
            ProcessorStateException.class, () -> StateManagerUtil.closeStateManager(logger,
                "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE));

        // Thrown stateMgr exception will not be wrapped.
        assertEquals("state manager failed to close", thrown.getMessage());

        ctrl.verify();
    }

    @Test
    public void testCloseStateManagerThrowsExceptionWhenDirty() {
        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall().andThrow(new ProcessorStateException("state manager failed to close"));

        stateDirectory.unlock(taskId);

        ctrl.checkOrder(true);
        ctrl.replay();

        assertThrows(
            ProcessorStateException.class,
            () -> StateManagerUtil.closeStateManager(
                logger, "logPrefix:", false, false, stateManager, stateDirectory, TaskType.ACTIVE));

        ctrl.verify();
    }

    @Test
    public void testCloseStateManagerWithStateStoreWipeOut() {
        expect(stateManager.taskId()).andReturn(taskId);
        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall();

        // The `baseDir` will be accessed when attempting to delete the state store.
        expect(stateManager.baseDir()).andReturn(TestUtils.tempDirectory("state_store"));

        stateDirectory.unlock(taskId);
        expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        StateManagerUtil.closeStateManager(logger,
            "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE);

        ctrl.verify();
    }

    @Test
    public void  shouldStillWipeStateStoresIfCloseThrowsException() throws IOException {
        final File randomFile = new File("/random/path");
        mockStatic(Utils.class);

        expect(stateManager.taskId()).andReturn(taskId);
        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall().andThrow(new ProcessorStateException("Close failed"));

        expect(stateManager.baseDir()).andReturn(randomFile);

        Utils.delete(randomFile);

        stateDirectory.unlock(taskId);
        expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        replayAll();

        assertThrows(ProcessorStateException.class, () ->
            StateManagerUtil.closeStateManager(logger, "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE));

        ctrl.verify();
    }

    @Test
    public void testCloseStateManagerWithStateStoreWipeOutRethrowWrappedIOException() throws IOException {
        final File unknownFile = new File("/unknown/path");
        mockStatic(Utils.class);

        expect(stateManager.taskId()).andReturn(taskId);
        expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        expectLastCall();

        expect(stateManager.baseDir()).andReturn(unknownFile);

        Utils.delete(unknownFile);
        expectLastCall().andThrow(new IOException("Deletion failed"));

        stateDirectory.unlock(taskId);
        expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        replayAll();

        final ProcessorStateException thrown = assertThrows(
            ProcessorStateException.class, () -> StateManagerUtil.closeStateManager(logger,
                "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE));

        assertEquals(IOException.class, thrown.getCause().getClass());

        ctrl.verify();
    }

    @Test
    public void shouldNotCloseStateManagerIfUnableToLockTaskDirectory() {
        expect(stateManager.taskId()).andReturn(taskId);

        expect(stateDirectory.lock(taskId)).andReturn(false);

        stateManager.close();
        expectLastCall().andThrow(new AssertionError("Should not be trying to close state you don't own!"));

        ctrl.checkOrder(true);
        ctrl.replay();

        replayAll();

        StateManagerUtil.closeStateManager(
            logger, "logPrefix:", true, false, stateManager, stateDirectory, TaskType.ACTIVE);
    }

    @Test
    public void shouldNotWipeStateStoresIfUnableToLockTaskDirectory() throws IOException {
        final File unknownFile = new File("/unknown/path");
        expect(stateManager.taskId()).andReturn(taskId);
        expect(stateDirectory.lock(taskId)).andReturn(false);

        expect(stateManager.baseDir()).andReturn(unknownFile);

        Utils.delete(unknownFile);
        expectLastCall().andThrow(new AssertionError("Should not be trying to wipe state you don't own!"));

        ctrl.checkOrder(true);
        ctrl.replay();

        replayAll();

        StateManagerUtil.closeStateManager(
            logger, "logPrefix:", false, true, stateManager, stateDirectory, TaskType.ACTIVE);
    }
}
