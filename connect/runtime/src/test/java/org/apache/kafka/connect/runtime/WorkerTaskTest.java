/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class WorkerTaskTest {

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    @Test
    public void standardStartup() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        TaskStatus.Listener statusListener = EasyMock.createMock(TaskStatus.Listener.class);

        WorkerTask workerTask = partialMockBuilder(WorkerTask.class)
                .withConstructor(ConnectorTaskId.class, TaskStatus.Listener.class, TargetState.class)
                .withArgs(taskId, statusListener, TargetState.STARTED)
                .addMockedMethod("initialize")
                .addMockedMethod("execute")
                .addMockedMethod("close")
                .createStrictMock();

        workerTask.initialize(TASK_CONFIG);
        expectLastCall();

        workerTask.execute();
        expectLastCall();

        statusListener.onStartup(taskId);
        expectLastCall();

        workerTask.close();
        expectLastCall();

        statusListener.onShutdown(taskId);
        expectLastCall();

        replay(workerTask);

        workerTask.initialize(TASK_CONFIG);
        workerTask.run();
        workerTask.stop();
        workerTask.awaitStop(1000L);

        verify(workerTask);
    }

    @Test
    public void stopBeforeStarting() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        TaskStatus.Listener statusListener = EasyMock.createMock(TaskStatus.Listener.class);

        WorkerTask workerTask = partialMockBuilder(WorkerTask.class)
                .withConstructor(ConnectorTaskId.class, TaskStatus.Listener.class, TargetState.class)
                .withArgs(taskId, statusListener, TargetState.STARTED)
                .addMockedMethod("initialize")
                .addMockedMethod("execute")
                .addMockedMethod("close")
                .createStrictMock();

        workerTask.initialize(TASK_CONFIG);
        EasyMock.expectLastCall();

        workerTask.close();
        EasyMock.expectLastCall();

        replay(workerTask);

        workerTask.initialize(TASK_CONFIG);
        workerTask.stop();
        workerTask.awaitStop(1000L);

        // now run should not do anything
        workerTask.run();

        verify(workerTask);
    }

    @Test
    public void cancelBeforeStopping() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        TaskStatus.Listener statusListener = EasyMock.createMock(TaskStatus.Listener.class);

        WorkerTask workerTask = partialMockBuilder(WorkerTask.class)
                .withConstructor(ConnectorTaskId.class, TaskStatus.Listener.class, TargetState.class)
                .withArgs(taskId, statusListener, TargetState.STARTED)
                .addMockedMethod("initialize")
                .addMockedMethod("execute")
                .addMockedMethod("close")
                .createStrictMock();

        final CountDownLatch stopped = new CountDownLatch(1);
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    stopped.await();
                } catch (Exception e) {
                }
            }
        };

        workerTask.initialize(TASK_CONFIG);
        EasyMock.expectLastCall();

        workerTask.execute();
        expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                thread.start();
                return null;
            }
        });

        statusListener.onStartup(taskId);
        expectLastCall();

        workerTask.close();
        expectLastCall();

        // there should be no call to onShutdown()

        replay(workerTask);

        workerTask.initialize(TASK_CONFIG);
        workerTask.run();

        workerTask.stop();
        workerTask.cancel();
        stopped.countDown();
        thread.join();

        verify(workerTask);
    }

    private static abstract class TestSinkTask extends SinkTask {
    }

}
