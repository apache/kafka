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

import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class WorkerTaskTest {

    private static final Map<String, String> EMPTY_TASK_PROPS = Collections.emptyMap();

    @Test
    public void standardStartup() {
        WorkerTask workerTask = partialMockBuilder(WorkerTask.class)
                .withConstructor(ConnectorTaskId.class)
                .withArgs(new ConnectorTaskId("foo", 0))
                .addMockedMethod("initialize")
                .addMockedMethod("execute")
                .addMockedMethod("close")
                .createStrictMock();

        workerTask.initialize(EMPTY_TASK_PROPS);
        EasyMock.expectLastCall();

        workerTask.execute();
        EasyMock.expectLastCall();

        workerTask.close();
        EasyMock.expectLastCall();

        replay(workerTask);

        workerTask.initialize(EMPTY_TASK_PROPS);
        workerTask.run();
        workerTask.stop();
        workerTask.awaitStop(1000L);

        verify(workerTask);
    }

    @Test
    public void stopBeforeStarting() {
        WorkerTask workerTask = partialMockBuilder(WorkerTask.class)
                .withConstructor(ConnectorTaskId.class)
                .withArgs(new ConnectorTaskId("foo", 0))
                .addMockedMethod("initialize")
                .addMockedMethod("execute")
                .addMockedMethod("close")
                .createStrictMock();

        workerTask.initialize(EMPTY_TASK_PROPS);
        EasyMock.expectLastCall();

        workerTask.close();
        EasyMock.expectLastCall();

        replay(workerTask);

        workerTask.initialize(EMPTY_TASK_PROPS);
        workerTask.stop();
        workerTask.awaitStop(1000L);

        // now run should not do anything
        workerTask.run();

        verify(workerTask);
    }


}
