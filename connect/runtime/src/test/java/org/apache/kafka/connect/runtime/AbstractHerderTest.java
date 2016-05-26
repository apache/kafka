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

import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractHerderTest extends EasyMockSupport {

    @Test
    public void connectorStatus() {
        Worker worker = null;
        String workerId = "workerId";
        String connector = "connector";
        int generation = 5;
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, StatusBackingStore.class, ConfigBackingStore.class)
                .withArgs(worker, workerId, statusStore, configStore)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        replayAll();


        ConnectorStateInfo state = herder.connectorStatus(connector);

        assertEquals(connector, state.name());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());

        verifyAll();
    }

    @Test
    public void taskStatus() {
        Worker worker = null;
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        String workerId = "workerId";

        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, StatusBackingStore.class, ConfigBackingStore.class)
                .withArgs(worker, workerId, statusStore, configStore)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(5);

        final Capture<TaskStatus> statusCapture = EasyMock.newCapture();
        statusStore.putSafe(EasyMock.capture(statusCapture));
        EasyMock.expectLastCall();

        EasyMock.expect(statusStore.get(taskId)).andAnswer(new IAnswer<TaskStatus>() {
            @Override
            public TaskStatus answer() throws Throwable {
                return statusCapture.getValue();
            }
        });

        replayAll();

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());

        verifyAll();
    }
}
