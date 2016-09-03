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

package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.MemoryConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@SuppressWarnings("unchecked")
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final List<String> TOPICS_LIST = Arrays.asList("topic1", "topic2");
    private static final String TOPICS_LIST_STR = "topic1,topic2";
    private static final int DEFAULT_MAX_TASKS = 1;
    private static final String WORKER_ID = "localhost:8083";

    private enum SourceSink {
        SOURCE, SINK
    };

    private StandaloneHerder herder;

    private Connector connector;
    @Mock protected Worker worker;
    @Mock protected Callback<Herder.Created<ConnectorInfo>> createCallback;
    @Mock protected StatusBackingStore statusBackingStore;

    @Before
    public void setup() {
        herder = new StandaloneHerder(worker, WORKER_ID, statusBackingStore, new MemoryConfigBackingStore());
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        // First addition should succeed
        expectAdd(SourceSink.SOURCE);

        // Second should fail
        createCallback.onCompletion(EasyMock.<AlreadyExistsException>anyObject(), EasyMock.<Herder.Created<ConnectorInfo>>isNull());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);
        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = PowerMock.createMock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SINK), false, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        EasyMock.expect(statusBackingStore.getAll(CONNECTOR_NAME)).andReturn(Collections.<TaskStatus>emptyList());
        statusBackingStore.put(new ConnectorStatus(CONNECTOR_NAME, AbstractStatus.State.DESTROYED, WORKER_ID, 0));

        expectDestroy();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);
        FutureCallback<Herder.Created<ConnectorInfo>> futureCb = new FutureCallback<>();
        herder.putConnectorConfig(CONNECTOR_NAME, null, true, futureCb);
        futureCb.get(1000L, TimeUnit.MILLISECONDS);

        // Second deletion should fail since the connector is gone
        futureCb = new FutureCallback<>();
        herder.putConnectorConfig(CONNECTOR_NAME, null, true, futureCb);
        try {
            futureCb.get(1000L, TimeUnit.MILLISECONDS);
            fail("Should have thrown NotFoundException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnector() throws Exception {
        expectAdd(SourceSink.SOURCE);

        worker.stopConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(true);

        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(connectorConfig(SourceSink.SOURCE)),
                EasyMock.anyObject(HerderConnectorContext.class), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        EasyMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, cb);
        cb.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorFailureOnStart() throws Exception {
        expectAdd(SourceSink.SOURCE);

        worker.stopConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(true);

        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(connectorConfig(SourceSink.SOURCE)),
                EasyMock.anyObject(HerderConnectorContext.class), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        EasyMock.expectLastCall().andReturn(false);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, cb);
        try {
            cb.get(1000L, TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException exception) {
            assertEquals(ConnectException.class, exception.getCause().getClass());
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTask() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        worker.stopAndAwaitTask(taskId);
        EasyMock.expectLastCall().andReturn(true);

        worker.startTask(taskId, connectorConfig(SourceSink.SOURCE), taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartTask(taskId, cb);
        cb.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskFailureOnStart() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        worker.stopAndAwaitTask(taskId);
        EasyMock.expectLastCall().andReturn(true);

        worker.startTask(taskId, connectorConfig(SourceSink.SOURCE), taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(false);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);

        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartTask(taskId, cb);
        try {
            cb.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected restart callback to raise an exception");
        } catch (ExecutionException exception) {
            assertEquals(ConnectException.class, exception.getCause().getClass());
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateAndStop() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);
        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        expectStop();

        statusBackingStore.stop();
        EasyMock.expectLastCall();
        worker.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE), false, createCallback);
        herder.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testAccessors() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);

        Callback<Collection<String>> listConnectorsCb = PowerMock.createMock(Callback.class);
        Callback<ConnectorInfo> connectorInfoCb = PowerMock.createMock(Callback.class);
        Callback<Map<String, String>> connectorConfigCb = PowerMock.createMock(Callback.class);
        Callback<List<TaskInfo>> taskConfigsCb = PowerMock.createMock(Callback.class);

        // Check accessors with empty worker
        listConnectorsCb.onCompletion(null, Collections.EMPTY_SET);
        EasyMock.expectLastCall();
        connectorInfoCb.onCompletion(EasyMock.<NotFoundException>anyObject(), EasyMock.<ConnectorInfo>isNull());
        EasyMock.expectLastCall();
        connectorConfigCb.onCompletion(EasyMock.<NotFoundException>anyObject(), EasyMock.<Map<String, String>>isNull());
        EasyMock.expectLastCall();
        taskConfigsCb.onCompletion(EasyMock.<NotFoundException>anyObject(), EasyMock.<List<TaskInfo>>isNull());
        EasyMock.expectLastCall();


        // Create connector
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        // Validate accessors with 1 connector
        listConnectorsCb.onCompletion(null, Collections.singleton(CONNECTOR_NAME));
        EasyMock.expectLastCall();
        ConnectorInfo connInfo = new ConnectorInfo(CONNECTOR_NAME, connConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)));
        connectorInfoCb.onCompletion(null, connInfo);
        EasyMock.expectLastCall();
        connectorConfigCb.onCompletion(null, connConfig);
        EasyMock.expectLastCall();

        TaskInfo taskInfo = new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE));
        taskConfigsCb.onCompletion(null, Arrays.asList(taskInfo));
        EasyMock.expectLastCall();


        PowerMock.replayAll();

        // All operations are synchronous for StandaloneHerder, so we don't need to actually wait after making each call
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        Map<String, String> newConnConfig = new HashMap<>(connConfig);
        newConnConfig.put("foo", "bar");

        Callback<Map<String, String>> connectorConfigCb = PowerMock.createMock(Callback.class);
        Callback<Herder.Created<ConnectorInfo>> putConnectorConfigCb = PowerMock.createMock(Callback.class);

        // Create
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);
        // Should get first config
        connectorConfigCb.onCompletion(null, connConfig);
        EasyMock.expectLastCall();
        // Update config, which requires stopping and restarting
        worker.stopConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(true);
        Capture<Map<String, String>> capturedConfig = EasyMock.newCapture();
        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(capturedConfig), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        EasyMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONNECTOR_NAME)).andReturn(true);
        // Generate same task config, which should result in no additional action to restart tasks
        EasyMock.expect(worker.connectorTaskConfigs(CONNECTOR_NAME, DEFAULT_MAX_TASKS, null))
                .andReturn(Collections.singletonList(taskConfig(SourceSink.SOURCE)));
        worker.isSinkConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(false);
        ConnectorInfo newConnInfo = new ConnectorInfo(CONNECTOR_NAME, newConnConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)));
        putConnectorConfigCb.onCompletion(null, new Herder.Created<>(false, newConnInfo));
        EasyMock.expectLastCall();
        // Should get new config
        connectorConfigCb.onCompletion(null, newConnConfig);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.putConnectorConfig(CONNECTOR_NAME, newConnConfig, true, putConnectorConfigCb);
        assertEquals("bar", capturedConfig.getValue().get("foo"));
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);

        PowerMock.verifyAll();

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutTaskConfigs() {
        Callback<Void> cb = PowerMock.createMock(Callback.class);

        PowerMock.replayAll();

        herder.putTaskConfigs(CONNECTOR_NAME,
                Arrays.asList(Collections.singletonMap("config", "value")),
                cb);

        PowerMock.verifyAll();
    }

    private void expectAdd(SourceSink sourceSink) throws Exception {

        Map<String, String> connectorProps = connectorConfig(sourceSink);

        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(connectorProps), EasyMock.anyObject(HerderConnectorContext.class),
                              EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        EasyMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONNECTOR_NAME)).andReturn(true);

        ConnectorInfo connInfo = new ConnectorInfo(CONNECTOR_NAME, connectorProps, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)));
        createCallback.onCompletion(null, new Herder.Created<>(true, connInfo));
        EasyMock.expectLastCall();

        // And we should instantiate the tasks. For a sink task, we should see added properties for the input topic partitions

        Map<String, String> generatedTaskProps = taskConfig(sourceSink);

        EasyMock.expect(worker.connectorTaskConfigs(CONNECTOR_NAME, DEFAULT_MAX_TASKS, sourceSink == SourceSink.SINK ? TOPICS_LIST : null))
            .andReturn(Collections.singletonList(generatedTaskProps));

        worker.startTask(new ConnectorTaskId(CONNECTOR_NAME, 0), connectorConfig(sourceSink), generatedTaskProps, herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(true);

        worker.isSinkConnector(CONNECTOR_NAME);
        PowerMock.expectLastCall().andReturn(sourceSink == SourceSink.SINK);
    }

    private void expectStop() {
        ConnectorTaskId task = new ConnectorTaskId(CONNECTOR_NAME, 0);
        worker.stopAndAwaitTasks(Collections.singletonList(task));
        EasyMock.expectLastCall().andReturn(Collections.singleton(task));
        worker.stopConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(true);
    }

    private void expectDestroy() {
        expectStop();
    }

    private static Map<String, String> connectorConfig(SourceSink sourceSink) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        Class<? extends Connector> connectorClass = sourceSink == SourceSink.SINK ? BogusSinkConnector.class : BogusSourceConnector.class;
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        if (sourceSink == SourceSink.SINK)
            props.put(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        return props;
    }

    private static Map<String, String> taskConfig(SourceSink sourceSink) {
        HashMap<String, String> generatedTaskProps = new HashMap<>();
        // Connectors can add any settings, so these are arbitrary
        generatedTaskProps.put("foo", "bar");
        Class<? extends Task> taskClass = sourceSink == SourceSink.SINK ? BogusSinkTask.class : BogusSourceTask.class;
        generatedTaskProps.put(TaskConfig.TASK_CLASS_CONFIG, taskClass.getName());
        if (sourceSink == SourceSink.SINK)
            generatedTaskProps.put(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        return generatedTaskProps;
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private abstract class BogusSinkConnector extends SinkConnector {
    }

    private abstract class BogusSinkTask extends SourceTask {
    }

}
