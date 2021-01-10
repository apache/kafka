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
package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.CloseableConnectorContext;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.WorkerConnector;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
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
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@SuppressWarnings("unchecked")
@PrepareForTest({StandaloneHerder.class, Plugins.class, WorkerConnector.class})
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final List<String> TOPICS_LIST = Arrays.asList("topic1", "topic2");
    private static final String TOPICS_LIST_STR = "topic1,topic2";
    private static final int DEFAULT_MAX_TASKS = 1;
    private static final String WORKER_ID = "localhost:8083";
    private static final String KAFKA_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    private enum SourceSink {
        SOURCE, SINK
    }

    private StandaloneHerder herder;

    private Connector connector;
    @Mock protected Worker worker;
    @Mock protected WorkerConfigTransformer transformer;
    @Mock private Plugins plugins;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private DelegatingClassLoader delegatingLoader;
    protected FutureCallback<Herder.Created<ConnectorInfo>> createCallback;
    @Mock protected StatusBackingStore statusBackingStore;

    private final ConnectorClientConfigOverridePolicy
        noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();


    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        herder = PowerMock.createPartialMock(StandaloneHerder.class, new String[]{"connectorTypeForClass"/*, "validateConnectorConfig"*/},
            worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, new MemoryConfigBackingStore(transformer), noneConnectorClientConfigOverridePolicy);
        createCallback = new FutureCallback<>();
        plugins = PowerMock.createMock(Plugins.class);
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        delegatingLoader = PowerMock.createMock(DelegatingClassLoader.class);
        PowerMock.mockStatic(Plugins.class);
        PowerMock.mockStatic(WorkerConnector.class);
        Capture<Map<String, String>> configCapture = Capture.newInstance();
        EasyMock.expect(transformer.transform(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(configCapture))).andAnswer(configCapture::getValue).anyTimes();
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorFailedValidation() throws Throwable {
        // Basic validation should be performed and return an error, but should still evaluate the connector's config
        connector = PowerMock.createMock(BogusSourceConnector.class);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        config.remove(ConnectorConfig.NAME_CONFIG);

        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);

        EasyMock.expect(connectorMock.config()).andStubReturn(new ConfigDef());

        ConfigValue validatedValue = new ConfigValue("foo.bar");
        EasyMock.expect(connectorMock.validate(config)).andReturn(new Config(singletonList(validatedValue)));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> createCallback.get(1000L, TimeUnit.SECONDS));
        assertEquals(BadRequestException.class, exception.getCause().getClass());
        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Throwable {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        // First addition should succeed
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config, config);

        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(2);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        // No new connector is created
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Second should fail
        FutureCallback<Herder.Created<ConnectorInfo>> failedCreateCallback = new FutureCallback<>();
        herder.putConnectorConfig(CONNECTOR_NAME, config, false, failedCreateCallback);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> failedCreateCallback.get(1000L, TimeUnit.SECONDS));
        assertEquals(AlreadyExistsException.class, exception.getCause().getClass());
        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = PowerMock.createMock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> config = connectorConfig(SourceSink.SINK);
        Connector connectorMock = PowerMock.createMock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, config);
        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        EasyMock.expect(statusBackingStore.getAll(CONNECTOR_NAME)).andReturn(Collections.<TaskStatus>emptyList());
        statusBackingStore.put(new ConnectorStatus(CONNECTOR_NAME, AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        statusBackingStore.put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));

        expectDestroy();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Herder.Created<ConnectorInfo>> deleteCallback = new FutureCallback<>();
        herder.deleteConnectorConfig(CONNECTOR_NAME, deleteCallback);
        deleteCallback.get(1000L, TimeUnit.MILLISECONDS);

        // Second deletion should fail since the connector is gone
        FutureCallback<Herder.Created<ConnectorInfo>> failedDeleteCallback = new FutureCallback<>();
        herder.deleteConnectorConfig(CONNECTOR_NAME, failedDeleteCallback);
        try {
            failedDeleteCallback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Should have thrown NotFoundException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnector() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall();

        Capture<Callback<TargetState>> onStart = EasyMock.newCapture();
        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(config), EasyMock.anyObject(HerderConnectorContext.class),
            EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), EasyMock.capture(onStart));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Boolean>() {
            @Override
            public Boolean answer() throws Throwable {
                onStart.getValue().onCompletion(null, TargetState.STARTED);
                return true;
            }
        });

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        restartCallback.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorFailureOnStart() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall();

        Capture<Callback<TargetState>> onStart = EasyMock.newCapture();
        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(config), EasyMock.anyObject(HerderConnectorContext.class),
            EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), EasyMock.capture(onStart));
        Exception exception = new ConnectException("Failed to start connector");
        EasyMock.expectLastCall().andAnswer(new IAnswer<Boolean>() {
            @Override
            public Boolean answer() throws Throwable {
                onStart.getValue().onCompletion(exception, null);
                return true;
            }
        });

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        try {
            restartCallback.get(1000L, TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTask() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        worker.stopAndAwaitTask(taskId);
        EasyMock.expectLastCall();

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(taskId, taskConfig(SourceSink.SOURCE)),
                new HashSet<>(),
                transformer);
        worker.startTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartTaskCallback = new FutureCallback<>();
        herder.restartTask(taskId, restartTaskCallback);
        restartTaskCallback.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskFailureOnStart() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        worker.stopAndAwaitTask(taskId);
        EasyMock.expectLastCall();

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE)),
                new HashSet<>(),
                transformer);
        worker.startTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(false);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

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

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        expectStop();

        statusBackingStore.put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));

        statusBackingStore.stop();
        EasyMock.expectLastCall();
        worker.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testAccessors() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        System.out.println(connConfig);

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
        expectConfigValidation(connector, true, connConfig);

        // Validate accessors with 1 connector
        listConnectorsCb.onCompletion(null, singleton(CONNECTOR_NAME));
        EasyMock.expectLastCall();
        ConnectorInfo connInfo = new ConnectorInfo(CONNECTOR_NAME, connConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
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
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        EasyMock.reset(transformer);
        EasyMock.expect(transformer.transform(EasyMock.eq(CONNECTOR_NAME), EasyMock.anyObject()))
            .andThrow(new AssertionError("Config transformation should not occur when requesting connector or task info"))
            .anyTimes();
        EasyMock.replay(transformer);

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
        // Callback<Herder.Created<ConnectorInfo>> putConnectorConfigCb = PowerMock.createMock(Callback.class);

        // Create
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connConfig);

        // Should get first config
        connectorConfigCb.onCompletion(null, connConfig);
        EasyMock.expectLastCall();
        // Update config, which requires stopping and restarting
        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall();
        Capture<Map<String, String>> capturedConfig = EasyMock.newCapture();
        Capture<Callback<TargetState>> onStart = EasyMock.newCapture();
        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(capturedConfig), EasyMock.<CloseableConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), EasyMock.capture(onStart));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Boolean>() {
            @Override
            public Boolean answer() throws Throwable {
                onStart.getValue().onCompletion(null, TargetState.STARTED);
                return true;
            }
        });
        EasyMock.expect(worker.isRunning(CONNECTOR_NAME)).andReturn(true);
        EasyMock.expect(worker.isTopicCreationEnabled()).andReturn(true);
        // Generate same task config, which should result in no additional action to restart tasks
        EasyMock.expect(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, newConnConfig, true)))
                .andReturn(singletonList(taskConfig(SourceSink.SOURCE)));
        worker.isSinkConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall().andReturn(false);

        expectConfigValidation(connectorMock, false, newConnConfig);
        connectorConfigCb.onCompletion(null, newConnConfig);
        EasyMock.expectLastCall();
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).anyTimes();

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);

        FutureCallback<Herder.Created<ConnectorInfo>> reconfigureCallback = new FutureCallback<>();
        herder.putConnectorConfig(CONNECTOR_NAME, newConnConfig, true, reconfigureCallback);
        Herder.Created<ConnectorInfo> newConnectorInfo = reconfigureCallback.get(1000L, TimeUnit.SECONDS);
        ConnectorInfo newConnInfo = new ConnectorInfo(CONNECTOR_NAME, newConnConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
        assertEquals(newConnInfo, newConnectorInfo.result());

        assertEquals("bar", capturedConfig.getValue().get("foo"));
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigs() {
        Callback<Void> cb = PowerMock.createMock(Callback.class);

        PowerMock.replayAll();

        assertThrows(UnsupportedOperationException.class, () -> herder.putTaskConfigs(CONNECTOR_NAME,
                singletonList(singletonMap("config", "value")), cb, null));
        PowerMock.verifyAll();
    }

    @Test
    public void testCorruptConfig() throws Throwable {
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, TOPICS_LIST_STR);
        Connector connectorMock = PowerMock.createMock(SinkConnector.class);
        String error = "This is an error in your config!";
        List<String> errors = new ArrayList<>(singletonList(error));
        String key = "foo.invalid.key";
        EasyMock.expect(connectorMock.validate(config)).andReturn(
            new Config(
                Arrays.asList(new ConfigValue(key, null, Collections.emptyList(), errors))
            )
        );
        ConfigDef configDef = new ConfigDef();
        configDef.define(key, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "");
        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);
        EasyMock.expect(connectorMock.config()).andStubReturn(configDef);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        PowerMock.replayAll();

        herder.putConnectorConfig(CONNECTOR_NAME, config, true, createCallback);
        try {
            createCallback.get(1000L, TimeUnit.SECONDS);
            fail("Should have failed to configure connector");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            Throwable cause = e.getCause();
            assertTrue(cause instanceof BadRequestException);
            assertEquals(
                cause.getMessage(),
                "Connector configuration is invalid and contains the following 1 error(s):\n" +
                    error + "\n" +
                    "You can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"
            );
        }

        PowerMock.verifyAll();
    }

    private void expectAdd(SourceSink sourceSink) {
        Map<String, String> connectorProps = connectorConfig(sourceSink);
        ConnectorConfig connConfig = sourceSink == SourceSink.SOURCE ?
            new SourceConnectorConfig(plugins, connectorProps, true) :
            new SinkConnectorConfig(plugins, connectorProps);

        Capture<Callback<TargetState>> onStart = EasyMock.newCapture();
        worker.startConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(connectorProps), EasyMock.anyObject(HerderConnectorContext.class),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), EasyMock.capture(onStart));
        // EasyMock.expectLastCall().andReturn(true);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Boolean>() {
            @Override
            public Boolean answer() throws Throwable {
                onStart.getValue().onCompletion(null, TargetState.STARTED);
                return true;
            }
        });
        EasyMock.expect(worker.isRunning(CONNECTOR_NAME)).andReturn(true);
        if (sourceSink == SourceSink.SOURCE) {
            EasyMock.expect(worker.isTopicCreationEnabled()).andReturn(true);
        }

        // And we should instantiate the tasks. For a sink task, we should see added properties for the input topic partitions

        Map<String, String> generatedTaskProps = taskConfig(sourceSink);

        EasyMock.expect(worker.connectorTaskConfigs(CONNECTOR_NAME, connConfig))
            .andReturn(singletonList(generatedTaskProps));

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig(sourceSink)),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), generatedTaskProps),
                new HashSet<>(),
                transformer);
        worker.startTask(new ConnectorTaskId(CONNECTOR_NAME, 0), configState, connectorConfig(sourceSink), generatedTaskProps, herder, TargetState.STARTED);
        EasyMock.expectLastCall().andReturn(true);

        EasyMock.expect(herder.connectorTypeForClass(BogusSourceConnector.class.getName()))
            .andReturn(ConnectorType.SOURCE).anyTimes();
        EasyMock.expect(herder.connectorTypeForClass(BogusSinkConnector.class.getName()))
            .andReturn(ConnectorType.SINK).anyTimes();
        worker.isSinkConnector(CONNECTOR_NAME);
        PowerMock.expectLastCall().andReturn(sourceSink == SourceSink.SINK);
    }

    private ConnectorInfo createdInfo(SourceSink sourceSink) {
        return new ConnectorInfo(CONNECTOR_NAME, connectorConfig(sourceSink),
            Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            SourceSink.SOURCE == sourceSink ? ConnectorType.SOURCE : ConnectorType.SINK);
    }

    private void expectStop() {
        ConnectorTaskId task = new ConnectorTaskId(CONNECTOR_NAME, 0);
        worker.stopAndAwaitTasks(singletonList(task));
        EasyMock.expectLastCall();
        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall();
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
        if (sourceSink == SourceSink.SINK) {
            props.put(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        } else {
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        }
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

    private void expectConfigValidation(
            Connector connectorMock,
            boolean shouldCreateConnector,
            Map<String, String>... configs
    ) {
        // config validation
        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        if (shouldCreateConnector) {
            EasyMock.expect(worker.getPlugins()).andReturn(plugins);
            EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);
        }
        EasyMock.expect(connectorMock.config()).andStubReturn(new ConfigDef());

        for (Map<String, String> config : configs)
            EasyMock.expect(connectorMock.validate(config)).andReturn(new Config(Collections.<ConfigValue>emptyList()));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);
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
