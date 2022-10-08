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
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.RestartPlan;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.WorkerConnector;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@SuppressWarnings("unchecked")
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final String TOPICS_LIST_STR = "topic1,topic2";
    private static final String WORKER_ID = "localhost:8083";
    private static final String KAFKA_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    private enum SourceSink {
        SOURCE, SINK
    }

    private StandaloneHerder herder;

    private Connector connector;
    @Mock
    protected Worker worker;
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

    private MockedStatic<Plugins> pluginsStatic;

    private MockedStatic<WorkerConnector> workerConnectorStatic;

    @Before
    public void setup() {
        worker = mock(Worker.class);
        herder = mock(StandaloneHerder.class, withSettings()
            .useConstructor(worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, new MemoryConfigBackingStore(transformer), noneConnectorClientConfigOverridePolicy)
            .defaultAnswer(CALLS_REAL_METHODS));
        createCallback = new FutureCallback<>();
        plugins = mock(Plugins.class);
        pluginLoader = mock(PluginClassLoader.class);
        delegatingLoader = mock(DelegatingClassLoader.class);
        pluginsStatic = mockStatic(Plugins.class);
        workerConnectorStatic = mockStatic(WorkerConnector.class);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(eq(CONNECTOR_NAME), configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
    }

    @After
    public void tearDown() {
        pluginsStatic.close();
        workerConnectorStatic.close();
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());
    }

    @Test
    public void testCreateConnectorFailedValidation() {
        // Basic validation should be performed and return an error, but should still evaluate the connector's config
        connector = mock(BogusSourceConnector.class);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        config.remove(ConnectorConfig.NAME_CONFIG);

        Connector connectorMock = mock(SourceConnector.class);
        when(worker.configTransformer()).thenReturn(transformer);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.compareAndSwapLoaders(connectorMock)).thenReturn(delegatingLoader);
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);

        when(connectorMock.config()).thenReturn(new ConfigDef());

        ConfigValue validatedValue = new ConfigValue("foo.bar");
        when(connectorMock.validate(config)).thenReturn(new Config(singletonList(validatedValue)));
        when(Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> createCallback.get(1000L, TimeUnit.SECONDS));
        assertEquals(BadRequestException.class, exception.getCause().getClass());
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Throwable {
        connector = mock(BogusSourceConnector.class);
        // First addition should succeed
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config, config);

        when(worker.configTransformer()).thenReturn(transformer);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.compareAndSwapLoaders(connectorMock)).thenReturn(delegatingLoader);
        // No new connector is created
        when(Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Second should fail
        FutureCallback<Herder.Created<ConnectorInfo>> failedCreateCallback = new FutureCallback<>();
        herder.putConnectorConfig(CONNECTOR_NAME, config, false, failedCreateCallback);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> failedCreateCallback.get(1000L, TimeUnit.SECONDS));
        assertEquals(AlreadyExistsException.class, exception.getCause().getClass());
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> config = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        when(statusBackingStore.getAll(CONNECTOR_NAME)).thenReturn(Collections.emptyList());
        statusBackingStore.put(new ConnectorStatus(CONNECTOR_NAME, AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        statusBackingStore.put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));

        expectDestroy();

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
    }

    @Test
    public void testRestartConnectorSameTaskConfigs() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        verify(worker).stopAndAwaitConnector(CONNECTOR_NAME);

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(config), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());

        when(worker.connectorNames()).thenReturn(Collections.singleton(CONNECTOR_NAME));
        when(worker.getPlugins()).thenReturn(plugins);
        // same task configs as earlier, so don't expect a new set of tasks to be brought up
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, config, true)))
            .thenReturn(Collections.singletonList(taskConfig(SourceSink.SOURCE)));

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        restartCallback.get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRestartConnectorNewTaskConfigs() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        worker.stopAndAwaitConnector(CONNECTOR_NAME);
        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(config), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());

        when(worker.connectorNames()).thenReturn(Collections.singleton(CONNECTOR_NAME));
        when(worker.getPlugins()).thenReturn(plugins);
        // changed task configs, expect a new set of tasks to be brought up (and the old ones to be stopped)
        Map<String, String> taskConfigs = taskConfig(SourceSink.SOURCE);
        taskConfigs.put("k", "v");
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, config, true)))
            .thenReturn(Collections.singletonList(taskConfigs));

        when(worker.startSourceTask(eq(new ConnectorTaskId(CONNECTOR_NAME, 0)), any(), eq(connectorConfig(SourceSink.SOURCE)), eq(taskConfigs), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        restartCallback.get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRestartConnectorFailureOnStart() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);

        Exception exception = new ConnectException("Failed to start connector");

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(exception, null);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(config), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        try {
            restartCallback.get(1000L, TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testRestartTask() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        doNothing().when(worker).stopAndAwaitTask(taskId);

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(taskId, taskConfig(SourceSink.SOURCE)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new HashSet<>(),
                new HashSet<>(),
                transformer);
        when(worker.startSourceTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartTaskCallback = new FutureCallback<>();
        herder.restartTask(taskId, restartTaskCallback);
        restartTaskCallback.get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRestartTaskFailureOnStart() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        worker.stopAndAwaitTask(taskId);
        doNothing().when(worker).stopAndAwaitTask(taskId);

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new HashSet<>(),
                new HashSet<>(),
                transformer);
        when(worker.startSourceTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED))
            .thenReturn(false);

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
    }

    @Test
    public void testRestartConnectorAndTasksUnknownConnector() {
        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        RestartRequest restartRequest = new RestartRequest("UnknownConnector", false, true);
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        ExecutionException ee = assertThrows(ExecutionException.class, () -> restartCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(ee.getCause() instanceof NotFoundException);
    }

    @Test
    public void testRestartConnectorAndTasksNoStatus() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        doReturn(Optional.empty()).when(herder).buildRestartPlan(restartRequest);

        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        ExecutionException ee = assertThrows(ExecutionException.class, () -> restartCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(ee.getCause() instanceof NotFoundException);
        assertTrue(ee.getMessage().contains("Status for connector"));
    }

    @Test
    public void testRestartConnectorAndTasksNoRestarts() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(false);
        when(restartPlan.shouldRestartTasks()).thenReturn(false);
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testRestartConnectorAndTasksOnlyConnector() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.shouldRestartTasks()).thenReturn(false);
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.onRestart(CONNECTOR_NAME);
        doNothing().when(herder).onRestart(CONNECTOR_NAME);

        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(connectorConfig), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testRestartConnectorAndTasksOnlyTasks() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(false);
        when(restartPlan.shouldRestartTasks()).thenReturn(true);
        when(restartPlan.restartTaskCount()).thenReturn(1);
        when(restartPlan.totalTaskCount()).thenReturn(1);
        when(restartPlan.taskIdsToRestart()).thenReturn(Collections.singletonList(taskId));
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.onRestart(taskId);
        doNothing().when(herder).onRestart(taskId);

        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(taskId));

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(taskId, taskConfig(SourceSink.SINK)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new HashSet<>(),
                new HashSet<>(),
                transformer);
        when(worker.startSinkTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SINK), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testRestartConnectorAndTasksBoth() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.shouldRestartTasks()).thenReturn(true);
        when(restartPlan.restartTaskCount()).thenReturn(1);
        when(restartPlan.totalTaskCount()).thenReturn(1);
        when(restartPlan.taskIdsToRestart()).thenReturn(Collections.singletonList(taskId));
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        doNothing().when(herder).onRestart(CONNECTOR_NAME);
        doNothing().when(herder).onRestart(taskId);

        connector = mock(BogusSinkConnector.class);
        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);
        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(taskId));

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(connectorConfig), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(taskId, taskConfig(SourceSink.SINK)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new HashSet<>(),
                new HashSet<>(),
                transformer);
        when(worker.startSinkTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SINK), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateAndStop() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        expectStop();

        statusBackingStore.put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));

        statusBackingStore.stop();
        doNothing().when(statusBackingStore).stop();
        doNothing().when(worker).stop();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.stop();
    }

    @Test
    public void testAccessors() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        System.out.println(connConfig);

        Callback<Collection<String>> listConnectorsCb = mock(Callback.class);
        Callback<ConnectorInfo> connectorInfoCb = mock(Callback.class);
        Callback<Map<String, String>> connectorConfigCb = mock(Callback.class);
        Callback<List<TaskInfo>> taskConfigsCb = mock(Callback.class);

        // Check accessors with empty worker
        doNothing().when(listConnectorsCb).onCompletion(null, Collections.EMPTY_SET);
        doNothing().when(connectorInfoCb).onCompletion(any(NotFoundException.class), isNull());
        doNothing().when(taskConfigsCb).onCompletion(any(NotFoundException.class), isNull());

        // Create connector
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);
        expectConfigValidation(connector, true, connConfig);

        // Validate accessors with 1 connector
        doNothing().when(listConnectorsCb).onCompletion(null, singleton(CONNECTOR_NAME));
        ConnectorInfo connInfo = new ConnectorInfo(CONNECTOR_NAME, connConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
        doNothing().when(connectorInfoCb).onCompletion(null, connInfo);

        TaskInfo taskInfo = new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE));
        doNothing().when(taskConfigsCb).onCompletion(null, Arrays.asList(taskInfo));

        // All operations are synchronous for StandaloneHerder, so we don't need to actually wait after making each call
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        Map<String, String> newConnConfig = new HashMap<>(connConfig);
        newConnConfig.put("foo", "bar");

        Callback<Map<String, String>> connectorConfigCb = mock(Callback.class);
        // Callback<Herder.Created<ConnectorInfo>> putConnectorConfigCb = mock(Callback.class);

        // Create
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connConfig);

        // Should get first config
        doNothing().when(connectorConfigCb).onCompletion(null, connConfig);
        // Update config, which requires stopping and restarting
        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);
        final ArgumentCaptor<Map<String, String>> capturedConfig = ArgumentCaptor.forClass(Map.class);
        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), capturedConfig.capture(), any(),
            eq(herder), eq(TargetState.STARTED), onStart.capture());
        // Generate same task config, which should result in no additional action to restart tasks
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, newConnConfig, true)))
            .thenReturn(singletonList(taskConfig(SourceSink.SOURCE)));

        expectConfigValidation(connectorMock, false, newConnConfig);
        doNothing().when(connectorConfigCb).onCompletion(null, newConnConfig);
        when(worker.getPlugins()).thenReturn(plugins);

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
    }

    @Test
    public void testPutTaskConfigs() {
        Callback<Void> cb = mock(Callback.class);

        assertThrows(UnsupportedOperationException.class, () -> herder.putTaskConfigs(CONNECTOR_NAME,
                singletonList(singletonMap("config", "value")), cb, null));
    }

    @Test
    public void testCorruptConfig() throws Throwable {
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, TOPICS_LIST_STR);
        Connector connectorMock = mock(SinkConnector.class);
        String error = "This is an error in your config!";
        List<String> errors = new ArrayList<>(singletonList(error));
        String key = "foo.invalid.key";
        when(connectorMock.validate(config)).thenReturn(
            new Config(
                Arrays.asList(new ConfigValue(key, null, Collections.emptyList(), errors))
            )
        );
        ConfigDef configDef = new ConfigDef();
        configDef.define(key, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "");
        when(worker.configTransformer()).thenReturn(transformer);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.compareAndSwapLoaders(connectorMock)).thenReturn(delegatingLoader);
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        when(connectorMock.config()).thenReturn(configDef);
        when(Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

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
    }

    private void expectAdd(SourceSink sourceSink) {
        Map<String, String> connectorProps = connectorConfig(sourceSink);
        ConnectorConfig connConfig = sourceSink == SourceSink.SOURCE ?
            new SourceConnectorConfig(plugins, connectorProps, true) :
            new SinkConnectorConfig(plugins, connectorProps);

        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(connectorProps), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STARTED), onStart.capture());
        when(worker.isRunning(CONNECTOR_NAME)).thenReturn(true);
        if (sourceSink == SourceSink.SOURCE) {
            when(worker.isTopicCreationEnabled()).thenReturn(true);
        }

        // And we should instantiate the tasks. For a sink task, we should see added properties for the input topic partitions

        Map<String, String> generatedTaskProps = taskConfig(sourceSink);

        when(worker.connectorTaskConfigs(CONNECTOR_NAME, connConfig))
            .thenReturn(singletonList(generatedTaskProps));

        ClusterConfigState configState = new ClusterConfigState(
                -1,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 1),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig(sourceSink)),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
                Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), generatedTaskProps),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new HashSet<>(),
                new HashSet<>(),
                transformer);
        if (sourceSink.equals(SourceSink.SOURCE)) {
            when(worker.startSourceTask(new ConnectorTaskId(CONNECTOR_NAME, 0), configState, connectorConfig(sourceSink), generatedTaskProps, herder, TargetState.STARTED)).thenReturn(true);
        } else {
            when(worker.startSinkTask(new ConnectorTaskId(CONNECTOR_NAME, 0), configState, connectorConfig(sourceSink), generatedTaskProps, herder, TargetState.STARTED)).thenReturn(true);
        }

        ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(herder.connectorType(configCapture.capture())).thenAnswer(invocation -> {
            String connectorClass = configCapture.getValue().get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            if (BogusSourceConnector.class.getName().equals(connectorClass)) {
                return ConnectorType.SOURCE;
            } else if (BogusSinkConnector.class.getName().equals(connectorClass)) {
                return ConnectorType.SINK;
            }
            return ConnectorType.UNKNOWN;
        });

        when(worker.isSinkConnector(CONNECTOR_NAME))
                .thenReturn(sourceSink == SourceSink.SINK);
    }

    private ConnectorInfo createdInfo(SourceSink sourceSink) {
        return new ConnectorInfo(CONNECTOR_NAME, connectorConfig(sourceSink),
            Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            SourceSink.SOURCE == sourceSink ? ConnectorType.SOURCE : ConnectorType.SINK);
    }

    private void expectStop() {
        ConnectorTaskId task = new ConnectorTaskId(CONNECTOR_NAME, 0);
        doNothing().when(worker).stopAndAwaitTasks(singletonList(task));
        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);
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
        when(worker.configTransformer()).thenReturn(transformer);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.compareAndSwapLoaders(connectorMock)).thenReturn(delegatingLoader);
        if (shouldCreateConnector) {
            when(worker.getPlugins()).thenReturn(plugins);
            when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        }
        when(connectorMock.config()).thenReturn(new ConfigDef());

        for (Map<String, String> config : configs)
            when(connectorMock.validate(config)).thenReturn(new Config(Collections.emptyList()));
        when(Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);
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
