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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
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
import org.apache.kafka.connect.runtime.distributed.SampleConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.storage.ClusterConfigState;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    private LoaderSwap loaderSwap;
    protected FutureCallback<Herder.Created<ConnectorInfo>> createCallback;
    @Mock protected StatusBackingStore statusBackingStore;
    private final SampleConnectorClientConfigOverridePolicy
        noneConnectorClientConfigOverridePolicy = new SampleConnectorClientConfigOverridePolicy();

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        worker = mock(Worker.class);
        herder = mock(StandaloneHerder.class, withSettings()
            .useConstructor(worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, new MemoryConfigBackingStore(transformer), noneConnectorClientConfigOverridePolicy, new MockTime())
            .defaultAnswer(CALLS_REAL_METHODS));
        createCallback = new FutureCallback<>();
        plugins = mock(Plugins.class);
        pluginLoader = mock(PluginClassLoader.class);
        loaderSwap = mock(LoaderSwap.class);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(eq(CONNECTOR_NAME), configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(worker, statusBackingStore);
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
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        when(plugins.connectorLoader(anyString())).thenReturn(pluginLoader);
        when(plugins.withClassLoader(pluginLoader)).thenReturn(loaderSwap);

        when(connectorMock.config()).thenReturn(new ConfigDef());

        ConfigValue validatedValue = new ConfigValue("foo.bar");
        when(connectorMock.validate(config)).thenReturn(new Config(singletonList(validatedValue)));

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> createCallback.get(1000L, TimeUnit.SECONDS));
        if (BadRequestException.class != exception.getCause().getClass()) {
            throw new AssertionError(exception.getCause());
        }
        verify(loaderSwap).close();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Throwable {
        connector = mock(BogusSourceConnector.class);
        // First addition should succeed
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Second should fail
        FutureCallback<Herder.Created<ConnectorInfo>> failedCreateCallback = new FutureCallback<>();
        // No new connector is created
        herder.putConnectorConfig(CONNECTOR_NAME, config, false, failedCreateCallback);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> failedCreateCallback.get(1000L, TimeUnit.SECONDS));
        if (AlreadyExistsException.class != exception.getCause().getClass()) {
            throw new AssertionError(exception.getCause());
        }
        verify(loaderSwap, times(2)).close();
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
    public void testCreateConnectorWithStoppedInitialState() throws Exception {
        connector = mock(BogusSinkConnector.class);
        Map<String, String> config = connectorConfig(SourceSink.SINK);
        Connector connectorMock = mock(SinkConnector.class);
        expectConfigValidation(connectorMock, false, config);
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);

        // Only the connector should be created; we expect no tasks to be spawned for a connector created with a paused or stopped initial state
        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STOPPED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(config), any(HerderConnectorContext.class),
            eq(herder), eq(TargetState.STOPPED), onStart.capture());

        when(worker.isRunning(CONNECTOR_NAME)).thenReturn(true);
        when(herder.connectorType(any())).thenReturn(ConnectorType.SINK);

        herder.putConnectorConfig(CONNECTOR_NAME, config, TargetState.STOPPED, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(
            new ConnectorInfo(CONNECTOR_NAME, connectorConfig(SourceSink.SINK), Collections.emptyList(), ConnectorType.SINK),
            connectorInfo.result()
        );
        verify(loaderSwap).close();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        when(statusBackingStore.getAll(CONNECTOR_NAME)).thenReturn(Collections.emptyList());

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Herder.Created<ConnectorInfo>> deleteCallback = new FutureCallback<>();
        expectDestroy();
        herder.deleteConnectorConfig(CONNECTOR_NAME, deleteCallback);
        verify(herder).onDeletion(CONNECTOR_NAME);
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));
        verify(statusBackingStore).put(new ConnectorStatus(CONNECTOR_NAME, ConnectorStatus.State.DESTROYED, WORKER_ID, 0));
        deleteCallback.get(1000L, TimeUnit.MILLISECONDS);

        // Second deletion should fail since the connector is gone
        FutureCallback<Herder.Created<ConnectorInfo>> failedDeleteCallback = new FutureCallback<>();
        herder.deleteConnectorConfig(CONNECTOR_NAME, failedDeleteCallback);
        ExecutionException e = assertThrows(
                "Should have thrown NotFoundException",
                ExecutionException.class,
                () -> failedDeleteCallback.get(1000L, TimeUnit.MILLISECONDS)
        );
        assertTrue(e.getCause() instanceof NotFoundException);
    }

    @Test
    public void testRestartConnectorSameTaskConfigs() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

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
        verify(worker).stopAndAwaitConnector(eq(CONNECTOR_NAME));
    }

    @Test
    public void testRestartConnectorNewTaskConfigs() throws Exception {
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

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

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        expectStop();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        verify(statusBackingStore).put(new TaskStatus(taskId, TaskStatus.State.DESTROYED, WORKER_ID, 0));
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
        verify(worker).stopAndAwaitTask(taskId);
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

        verifyConnectorStatusRestart();
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
        ArgumentCaptor<TaskStatus> taskStatus = ArgumentCaptor.forClass(TaskStatus.class);
        verify(statusBackingStore).put(taskStatus.capture());
        assertEquals(AbstractStatus.State.RESTARTING, taskStatus.getValue().state());
        assertEquals(taskId, taskStatus.getValue().id());
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

        ArgumentCaptor<TaskStatus> taskStatus = ArgumentCaptor.forClass(TaskStatus.class);

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

        verifyConnectorStatusRestart();
        verify(statusBackingStore).put(taskStatus.capture());
        assertEquals(AbstractStatus.State.RESTARTING, taskStatus.getValue().state());
        assertEquals(taskId, taskStatus.getValue().id());
    }

    @Test
    public void testCreateAndStop() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        expectStop();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        herder.stop();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
        verify(worker).stop();
        verify(statusBackingStore).stop();
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));
    }

    @Test
    public void testAccessors() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        System.out.println(connConfig);

        Callback<Collection<String>> listConnectorsCb = mock(Callback.class);
        Callback<ConnectorInfo> connectorInfoCb = mock(Callback.class);
        Callback<Map<String, String>> connectorConfigCb = mock(Callback.class);
        Callback<List<TaskInfo>> taskConfigsCb = mock(Callback.class);
        Callback<Map<ConnectorTaskId, Map<String, String>>> tasksConfigCb = mock(Callback.class);

        // Check accessors with empty worker
        doNothing().when(listConnectorsCb).onCompletion(null, Collections.EMPTY_SET);
        doNothing().when(connectorInfoCb).onCompletion(any(NotFoundException.class), isNull());
        doNothing().when(taskConfigsCb).onCompletion(any(NotFoundException.class), isNull());
        doNothing().when(tasksConfigCb).onCompletion(any(NotFoundException.class), isNull());
        doNothing().when(connectorConfigCb).onCompletion(any(NotFoundException.class), isNull());

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

        Map<ConnectorTaskId, Map<String, String>> tasksConfig = Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0),
            taskConfig(SourceSink.SOURCE));
        doNothing().when(tasksConfigCb).onCompletion(null, tasksConfig);

        // All operations are synchronous for StandaloneHerder, so we don't need to actually wait after making each call
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);
        herder.tasksConfig(CONNECTOR_NAME, tasksConfigCb);

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        reset(transformer);
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);
        herder.tasksConfig(CONNECTOR_NAME, tasksConfigCb);
        // Config transformation should not occur when requesting connector or task info
        verify(transformer, never()).transform(eq(CONNECTOR_NAME), any());
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        Map<String, String> newConnConfig = new HashMap<>(connConfig);
        newConnConfig.put("foo", "bar");

        Callback<Map<String, String>> connectorConfigCb = mock(Callback.class);

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

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);

        FutureCallback<Herder.Created<ConnectorInfo>> reconfigureCallback = new FutureCallback<>();
        doNothing().when(connectorConfigCb).onCompletion(null, newConnConfig);
        herder.putConnectorConfig(CONNECTOR_NAME, newConnConfig, true, reconfigureCallback);
        Herder.Created<ConnectorInfo> newConnectorInfo = reconfigureCallback.get(1000L, TimeUnit.SECONDS);
        ConnectorInfo newConnInfo = new ConnectorInfo(CONNECTOR_NAME, newConnConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
        assertEquals(newConnInfo, newConnectorInfo.result());

        assertEquals("bar", capturedConfig.getValue().get("foo"));
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        verifyNoMoreInteractions(connectorConfigCb);
    }

    @Test
    public void testPutTaskConfigs() {
        Callback<Void> cb = mock(Callback.class);

        assertThrows(UnsupportedOperationException.class, () -> herder.putTaskConfigs(CONNECTOR_NAME,
                singletonList(singletonMap("config", "value")), cb, null));
    }

    @Test
    public void testCorruptConfig() {
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
        when(plugins.connectorLoader(anyString())).thenReturn(pluginLoader);
        when(plugins.withClassLoader(pluginLoader)).thenReturn(loaderSwap);
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        when(connectorMock.config()).thenReturn(configDef);

        herder.putConnectorConfig(CONNECTOR_NAME, config, true, createCallback);
        ExecutionException e = assertThrows(
                "Should have failed to configure connector",
                ExecutionException.class,
                () -> createCallback.get(1000L, TimeUnit.SECONDS)
        );
        assertNotNull(e.getCause());
        Throwable cause = e.getCause();
        assertTrue(cause instanceof BadRequestException);
        assertEquals(
                cause.getMessage(),
                "Connector configuration is invalid and contains the following 1 error(s):\n" +
                        error + "\n" +
                        "You can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"
        );
        verify(loaderSwap).close();
    }

    @Test
    public void testTargetStates() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, connectorConfig);

        // We pause, then stop, the connector
        expectTargetState(CONNECTOR_NAME, TargetState.PAUSED);
        expectTargetState(CONNECTOR_NAME, TargetState.STOPPED);

        expectStop();


        FutureCallback<Void> stopCallback = new FutureCallback<>();
        FutureCallback<List<TaskInfo>> taskConfigsCallback = new FutureCallback<>();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());
        herder.pauseConnector(CONNECTOR_NAME);
        herder.stopConnector(CONNECTOR_NAME, stopCallback);
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        stopCallback.get(10L, TimeUnit.SECONDS);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCallback);
        assertEquals(Collections.emptyList(), taskConfigsCallback.get(1, TimeUnit.SECONDS));

        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        herder.stop();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
        verify(worker).stop();
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        verify(statusBackingStore).stop();
    }

    @Test
    public void testModifyConnectorOffsetsUnknownConnector() {
        FutureCallback<Message> alterOffsetsCallback = new FutureCallback<>();
        herder.alterConnectorOffsets("unknown-connector",
                Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                alterOffsetsCallback);
        ExecutionException e = assertThrows(ExecutionException.class, () -> alterOffsetsCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(e.getCause() instanceof NotFoundException);

        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets("unknown-connector", resetOffsetsCallback);
        e = assertThrows(ExecutionException.class, () -> resetOffsetsCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(e.getCause() instanceof NotFoundException);
    }

    @Test
    public void testModifyConnectorOffsetsConnectorNotInStoppedState() {
        herder.configState = new ClusterConfigState(
                10,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 3),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.PAUSED),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
        );

        FutureCallback<Message> alterOffsetsCallback = new FutureCallback<>();
        herder.alterConnectorOffsets(CONNECTOR_NAME,
                Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                alterOffsetsCallback);
        ExecutionException e = assertThrows(ExecutionException.class, () -> alterOffsetsCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(e.getCause() instanceof BadRequestException);

        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONNECTOR_NAME, resetOffsetsCallback);
        e = assertThrows(ExecutionException.class, () -> resetOffsetsCallback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(e.getCause() instanceof BadRequestException);
    }

    @Test
    public void testAlterConnectorOffsets() throws Exception {
        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been altered successfully");
        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONNECTOR_NAME), eq(connectorConfig(SourceSink.SOURCE)), any(Map.class), workerCallbackCapture.capture());

        herder.configState = new ClusterConfigState(
                10,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 0),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STOPPED),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
        );
        FutureCallback<Message> alterOffsetsCallback = new FutureCallback<>();
        herder.alterConnectorOffsets(CONNECTOR_NAME,
                Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                alterOffsetsCallback);
        assertEquals(msg, alterOffsetsCallback.get(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testResetConnectorOffsets() throws Exception {
        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been reset successfully");

        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONNECTOR_NAME), eq(connectorConfig(SourceSink.SOURCE)), isNull(), workerCallbackCapture.capture());

        herder.configState = new ClusterConfigState(
                10,
                null,
                Collections.singletonMap(CONNECTOR_NAME, 0),
                Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
                Collections.singletonMap(CONNECTOR_NAME, TargetState.STOPPED),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
        );
        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONNECTOR_NAME, resetOffsetsCallback);
        assertEquals(msg, resetOffsetsCallback.get(1000, TimeUnit.MILLISECONDS));
    }

    @Test()
    public void testRequestTaskReconfigurationDoesNotDeadlock() throws Exception {
        connector = mock(BogusSourceConnector.class);
        expectAdd(SourceSink.SOURCE);

        // Start the connector
        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        Connector connectorMock = mock(SourceConnector.class);
        expectConfigValidation(connectorMock, true, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);

        // Wait on connector to start
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(1000L, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Prepare for task config update
        when(worker.connectorNames()).thenReturn(Collections.singleton(CONNECTOR_NAME));
        expectStop();

        // Prepare for connector and task config update
        Map<String, String> newConfig = connectorConfig(SourceSink.SOURCE);
        newConfig.put("dummy-connector-property", "yes");
        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(newConfig), any(HerderConnectorContext.class),
                eq(herder), eq(TargetState.STARTED), onStart.capture());

        // Common invocations
        when(worker.startSourceTask(eq(new ConnectorTaskId(CONNECTOR_NAME, 0)), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);
        Map<String, String> updatedTaskConfig1 = taskConfig(SourceSink.SOURCE);
        updatedTaskConfig1.put("dummy-task-property", "1");
        Map<String, String> updatedTaskConfig2 = taskConfig(SourceSink.SOURCE);
        updatedTaskConfig2.put("dummy-task-property", "2");
        when(worker.connectorTaskConfigs(eq(CONNECTOR_NAME), any()))
                .thenReturn(
                        Collections.singletonList(updatedTaskConfig1),
                        Collections.singletonList(updatedTaskConfig2));

        // Set new config on the connector and tasks
        FutureCallback<Herder.Created<ConnectorInfo>> reconfigureCallback = new FutureCallback<>();
        expectConfigValidation(connectorMock, false, newConfig);
        herder.putConnectorConfig(CONNECTOR_NAME, newConfig, true, reconfigureCallback);

        // Reconfigure the tasks
        herder.requestTaskReconfiguration(CONNECTOR_NAME);

        // Wait on connector update
        Herder.Created<ConnectorInfo> updatedConnectorInfo = reconfigureCallback.get(1000L, TimeUnit.MILLISECONDS);
        ConnectorInfo expectedConnectorInfo = new ConnectorInfo(CONNECTOR_NAME, newConfig, Arrays.asList(new ConnectorTaskId(CONNECTOR_NAME, 0)), ConnectorType.SOURCE);
        assertEquals(expectedConnectorInfo, updatedConnectorInfo.result());

        verify(statusBackingStore, times(2)).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));
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

    private void expectTargetState(String connector, TargetState state) {
        ArgumentCaptor<Callback<TargetState>> stateChangeCallback = ArgumentCaptor.forClass(Callback.class);

        doAnswer(invocation -> {
            stateChangeCallback.getValue().onCompletion(null, state);
            return null;
        }).when(worker).setTargetState(eq(connector), eq(state), stateChangeCallback.capture());
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
        when(plugins.connectorLoader(anyString())).thenReturn(pluginLoader);
        when(plugins.withClassLoader(pluginLoader)).thenReturn(loaderSwap);
        if (shouldCreateConnector) {
            when(worker.getPlugins()).thenReturn(plugins);
            when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        }
        when(connectorMock.config()).thenReturn(new ConfigDef());

        for (Map<String, String> config : configs)
            when(connectorMock.validate(config)).thenReturn(new Config(Collections.emptyList()));
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

    private void verifyConnectorStatusRestart() {
        ArgumentCaptor<ConnectorStatus> connectorStatus = ArgumentCaptor.forClass(ConnectorStatus.class);
        verify(statusBackingStore).put(connectorStatus.capture());
        assertEquals(CONNECTOR_NAME, connectorStatus.getValue().id());
        assertEquals(AbstractStatus.State.RESTARTING, connectorStatus.getValue().state());
    }
}
