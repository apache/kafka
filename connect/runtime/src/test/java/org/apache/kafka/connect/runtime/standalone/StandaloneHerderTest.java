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
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.AppliedConnectorConfig;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.MemoryConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@SuppressWarnings("unchecked")
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final String TOPICS_LIST_STR = "topic1,topic2";
    private static final String WORKER_ID = "localhost:8083";
    private static final String KAFKA_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";
    private static final Long WAIT_TIME_MS = 15000L;

    private enum SourceSink {
        SOURCE, SINK
    }

    private StandaloneHerder herder;

    @Mock
    protected Worker worker;
    @Mock
    protected WorkerConfigTransformer transformer;
    @Mock
    private Plugins plugins;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private LoaderSwap loaderSwap;
    protected FutureCallback<Herder.Created<ConnectorInfo>> createCallback;
    @Mock
    protected StatusBackingStore statusBackingStore;
    private final SampleConnectorClientConfigOverridePolicy
        noneConnectorClientConfigOverridePolicy = new SampleConnectorClientConfigOverridePolicy();

    public void initialize(boolean mockTransform) {
        herder = mock(StandaloneHerder.class, withSettings()
            .useConstructor(worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, new MemoryConfigBackingStore(transformer), noneConnectorClientConfigOverridePolicy, new MockTime())
            .defaultAnswer(CALLS_REAL_METHODS));
        createCallback = new FutureCallback<>();
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        if (mockTransform)
            when(transformer.transform(eq(CONNECTOR_NAME), configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(worker, statusBackingStore);
        herder.stop();
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());
    }

    @Test
    public void testCreateConnectorFailedValidation() {
        initialize(false);
        // Basic validation should be performed and return an error, but should still evaluate the connector's config

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

        ExecutionException exception = assertThrows(ExecutionException.class, () -> createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        if (BadRequestException.class != exception.getCause().getClass()) {
            throw new AssertionError(exception.getCause());
        }
        verify(loaderSwap).close();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Throwable {
        initialize(true);
        // First addition should succeed
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, config, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Second should fail
        FutureCallback<Herder.Created<ConnectorInfo>> failedCreateCallback = new FutureCallback<>();
        // No new connector is created
        herder.putConnectorConfig(CONNECTOR_NAME, config, false, failedCreateCallback);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> failedCreateCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        if (AlreadyExistsException.class != exception.getCause().getClass()) {
            throw new AssertionError(exception.getCause());
        }
        verify(loaderSwap, times(2)).close();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SINK);

        Map<String, String> config = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());
    }

    @Test
    public void testCreateConnectorWithStoppedInitialState() throws Exception {
        initialize(true);
        Map<String, String> config = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, config);

        // Only the connector should be created; we expect no tasks to be spawned for a connector created with a paused or stopped initial state
        mockStartConnector(config, null, TargetState.STOPPED, null);

        when(worker.isRunning(CONNECTOR_NAME)).thenReturn(true);
        when(herder.connectorType(any())).thenReturn(ConnectorType.SINK);

        herder.putConnectorConfig(CONNECTOR_NAME, config, TargetState.STOPPED, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(
            new ConnectorInfo(CONNECTOR_NAME, connectorConfig(SourceSink.SINK), Collections.emptyList(), ConnectorType.SINK),
            connectorInfo.result()
        );
        verify(loaderSwap).close();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, config);

        when(statusBackingStore.getAll(CONNECTOR_NAME)).thenReturn(Collections.emptyList());

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Herder.Created<ConnectorInfo>> deleteCallback = new FutureCallback<>();
        expectDestroy();
        herder.deleteConnectorConfig(CONNECTOR_NAME, deleteCallback);
        verify(herder).onDeletion(CONNECTOR_NAME);
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));
        verify(statusBackingStore).put(new ConnectorStatus(CONNECTOR_NAME, ConnectorStatus.State.DESTROYED, WORKER_ID, 0));
        deleteCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        // Second deletion should fail since the connector is gone
        FutureCallback<Herder.Created<ConnectorInfo>> failedDeleteCallback = new FutureCallback<>();
        herder.deleteConnectorConfig(CONNECTOR_NAME, failedDeleteCallback);
        ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> failedDeleteCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS),
            "Should have thrown NotFoundException"
        );
        assertInstanceOf(NotFoundException.class, e.getCause());
    }

    @Test
    public void testRestartConnectorSameTaskConfigs() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE, false);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, config);

        mockStartConnector(config, TargetState.STARTED, TargetState.STARTED, null);

        when(worker.connectorNames()).thenReturn(Collections.singleton(CONNECTOR_NAME));
        when(worker.getPlugins()).thenReturn(plugins);
        // same task configs as earlier, so don't expect a new set of tasks to be brought up
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, config, true)))
            .thenReturn(Collections.singletonList(taskConfig(SourceSink.SOURCE)));

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartCallback = new FutureCallback<>();
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        verify(worker).stopAndAwaitConnector(eq(CONNECTOR_NAME));
    }

    @Test
    public void testRestartConnectorNewTaskConfigs() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectConfigValidation(SourceSink.SOURCE, config);

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        mockStartConnector(config, TargetState.STARTED, TargetState.STARTED, null);

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
        restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRestartConnectorFailureOnStart() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, config);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);


        Exception exception = new ConnectException("Failed to start connector");

        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());
        FutureCallback<Void> restartCallback = new FutureCallback<>();
        mockStartConnector(config, null, TargetState.STARTED, exception);
        herder.restartConnector(CONNECTOR_NAME, restartCallback);
        ExecutionException e = assertThrows(ExecutionException.class, () -> restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertEquals(exception, e.getCause());
    }

    @Test
    public void testRestartTask() throws Exception {
        initialize(true);
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);

        expectConfigValidation(SourceSink.SOURCE, connectorConfig);

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
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            new HashSet<>(),
            new HashSet<>(),
            transformer);
        when(worker.startSourceTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> restartTaskCallback = new FutureCallback<>();
        herder.restartTask(taskId, restartTaskCallback);
        restartTaskCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRestartTaskFailureOnStart() throws Exception {
        initialize(true);
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, connectorConfig);

        ClusterConfigState configState = new ClusterConfigState(
            -1,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 1),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
            Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            new HashSet<>(),
            new HashSet<>(),
            transformer);
        when(worker.startSourceTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SOURCE), herder, TargetState.STARTED))
            .thenReturn(false);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartTask(taskId, cb);
        verify(worker).stopAndAwaitTask(taskId);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> cb.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, exception.getCause().getClass());
    }

    @Test
    public void testRestartConnectorAndTasksUnknownConnector() {
        initialize(false);
        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        RestartRequest restartRequest = new RestartRequest("UnknownConnector", false, true);
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        ExecutionException ee = assertThrows(ExecutionException.class, () -> restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, ee.getCause());
    }

    @Test
    public void testRestartConnectorAndTasksNoStatus() throws Exception {
        initialize(true);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        doReturn(Optional.empty()).when(herder).buildRestartPlan(restartRequest);

        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, connectorConfig);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        ExecutionException ee = assertThrows(ExecutionException.class, () -> restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, ee.getCause());
        assertTrue(ee.getMessage().contains("Status for connector"));
    }

    @Test
    public void testRestartConnectorAndTasksNoRestarts() throws Exception {
        initialize(true);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(false);
        when(restartPlan.shouldRestartTasks()).thenReturn(false);
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, connectorConfig);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testRestartConnectorAndTasksOnlyConnector() throws Exception {
        initialize(true);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.shouldRestartTasks()).thenReturn(false);
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        expectAdd(SourceSink.SINK, false);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, connectorConfig);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);

        mockStartConnector(connectorConfig, null, TargetState.STARTED, null);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        verifyConnectorStatusRestart();
    }

    @Test
    public void testRestartConnectorAndTasksOnlyTasks() throws Exception {
        initialize(true);
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

        expectAdd(SourceSink.SINK);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, connectorConfig);

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
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            new HashSet<>(),
            new HashSet<>(),
            transformer);
        when(worker.startSinkTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SINK), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        ArgumentCaptor<TaskStatus> taskStatus = ArgumentCaptor.forClass(TaskStatus.class);
        verify(statusBackingStore).put(taskStatus.capture());
        assertEquals(AbstractStatus.State.RESTARTING, taskStatus.getValue().state());
        assertEquals(taskId, taskStatus.getValue().id());
    }

    @Test
    public void testRestartConnectorAndTasksBoth() throws Exception {
        initialize(true);
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

        expectAdd(SourceSink.SINK, false);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SINK);
        expectConfigValidation(SourceSink.SINK, connectorConfig);

        doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);
        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(taskId));

        mockStartConnector(connectorConfig, null, TargetState.STARTED, null);

        ClusterConfigState configState = new ClusterConfigState(
            -1,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 1),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
            Collections.singletonMap(taskId, taskConfig(SourceSink.SINK)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            new HashSet<>(),
            new HashSet<>(),
            transformer);
        when(worker.startSinkTask(taskId, configState, connectorConfig, taskConfig(SourceSink.SINK), herder, TargetState.STARTED))
            .thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SINK), connectorInfo.result());

        FutureCallback<ConnectorStateInfo> restartCallback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, restartCallback);
        assertEquals(connectorStateInfo, restartCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        verifyConnectorStatusRestart();
        verify(statusBackingStore).put(taskStatus.capture());
        assertEquals(AbstractStatus.State.RESTARTING, taskStatus.getValue().state());
        assertEquals(taskId, taskStatus.getValue().id());
    }

    @Test
    public void testCreateAndStop() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, connectorConfig);

        expectStop();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
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
        initialize(true);
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
        doNothing().when(connectorConfigCb).onCompletion(any(NotFoundException.class), isNull());

        expectAdd(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, connConfig);

        // Validate accessors with 1 connector
        doNothing().when(listConnectorsCb).onCompletion(null, singleton(CONNECTOR_NAME));
        ConnectorInfo connInfo = new ConnectorInfo(CONNECTOR_NAME, connConfig, singletonList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
        doNothing().when(connectorInfoCb).onCompletion(null, connInfo);

        TaskInfo taskInfo = new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 0), taskConfig(SourceSink.SOURCE));
        doNothing().when(taskConfigsCb).onCompletion(null, singletonList(taskInfo));

        // All operations are synchronous for StandaloneHerder, so we don't need to actually wait after making each call
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        reset(transformer);
        herder.connectors(listConnectorsCb);
        herder.connectorInfo(CONNECTOR_NAME, connectorInfoCb);
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCb);
        // Config transformation should not occur when requesting connector or task info
        verify(transformer, never()).transform(eq(CONNECTOR_NAME), any());
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        initialize(true);
        Map<String, String> connConfig = connectorConfig(SourceSink.SOURCE);
        Map<String, String> newConnConfig = new HashMap<>(connConfig);
        newConnConfig.put("foo", "bar");

        Callback<Map<String, String>> connectorConfigCb = mock(Callback.class);


        expectAdd(SourceSink.SOURCE, false);
        expectConfigValidation(SourceSink.SOURCE, connConfig, newConnConfig);

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
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        // Generate same task config, but from different connector config, resulting
        // in task restarts
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, new SourceConnectorConfig(plugins, newConnConfig, true)))
            .thenReturn(singletonList(taskConfig(SourceSink.SOURCE)));
        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(taskId));
        doNothing().when(statusBackingStore).put(new TaskStatus(taskId, TaskStatus.State.DESTROYED, WORKER_ID, 0));
        when(worker.startSourceTask(eq(taskId), any(), eq(newConnConfig), eq(taskConfig(SourceSink.SOURCE)), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.putConnectorConfig(CONNECTOR_NAME, connConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);

        FutureCallback<Herder.Created<ConnectorInfo>> reconfigureCallback = new FutureCallback<>();
        doNothing().when(connectorConfigCb).onCompletion(null, newConnConfig);
        herder.putConnectorConfig(CONNECTOR_NAME, newConnConfig, true, reconfigureCallback);
        Herder.Created<ConnectorInfo> newConnectorInfo = reconfigureCallback.get(1000L, TimeUnit.SECONDS);
        ConnectorInfo newConnInfo = new ConnectorInfo(CONNECTOR_NAME, newConnConfig, singletonList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
            ConnectorType.SOURCE);
        assertEquals(newConnInfo, newConnectorInfo.result());

        assertEquals("bar", capturedConfig.getValue().get("foo"));
        herder.connectorConfig(CONNECTOR_NAME, connectorConfigCb);
        verifyNoMoreInteractions(connectorConfigCb);
    }

    @Test
    public void testPatchConnectorConfigNotFound() {
        initialize(false);
        Map<String, String> connConfigPatch = new HashMap<>();
        connConfigPatch.put("foo1", "baz1");

        Callback<Herder.Created<ConnectorInfo>> patchCallback = mock(Callback.class);
        herder.patchConnectorConfig(CONNECTOR_NAME, connConfigPatch, patchCallback);

        ArgumentCaptor<NotFoundException> exceptionCaptor = ArgumentCaptor.forClass(NotFoundException.class);
        verify(patchCallback).onCompletion(exceptionCaptor.capture(), isNull());
        assertEquals(exceptionCaptor.getValue().getMessage(), "Connector " + CONNECTOR_NAME + " not found");
    }

    @Test
    public void testPatchConnectorConfig() throws ExecutionException, InterruptedException, TimeoutException {
        initialize(true);
        // Create the connector.
        Map<String, String> originalConnConfig = connectorConfig(SourceSink.SOURCE);
        originalConnConfig.put("foo0", "unaffected");
        originalConnConfig.put("foo1", "will-be-changed");
        originalConnConfig.put("foo2", "will-be-removed");

        Map<String, String> connConfigPatch = new HashMap<>();
        connConfigPatch.put("foo1", "changed");
        connConfigPatch.put("foo2", null);
        connConfigPatch.put("foo3", "added");

        Map<String, String> patchedConnConfig = new HashMap<>(originalConnConfig);
        patchedConnConfig.put("foo0", "unaffected");
        patchedConnConfig.put("foo1", "changed");
        patchedConnConfig.remove("foo2");
        patchedConnConfig.put("foo3", "added");

        expectAdd(SourceSink.SOURCE, false, false, false);
        expectConfigValidation(SourceSink.SOURCE, originalConnConfig, patchedConnConfig);

        expectConnectorStartingWithoutTasks(originalConnConfig, true);

        herder.putConnectorConfig(CONNECTOR_NAME, originalConnConfig, false, createCallback);
        createCallback.get(1000L, TimeUnit.SECONDS);

        expectConnectorStartingWithoutTasks(patchedConnConfig, false);

        FutureCallback<Herder.Created<ConnectorInfo>> patchCallback = new FutureCallback<>();
        herder.patchConnectorConfig(CONNECTOR_NAME, connConfigPatch, patchCallback);

        Map<String, String> returnedConfig = patchCallback.get(1000L, TimeUnit.SECONDS).result().config();
        assertEquals(patchedConnConfig, returnedConfig);

        // Also check the returned config when requested.
        FutureCallback<Map<String, String>> configCallback = new FutureCallback<>();
        herder.connectorConfig(CONNECTOR_NAME, configCallback);

        Map<String, String> returnedConfig2 = configCallback.get(1000L, TimeUnit.SECONDS);
        assertEquals(patchedConnConfig, returnedConfig2);
    }

    private void expectConnectorStartingWithoutTasks(Map<String, String> config, boolean mockStop) {
        if (mockStop) {
            doNothing().when(worker).stopAndAwaitConnector(CONNECTOR_NAME);
        }
        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), any(Map.class), any(),
            eq(herder), eq(TargetState.STARTED), onStart.capture());
        ConnectorConfig connConfig = new SourceConnectorConfig(plugins, config, true);
        when(worker.connectorTaskConfigs(CONNECTOR_NAME, connConfig))
            .thenReturn(emptyList());
    }

    @Test
    public void testPutTaskConfigs() {
        initialize(false);
        Callback<Void> cb = mock(Callback.class);

        assertThrows(UnsupportedOperationException.class, () -> herder.putTaskConfigs(CONNECTOR_NAME,
            singletonList(singletonMap("config", "value")), cb, null));
    }

    @Test
    public void testCorruptConfig() {
        initialize(false);
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
                singletonList(new ConfigValue(key, null, Collections.emptyList(), errors))
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
            ExecutionException.class,
            () -> createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS),
            "Should have failed to configure connector"
        );
        assertNotNull(e.getCause());
        Throwable cause = e.getCause();
        assertInstanceOf(BadRequestException.class, cause);
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
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);
        expectConfigValidation(SourceSink.SOURCE, connectorConfig);

        // We pause, then stop, the connector
        expectTargetState(CONNECTOR_NAME, TargetState.PAUSED);
        expectTargetState(CONNECTOR_NAME, TargetState.STOPPED);

        expectStop();


        FutureCallback<Void> stopCallback = new FutureCallback<>();
        FutureCallback<List<TaskInfo>> taskConfigsCallback = new FutureCallback<>();

        herder.putConnectorConfig(CONNECTOR_NAME, connectorConfig, false, createCallback);
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());
        herder.pauseConnector(CONNECTOR_NAME);
        herder.stopConnector(CONNECTOR_NAME, stopCallback);
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        stopCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        herder.taskConfigs(CONNECTOR_NAME, taskConfigsCallback);
        assertEquals(Collections.emptyList(), taskConfigsCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        herder.stop();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
        verify(worker).stop();
        verify(statusBackingStore).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), AbstractStatus.State.DESTROYED, WORKER_ID, 0));
        verify(statusBackingStore).stop();
    }

    @Test
    public void testModifyConnectorOffsetsUnknownConnector() {
        initialize(false);
        FutureCallback<Message> alterOffsetsCallback = new FutureCallback<>();
        herder.alterConnectorOffsets("unknown-connector",
            Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
            alterOffsetsCallback);
        ExecutionException e = assertThrows(ExecutionException.class, () -> alterOffsetsCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, e.getCause());

        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets("unknown-connector", resetOffsetsCallback);
        e = assertThrows(ExecutionException.class, () -> resetOffsetsCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, e.getCause());
    }

    @Test
    public void testModifyConnectorOffsetsConnectorNotInStoppedState() {
        initialize(false);
        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);

        herder.configState = new ClusterConfigState(
            10,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 3),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.PAUSED),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            Collections.emptySet(),
            Collections.emptySet()
        );

        FutureCallback<Message> alterOffsetsCallback = new FutureCallback<>();
        herder.alterConnectorOffsets(CONNECTOR_NAME,
            Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
            alterOffsetsCallback);
        ExecutionException e = assertThrows(ExecutionException.class, () -> alterOffsetsCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(BadRequestException.class, e.getCause());

        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONNECTOR_NAME, resetOffsetsCallback);
        e = assertThrows(ExecutionException.class, () -> resetOffsetsCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS));
        assertInstanceOf(BadRequestException.class, e.getCause());
    }

    @Test
    public void testAlterConnectorOffsets() throws Exception {
        initialize(false);
        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been altered successfully");
        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONNECTOR_NAME), eq(connectorConfig(SourceSink.SOURCE)), any(Map.class), workerCallbackCapture.capture());

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);

        herder.configState = new ClusterConfigState(
            10,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 0),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.STOPPED),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
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
        initialize(false);
        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been reset successfully");

        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONNECTOR_NAME), eq(connectorConfig(SourceSink.SOURCE)), isNull(), workerCallbackCapture.capture());

        Map<String, String> connectorConfig = connectorConfig(SourceSink.SOURCE);

        herder.configState = new ClusterConfigState(
            10,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 0),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig(SourceSink.SOURCE)),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.STOPPED),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            Collections.emptySet(),
            Collections.emptySet()
        );
        FutureCallback<Message> resetOffsetsCallback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONNECTOR_NAME, resetOffsetsCallback);
        assertEquals(msg, resetOffsetsCallback.get(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testRequestTaskReconfigurationDoesNotDeadlock() throws Exception {
        initialize(true);
        expectAdd(SourceSink.SOURCE);

        // Start the connector
        Map<String, String> config = connectorConfig(SourceSink.SOURCE);
        // Prepare for connector and task config update
        Map<String, String> newConfig = connectorConfig(SourceSink.SOURCE);
        newConfig.put("dummy-connector-property", "yes");
        expectConfigValidation(SourceSink.SOURCE, config, newConfig);
        mockStartConnector(newConfig, TargetState.STARTED, TargetState.STARTED, null);
        herder.putConnectorConfig(CONNECTOR_NAME, config, false, createCallback);

        // Wait on connector to start
        Herder.Created<ConnectorInfo> connectorInfo = createCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        assertEquals(createdInfo(SourceSink.SOURCE), connectorInfo.result());

        // Prepare for task config update
        when(worker.connectorNames()).thenReturn(Collections.singleton(CONNECTOR_NAME));
        expectStop();


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
        herder.putConnectorConfig(CONNECTOR_NAME, newConfig, true, reconfigureCallback);

        // Reconfigure the tasks
        herder.requestTaskReconfiguration(CONNECTOR_NAME);

        // Wait on connector update
        Herder.Created<ConnectorInfo> updatedConnectorInfo = reconfigureCallback.get(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        ConnectorInfo expectedConnectorInfo = new ConnectorInfo(CONNECTOR_NAME, newConfig, singletonList(new ConnectorTaskId(CONNECTOR_NAME, 0)), ConnectorType.SOURCE);
        assertEquals(expectedConnectorInfo, updatedConnectorInfo.result());

        verify(statusBackingStore, times(2)).put(new TaskStatus(new ConnectorTaskId(CONNECTOR_NAME, 0), TaskStatus.State.DESTROYED, WORKER_ID, 0));
    }

    private void expectAdd(SourceSink sourceSink) {
        expectAdd(sourceSink, true);
    }
    private void expectAdd(SourceSink sourceSink, boolean mockStartConnector) {
        expectAdd(sourceSink, mockStartConnector, true, true);
    }

    private void expectAdd(SourceSink sourceSink,
                           boolean mockStartConnector,
                           boolean mockConnectorTaskConfigs,
                           boolean mockStartSourceTask) {
        Map<String, String> connectorProps = connectorConfig(sourceSink);
        ConnectorConfig connConfig = sourceSink == SourceSink.SOURCE ?
            new SourceConnectorConfig(plugins, connectorProps, true) :
            new SinkConnectorConfig(plugins, connectorProps);

        if (mockStartConnector) {
            mockStartConnector(connectorProps, TargetState.STARTED, TargetState.STARTED, null);
        }

        when(worker.isRunning(CONNECTOR_NAME)).thenReturn(true);
        if (sourceSink == SourceSink.SOURCE) {
            when(worker.isTopicCreationEnabled()).thenReturn(true);
        }

        // And we should instantiate the tasks. For a sink task, we should see added properties for the input topic partitions

        Map<String, String> connectorConfig = connectorConfig(sourceSink);
        Map<String, String> generatedTaskProps = taskConfig(sourceSink);

        if (mockConnectorTaskConfigs) {
            when(worker.connectorTaskConfigs(CONNECTOR_NAME, connConfig)).thenReturn(singletonList(generatedTaskProps));
        }

        ClusterConfigState configState = new ClusterConfigState(
            -1,
            null,
            Collections.singletonMap(CONNECTOR_NAME, 1),
            Collections.singletonMap(CONNECTOR_NAME, connectorConfig),
            Collections.singletonMap(CONNECTOR_NAME, TargetState.STARTED),
            Collections.singletonMap(new ConnectorTaskId(CONNECTOR_NAME, 0), generatedTaskProps),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONNECTOR_NAME, new AppliedConnectorConfig(connectorConfig)),
            new HashSet<>(),
            new HashSet<>(),
            transformer);

        if (sourceSink.equals(SourceSink.SOURCE) && mockStartSourceTask) {
            when(worker.startSourceTask(new ConnectorTaskId(CONNECTOR_NAME, 0), configState, connectorConfig(sourceSink), generatedTaskProps, herder, TargetState.STARTED)).thenReturn(true);
        }

        if (sourceSink.equals(SourceSink.SINK)) {
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
            singletonList(new ConnectorTaskId(CONNECTOR_NAME, 0)),
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
        SourceSink sourceSink,
        Map<String, String>... configs
    ) {
        // config validation
        Connector connectorMock = sourceSink == SourceSink.SOURCE ? mock(SourceConnector.class) : mock(SinkConnector.class);
        when(worker.configTransformer()).thenReturn(transformer);
        final ArgumentCaptor<Map<String, String>> configCapture = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(configCapture.capture())).thenAnswer(invocation -> configCapture.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.connectorLoader(anyString())).thenReturn(pluginLoader);
        when(plugins.withClassLoader(pluginLoader)).thenReturn(loaderSwap);

        // Assume the connector should always be created
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.newConnector(anyString())).thenReturn(connectorMock);
        when(connectorMock.config()).thenReturn(new ConfigDef());

        // Set up validation for each config
        for (Map<String, String> config : configs) {
            when(connectorMock.validate(config)).thenReturn(new Config(Collections.emptyList()));
        }
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract static class BogusSourceConnector extends SourceConnector {
    }

    private abstract static class BogusSourceTask extends SourceTask {
    }

    private abstract static class BogusSinkConnector extends SinkConnector {
    }

    private abstract static class BogusSinkTask extends SourceTask {
    }

    private void verifyConnectorStatusRestart() {
        ArgumentCaptor<ConnectorStatus> connectorStatus = ArgumentCaptor.forClass(ConnectorStatus.class);
        verify(statusBackingStore).put(connectorStatus.capture());
        assertEquals(CONNECTOR_NAME, connectorStatus.getValue().id());
        assertEquals(AbstractStatus.State.RESTARTING, connectorStatus.getValue().state());
    }

    private void mockStartConnector(Map<String, String> config, TargetState result, TargetState targetState, Exception exception) {
        final ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(exception, result);
            return true;
        }).when(worker).startConnector(eq(CONNECTOR_NAME), eq(config),
                any(HerderConnectorContext.class),
                eq(herder), eq(targetState), onStart.capture());
    }
}
