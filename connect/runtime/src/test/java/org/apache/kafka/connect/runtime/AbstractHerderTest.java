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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.provider.DirectoryConfigProvider;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.distributed.SampleConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.AbstractHerder.keysWithVariableValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class AbstractHerderTest {

    private static final String CONN1 = "sourceA";
    private static final ConnectorTaskId TASK0 = new ConnectorTaskId(CONN1, 0);
    private static final ConnectorTaskId TASK1 = new ConnectorTaskId(CONN1, 1);
    private static final ConnectorTaskId TASK2 = new ConnectorTaskId(CONN1, 2);
    private static final Integer MAX_TASKS = 3;
    private static final Map<String, String> CONN1_CONFIG = new HashMap<>();
    private static final String TEST_KEY = "testKey";
    private static final String TEST_KEY2 = "testKey2";
    private static final String TEST_KEY3 = "testKey3";
    private static final String TEST_VAL = "testVal";
    private static final String TEST_VAL2 = "testVal2";
    private static final String TEST_REF = "${file:/tmp/somefile.txt:somevar}";
    private static final String TEST_REF2 = "${file:/tmp/somefile2.txt:somevar2}";
    private static final String TEST_REF3 = "${file:/tmp/somefile3.txt:somevar3}";
    static {
        CONN1_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN1);
        CONN1_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN1_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        CONN1_CONFIG.put(TEST_KEY, TEST_REF);
        CONN1_CONFIG.put(TEST_KEY2, TEST_REF2);
        CONN1_CONFIG.put(TEST_KEY3, TEST_REF3);
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, SampleSourceConnector.SampleSourceTask.class.getName());
        TASK_CONFIG.put(TEST_KEY, TEST_REF);
    }
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
    }
    private static final HashMap<ConnectorTaskId, Map<String, String>> TASK_CONFIGS_MAP = new HashMap<>();
    static {
        TASK_CONFIGS_MAP.put(TASK0, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK1, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK2, TASK_CONFIG);
    }
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_NO_TASKS = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.STARTED),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Collections.emptySet());

    private final String workerId = "workerId";
    private final String kafkaClusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private final int generation = 5;
    private final String connectorName = "connector";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();

    @Mock private Worker worker;
    @Mock private WorkerConfigTransformer transformer;
    @Mock private ConfigBackingStore configStore;
    @Mock private StatusBackingStore statusStore;
    @Mock private ClassLoader classLoader;
    @Mock private LoaderSwap loaderSwap;
    @Mock private Plugins plugins;

    @Test
    public void testConnectors() {
        AbstractHerder herder = testHerder();

        when(configStore.snapshot()).thenReturn(SNAPSHOT);
        assertEquals(Collections.singleton(CONN1), new HashSet<>(herder.connectors()));
    }

    @Test
    public void testConnectorClientConfigOverridePolicyClose() {
        SampleConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new SampleConnectorClientConfigOverridePolicy();

        AbstractHerder herder = testHerder(noneConnectorClientConfigOverridePolicy);

        herder.stopServices();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
    }

    @Test
    public void testConnectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connectorName, 0);

        AbstractHerder herder = testHerder();

        when(plugins.newConnector(anyString())).thenReturn(new SampleSourceConnector());
        when(herder.plugins()).thenReturn(plugins);

        when(herder.rawConfig(connectorName)).thenReturn(Collections.singletonMap(
                ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName()
        ));

        when(statusStore.get(connectorName))
                .thenReturn(new ConnectorStatus(connectorName, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connectorName))
                .thenReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        ConnectorStateInfo state = herder.connectorStatus(connectorName);

        assertEquals(connectorName, state.name());
        assertEquals(ConnectorType.SOURCE, state.type());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());
    }

    @Test
    public void testConnectorStatusMissingPlugin() {
        ConnectorTaskId taskId = new ConnectorTaskId(connectorName, 0);

        AbstractHerder herder = testHerder();

        when(plugins.newConnector(anyString())).thenThrow(new ConnectException("Unable to find class"));
        when(herder.plugins()).thenReturn(plugins);

        when(herder.rawConfig(connectorName))
                .thenReturn(Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "missing"));

        when(statusStore.get(connectorName))
                .thenReturn(new ConnectorStatus(connectorName, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connectorName))
                .thenReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        ConnectorStateInfo state = herder.connectorStatus(connectorName);

        assertEquals(connectorName, state.name());
        assertEquals(ConnectorType.UNKNOWN, state.type());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());
    }

    @Test
    public void testConnectorInfo() {
        AbstractHerder herder = testHerder();

        when(plugins.newConnector(anyString())).thenReturn(new SampleSourceConnector());
        when(herder.plugins()).thenReturn(plugins);

        when(configStore.snapshot()).thenReturn(SNAPSHOT);

        ConnectorInfo info = herder.connectorInfo(CONN1);

        assertEquals(CONN1, info.name());
        assertEquals(CONN1_CONFIG, info.config());
        assertEquals(Arrays.asList(TASK0, TASK1, TASK2), info.tasks());
        assertEquals(ConnectorType.SOURCE, info.type());
    }

    @Test
    public void testPauseConnector() {
        AbstractHerder herder = testHerder();

        when(configStore.contains(CONN1)).thenReturn(true);

        herder.pauseConnector(CONN1);

        verify(configStore).putTargetState(CONN1, TargetState.PAUSED);
    }

    @Test
    public void testResumeConnector() {
        AbstractHerder herder = testHerder();

        when(configStore.contains(CONN1)).thenReturn(true);

        herder.resumeConnector(CONN1);

        verify(configStore).putTargetState(CONN1, TargetState.STARTED);
    }

    @Test
    public void testConnectorInfoMissingPlugin() {
        AbstractHerder herder = testHerder();

        when(plugins.newConnector(anyString())).thenThrow(new ConnectException("No class found"));
        when(herder.plugins()).thenReturn(plugins);

        when(configStore.snapshot()).thenReturn(SNAPSHOT);

        ConnectorInfo info = herder.connectorInfo(CONN1);

        assertEquals(CONN1, info.name());
        assertEquals(CONN1_CONFIG, info.config());
        assertEquals(Arrays.asList(TASK0, TASK1, TASK2), info.tasks());
        assertEquals(ConnectorType.UNKNOWN, info.type());
    }

    @Test
    public void testTaskStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connectorName, 0);
        String workerId = "workerId";

        AbstractHerder herder = testHerder();

        final ArgumentCaptor<TaskStatus> taskStatusArgumentCaptor = ArgumentCaptor.forClass(TaskStatus.class);
        doNothing().when(statusStore).putSafe(taskStatusArgumentCaptor.capture());

        when(statusStore.get(taskId)).thenAnswer(invocation -> taskStatusArgumentCaptor.getValue());

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());
    }

    @Test
    public void testBuildRestartPlanForUnknownConnector() {
        String connectorName = "UnknownConnector";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);
        AbstractHerder herder = testHerder();

        when(statusStore.get(connectorName)).thenReturn(null);

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertFalse(mayBeRestartPlan.isPresent());
    }

    @Test
    public void testConfigValidationNullConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put("name", "somename");
        config.put("required", "value");
        config.put("testKey", null);

        final ConfigInfos configInfos = herder.validateConnectorConfig(config, s -> null, false);

        assertEquals(1, configInfos.errorCount());
        assertErrorForKey(configInfos, "testKey");
        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationMultipleNullConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put("name", "somename");
        config.put("required", "value");
        config.put("testKey", null);
        config.put("secondTestKey", null);

        final ConfigInfos configInfos = herder.validateConnectorConfig(config, s -> null, false);

        assertEquals(2, configInfos.errorCount());
        assertErrorForKey(configInfos, "testKey");
        assertErrorForKey(configInfos, "secondTestKey");
        verifyValidationIsolation();
    }

    @Test
    public void testBuildRestartPlanForConnectorAndTasks() {
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connectorName, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connectorName, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = testHerder();

        when(herder.rawConfig(connectorName)).thenReturn(null);

        when(statusStore.get(connectorName))
                .thenReturn(new ConnectorStatus(connectorName, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connectorName)).thenReturn(taskStatuses);

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertTrue(restartPlan.shouldRestartConnector());
        assertTrue(restartPlan.shouldRestartTasks());
        assertEquals(2, restartPlan.taskIdsToRestart().size());
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId1));
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId2));
    }

    @Test
    public void testBuildRestartPlanForNoRestart() {
        RestartRequest restartRequest = new RestartRequest(connectorName, true, false);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connectorName, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connectorName, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = testHerder();

        when(herder.rawConfig(connectorName)).thenReturn(null);

        when(statusStore.get(connectorName))
                .thenReturn(new ConnectorStatus(connectorName, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connectorName)).thenReturn(taskStatuses);

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertFalse(restartPlan.shouldRestartConnector());
        assertFalse(restartPlan.shouldRestartTasks());
        assertTrue(restartPlan.taskIdsToRestart().isEmpty());
    }

    @Test
    public void testConfigValidationEmptyConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy, 0);

        assertThrows(BadRequestException.class, () -> herder.validateConnectorConfig(Collections.emptyMap(), s -> null, false));
        verify(transformer).transform(Collections.emptyMap());
    }

    @Test
    public void testConfigValidationMissingName() {
        final Class<? extends Connector> connectorClass = SampleSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        ConfigInfos result = herder.validateConnectorConfig(config, s -> null, false);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        assertEquals(Arrays.asList(ConnectorConfig.COMMON_GROUP, ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP, ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP, SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_GROUP,
                SourceConnectorConfig.OFFSETS_TOPIC_GROUP), result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        // Base connector config has 15 fields, connector's configs add 7
        assertEquals(22, infos.size());
        // Missing name should generate an error
        assertEquals(ConnectorConfig.NAME_CONFIG,
                infos.get(ConnectorConfig.NAME_CONFIG).configValue().name());
        assertEquals(1, infos.get(ConnectorConfig.NAME_CONFIG).configValue().errors().size());
        // "required" config from connector should generate an error
        assertEquals("required", infos.get("required").configValue().name());
        assertEquals(1, infos.get("required").configValue().errors().size());

        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationInvalidTopics() {
        final Class<? extends Connector> connectorClass = SampleSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1,topic2");
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");

        ConfigInfos validation = herder.validateConnectorConfig(config, s -> null, false);

        ConfigInfo topicsListInfo = findInfo(validation, SinkConnectorConfig.TOPICS_CONFIG);
        assertNotNull(topicsListInfo);
        assertEquals(1, topicsListInfo.configValue().errors().size());

        ConfigInfo topicsRegexInfo = findInfo(validation, SinkConnectorConfig.TOPICS_REGEX_CONFIG);
        assertNotNull(topicsRegexInfo);
        assertEquals(1, topicsRegexInfo.configValue().errors().size());

        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationTopicsWithDlq() {
        final Class<? extends Connector> connectorClass = SampleSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        ConfigInfos validation = herder.validateConnectorConfig(config, s -> null, false);

        ConfigInfo topicsListInfo = findInfo(validation, SinkConnectorConfig.TOPICS_CONFIG);
        assertNotNull(topicsListInfo);
        assertEquals(1, topicsListInfo.configValue().errors().size());

        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationTopicsRegexWithDlq() {
        final Class<? extends Connector> connectorClass = SampleSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        ConfigInfos validation = herder.validateConnectorConfig(config, s -> null, false);

        ConfigInfo topicsRegexInfo = findInfo(validation, SinkConnectorConfig.TOPICS_REGEX_CONFIG);
        assertNotNull(topicsRegexInfo);
        assertEquals(1, topicsRegexInfo.configValue().errors().size());

        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationTransformsExtendResults() {
        final Class<? extends Connector> connectorClass = SampleSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        when(plugins.transformations()).thenReturn(Collections.singleton(transformationPluginDesc()));

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA,xformB");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config, s -> null, false);
        assertEquals(herder.connectorType(config), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_GROUP,
                SourceConnectorConfig.OFFSETS_TOPIC_GROUP,
                "Transforms: xformA",
                "Transforms: xformB"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(27, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type",
                infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig",
                infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformB.type", infos.get("transforms.xformB.type").configValue().name());
        assertFalse(infos.get("transforms.xformB.type").configValue().errors().isEmpty());

        verify(plugins, times(2)).transformations();
        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationPredicatesExtendResults() {
        final Class<? extends Connector> connectorClass = SampleSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        when(plugins.transformations()).thenReturn(Collections.singleton(transformationPluginDesc()));
        when(plugins.predicates()).thenReturn(Collections.singleton(predicatePluginDesc()));

        // Define 2 predicates. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.predicate", "predX");
        config.put(ConnectorConfig.PREDICATES_CONFIG, "predX,predY");
        config.put(ConnectorConfig.PREDICATES_CONFIG + ".predX.type", SamplePredicate.class.getName());
        config.put("required", "value"); // connector required config

        ConfigInfos result = herder.validateConnectorConfig(config, s -> null, false);
        assertEquals(ConnectorType.SOURCE, herder.connectorType(config));

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_GROUP,
                SourceConnectorConfig.OFFSETS_TOPIC_GROUP,
                "Transforms: xformA",
                "Predicates: predX",
                "Predicates: predY"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(29, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type", infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig", infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformA.predicate", infos.get("transforms.xformA.predicate").configValue().name());
        assertTrue(infos.get("transforms.xformA.predicate").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.negate", infos.get("transforms.xformA.negate").configValue().name());
        assertTrue(infos.get("transforms.xformA.negate").configValue().errors().isEmpty());
        assertEquals("predicates.predX.type", infos.get("predicates.predX.type").configValue().name());
        assertEquals("predicates.predX.predconfig", infos.get("predicates.predX.predconfig").configValue().name());
        assertEquals("predicates.predY.type", infos.get("predicates.predY.type").configValue().name());
        assertFalse(infos.get("predicates.predY.type").configValue().errors().isEmpty());

        verify(plugins).transformations();
        verify(plugins, times(2)).predicates();
        verifyValidationIsolation();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Predicate<?>> predicatePluginDesc() {
        return new PluginDesc(SamplePredicate.class, "1.0", PluginType.PREDICATE, classLoader);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Transformation<?>> transformationPluginDesc() {
        return new PluginDesc(SampleTransformation.class, "1.0", PluginType.TRANSFORMATION, classLoader);
    }

    @Test
    public void testConfigValidationPrincipalOnlyOverride() {
        final Class<? extends Connector> connectorClass = SampleSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, new PrincipalConnectorClientConfigOverridePolicy());

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        String ackConfigKey = producerOverrideKey(ProducerConfig.ACKS_CONFIG);
        String saslConfigKey = producerOverrideKey(SaslConfigs.SASL_JAAS_CONFIG);
        config.put(ackConfigKey, "none");
        config.put(saslConfigKey, "jaas_config");

        ConfigInfos result = herder.validateConnectorConfig(config, s -> null, false);
        assertEquals(herder.connectorType(config), ConnectorType.SOURCE);

        // We expect there to be errors due to now allowed override policy for ACKS.... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(SampleSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
            ConnectorConfig.COMMON_GROUP,
            ConnectorConfig.TRANSFORMS_GROUP,
            ConnectorConfig.PREDICATES_GROUP,
            ConnectorConfig.ERROR_GROUP,
            SourceConnectorConfig.TOPIC_CREATION_GROUP,
            SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_GROUP,
            SourceConnectorConfig.OFFSETS_TOPIC_GROUP
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        // Base connector config has 15 fields, connector's configs add 7, and 2 producer overrides
        assertEquals(24, result.values().size());
        assertTrue(result.values().stream().anyMatch(
            configInfo -> ackConfigKey.equals(configInfo.configValue().name()) && !configInfo.configValue().errors().isEmpty()));
        assertTrue(result.values().stream().anyMatch(
            configInfo -> saslConfigKey.equals(configInfo.configValue().name()) && configInfo.configValue().errors().isEmpty()));

        verifyValidationIsolation();
    }

    @Test
    public void testConfigValidationAllOverride() {
        final Class<? extends Connector> connectorClass = SampleSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, new AllConnectorClientConfigOverridePolicy());

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        // Try to test a variety of configuration types: string, int, long, boolean, list, class
        String protocolConfigKey = producerOverrideKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        config.put(protocolConfigKey, "SASL_PLAINTEXT");
        String maxRequestSizeConfigKey = producerOverrideKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        config.put(maxRequestSizeConfigKey, "420");
        String maxBlockConfigKey = producerOverrideKey(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        config.put(maxBlockConfigKey, "28980");
        String idempotenceConfigKey = producerOverrideKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        config.put(idempotenceConfigKey, "true");
        String bootstrapServersConfigKey = producerOverrideKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        config.put(bootstrapServersConfigKey, "SASL_PLAINTEXT://localhost:12345,SASL_PLAINTEXT://localhost:23456");
        String loginCallbackHandlerConfigKey = producerOverrideKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        config.put(loginCallbackHandlerConfigKey, OAuthBearerUnsecuredLoginCallbackHandler.class.getName());

        final Set<String> overriddenClientConfigs = new HashSet<>();
        overriddenClientConfigs.add(protocolConfigKey);
        overriddenClientConfigs.add(maxRequestSizeConfigKey);
        overriddenClientConfigs.add(maxBlockConfigKey);
        overriddenClientConfigs.add(idempotenceConfigKey);
        overriddenClientConfigs.add(bootstrapServersConfigKey);
        overriddenClientConfigs.add(loginCallbackHandlerConfigKey);

        ConfigInfos result = herder.validateConnectorConfig(config, s -> null, false);
        assertEquals(herder.connectorType(config), ConnectorType.SOURCE);

        Map<String, String> validatedOverriddenClientConfigs = new HashMap<>();
        for (ConfigInfo configInfo : result.values()) {
            String configName = configInfo.configKey().name();
            if (overriddenClientConfigs.contains(configName)) {
                validatedOverriddenClientConfigs.put(configName, configInfo.configValue().value());
            }
        }
        Map<String, String> rawOverriddenClientConfigs = config.entrySet().stream()
            .filter(e -> overriddenClientConfigs.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(rawOverriddenClientConfigs, validatedOverriddenClientConfigs);

        verifyValidationIsolation();
    }

    @Test
    public void testReverseTransformConfigs() {
        // Construct a task config with constant values for TEST_KEY and TEST_KEY2
        Map<String, String> newTaskConfig = new HashMap<>();
        newTaskConfig.put(TaskConfig.TASK_CLASS_CONFIG, SampleSourceConnector.SampleSourceTask.class.getName());
        newTaskConfig.put(TEST_KEY, TEST_VAL);
        newTaskConfig.put(TEST_KEY2, TEST_VAL2);
        List<Map<String, String>> newTaskConfigs = new ArrayList<>();
        newTaskConfigs.add(newTaskConfig);

        // The SNAPSHOT has a task config with TEST_KEY and TEST_REF
        List<Map<String, String>> reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT, newTaskConfigs);
        assertEquals(TEST_REF, reverseTransformed.get(0).get(TEST_KEY));

        // The SNAPSHOT has no task configs but does have a connector config with TEST_KEY2 and TEST_REF2
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertEquals(TEST_REF2, reverseTransformed.get(0).get(TEST_KEY2));

        // The reverseTransformed result should not have TEST_KEY3 since newTaskConfigs does not have TEST_KEY3
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertFalse(reverseTransformed.get(0).containsKey(TEST_KEY3));
    }

    private void assertErrorForKey(ConfigInfos configInfos, String testKey) {
        final List<String> errorsForKey = configInfos.values().stream()
                .map(ConfigInfo::configValue)
                .filter(configValue -> configValue.name().equals(testKey))
                .map(ConfigValueInfo::errors)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        assertEquals(1, errorsForKey.size());
    }

    @Test
    public void testConfigProviderRegex() {
        testConfigProviderRegex("\"${::}\"");
        testConfigProviderRegex("${::}");
        testConfigProviderRegex("\"${:/a:somevar}\"");
        testConfigProviderRegex("\"${file::somevar}\"");
        testConfigProviderRegex("${file:/a/b/c:}");
        testConfigProviderRegex("${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} not null");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} password=${file:/tmp/somefile.txt:othervar}");
        testConfigProviderRegex("plain.PlainLoginModule required username", false);
    }

    @Test
    public void testGenerateResultWithConfigValuesAllUsingConfigKeysAndWithNoErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(0, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1");
    }

    @Test
    public void testGenerateResultWithConfigValuesAllUsingConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(1, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
    }

    @Test
    public void testGenerateResultWithConfigValuesMoreThanConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");
        addValue(values, "config.extra1", "value.extra1");
        addValue(values, "config.extra2", "value.extra2", "error extra2");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(2, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertNoInfoKey(infos, "config.extra1");
        assertNoInfoKey(infos, "config.extra2");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
        assertInfoValue(infos, "config.extra1", "value.extra1");
        assertInfoValue(infos, "config.extra2", "value.extra2", "error extra2");
    }

    @Test
    public void testGenerateResultWithConfigValuesWithNoConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();

        List<String> groups = new ArrayList<>();
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");
        addValue(values, "config.extra1", "value.extra1");
        addValue(values, "config.extra2", "value.extra2", "error extra2");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(2, infos.errorCount());
        assertNoInfoKey(infos, "config.a1");
        assertNoInfoKey(infos, "config.b1");
        assertNoInfoKey(infos, "config.b2");
        assertNoInfoKey(infos, "config.c1");
        assertNoInfoKey(infos, "config.extra1");
        assertNoInfoKey(infos, "config.extra2");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
        assertInfoValue(infos, "config.extra1", "value.extra1");
        assertInfoValue(infos, "config.extra2", "value.extra2", "error extra2");
    }

    @Test
    public void testSinkConnectorPluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "sink",
                SampleSinkConnector::new,
                SampleSinkConnector::config,
                Optional.of(SinkConnectorConfig.configDef())
        );
    }

    @Test
    public void testSinkConnectorPluginConfigIncludingCommon() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "sink",
                SampleSinkConnector::new,
                SampleSinkConnector::configWithCommon,
                Optional.empty()
        );
    }

    @Test
    public void testSourceConnectorPluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "source",
                SampleSourceConnector::new,
                SampleSourceConnector::config,
                Optional.of(SourceConnectorConfig.configDef())
        );
    }

    @Test
    public void testSourceConnectorPluginConfigIncludingCommon() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "source",
                SampleSourceConnector::new,
                SampleSourceConnector::configWithCommon,
                Optional.empty()
        );
    }

    @Test
    public void testConverterPluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "converter",
                SampleConverterWithHeaders::new,
                SampleConverterWithHeaders::config,
                Optional.empty()
        );
    }

    @Test
    public void testHeaderConverterPluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "header-converter",
                SampleHeaderConverter::new,
                SampleHeaderConverter::config,
                Optional.empty()
        );
    }

    @Test
    public void testPredicatePluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "predicate",
                SamplePredicate::new,
                SamplePredicate::config,
                Optional.empty()
        );
    }

    @Test
    public void testTransformationPluginConfig() throws ClassNotFoundException {
        testConnectorPluginConfig(
                "transformation",
                SampleTransformation::new,
                SampleTransformation::config,
                Optional.empty()
        );
    }

    private <T> void testConnectorPluginConfig(
            String pluginName,
            Supplier<T> newPluginInstance,
            Function<T, ConfigDef> pluginConfig,
            Optional<ConfigDef> baseConfig
    ) throws ClassNotFoundException {
        AbstractHerder herder = testHerder();

        when(plugins.pluginClass(pluginName)).then(invocation -> newPluginInstance.get().getClass());
        when(plugins.newPlugin(anyString())).then(invocation -> newPluginInstance.get());
        when(herder.plugins()).thenReturn(plugins);

        List<ConfigKeyInfo> configs = herder.connectorPluginConfig(pluginName);
        assertNotNull(configs);

        ConfigDef expectedConfig = pluginConfig.apply(newPluginInstance.get());
        int expectedConfigSize = baseConfig.map(config -> config.names().size()).orElse(0)
                + expectedConfig.names().size();
        assertEquals(expectedConfigSize, configs.size());
        // Make sure that we used the correct class loader when interacting with the plugin
        verify(plugins).withClassLoader(newPluginInstance.get().getClass().getClassLoader());
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorConfigDefWithBadName() throws Exception {
        String connName = "AnotherPlugin";
        AbstractHerder herder = testHerder();
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.pluginClass(anyString())).thenThrow(new ClassNotFoundException());
        herder.connectorPluginConfig(connName);
    }

    @Test(expected = BadRequestException.class)
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testGetConnectorConfigDefWithInvalidPluginType() throws Exception {
        String connName = "AnotherPlugin";
        AbstractHerder herder = testHerder();
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.pluginClass(anyString())).thenReturn((Class) Object.class);
        when(plugins.newPlugin(anyString())).thenReturn(new DirectoryConfigProvider());
        herder.connectorPluginConfig(connName);
    }

    @Test
    public void testGetConnectorTypeWithMissingPlugin() {
        String connName = "AnotherPlugin";
        AbstractHerder herder = testHerder();
        when(worker.getPlugins()).thenReturn(plugins);
        when(plugins.newConnector(anyString())).thenThrow(new ConnectException("No class found"));
        assertEquals(ConnectorType.UNKNOWN, herder.connectorType(Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connName)));
    }

    @Test
    public void testGetConnectorTypeWithNullConfig() {
        AbstractHerder herder = testHerder();
        assertEquals(ConnectorType.UNKNOWN, herder.connectorType(null));
    }

    @Test
    public void testGetConnectorTypeWithEmptyConfig() {
        AbstractHerder herder = testHerder();
        assertEquals(ConnectorType.UNKNOWN, herder.connectorType(Collections.emptyMap()));
    }

    @Test
    public void testConnectorOffsetsConnectorNotFound() {
        when(configStore.snapshot()).thenReturn(SNAPSHOT);
        AbstractHerder herder = testHerder();
        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        herder.connectorOffsets("unknown-connector", cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    @Test
    public void testConnectorOffsets() throws Exception {
        ConnectorOffsets offsets = new ConnectorOffsets(Arrays.asList(
                new ConnectorOffset(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                new ConnectorOffset(Collections.singletonMap("partitionKey", "partitionValue2"), Collections.singletonMap("offsetKey", "offsetValue"))
        ));
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Callback<ConnectorOffsets>> workerCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            workerCallback.getValue().onCompletion(null, offsets);
            return null;
        }).when(worker).connectorOffsets(eq(CONN1), eq(CONN1_CONFIG), workerCallback.capture());
        AbstractHerder herder = testHerder();
        when(configStore.snapshot()).thenReturn(SNAPSHOT);

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        herder.connectorOffsets(CONN1, cb);
        assertEquals(offsets, cb.get(1000, TimeUnit.MILLISECONDS));
    }

    protected void addConfigKey(Map<String, ConfigDef.ConfigKey> keys, String name, String group) {
        ConfigDef configDef = new ConfigDef().define(name, ConfigDef.Type.STRING, null, null,
                ConfigDef.Importance.HIGH, "doc", group, 10,
                ConfigDef.Width.MEDIUM, "display name", Collections.emptyList(), null, null);
        keys.putAll(configDef.configKeys());
    }

    protected void addValue(List<ConfigValue> values, String name, String value, String...errors) {
        values.add(new ConfigValue(name, value, new ArrayList<>(), Arrays.asList(errors)));
    }

    protected void assertInfoKey(ConfigInfos infos, String name, String group) {
        ConfigInfo info = findInfo(infos, name);
        assertEquals(name, info.configKey().name());
        assertEquals(group, info.configKey().group());
    }

    protected void assertNoInfoKey(ConfigInfos infos, String name) {
        ConfigInfo info = findInfo(infos, name);
        assertNull(info.configKey());
    }

    protected void assertInfoValue(ConfigInfos infos, String name, String value, String...errors) {
        ConfigValueInfo info = findInfo(infos, name).configValue();
        assertEquals(name, info.name());
        assertEquals(value, info.value());
        assertEquals(Arrays.asList(errors), info.errors());
    }

    protected ConfigInfo findInfo(ConfigInfos infos, String name) {
        return infos.values()
                    .stream()
                    .filter(i -> i.configValue().name().equals(name))
                    .findFirst()
                    .orElse(null);
    }

    private void testConfigProviderRegex(String rawConnConfig) {
        testConfigProviderRegex(rawConnConfig, true);
    }

    private void testConfigProviderRegex(String rawConnConfig, boolean expected) {
        Set<String> keys = keysWithVariableValues(Collections.singletonMap("key", rawConnConfig), ConfigTransformer.DEFAULT_PATTERN);
        boolean actual = keys != null && !keys.isEmpty() && keys.contains("key");
        assertEquals(String.format("%s should have matched regex", rawConnConfig), expected, actual);
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass,
                                                        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        return createConfigValidationHerder(connectorClass, connectorClientConfigOverridePolicy, 1);
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass,
                                                        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                                        int countOfCallingNewConnector) {

        AbstractHerder herder = testHerder(connectorClientConfigOverridePolicy);

        // Call to validateConnectorConfig
        when(worker.configTransformer()).thenReturn(transformer);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, String>> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(mapArgumentCaptor.capture())).thenAnswer(invocation -> mapArgumentCaptor.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        final Connector connector;
        try {
            connector = connectorClass.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Couldn't create connector", e);
        }
        if (countOfCallingNewConnector > 0) {
            mockValidationIsolation(connectorClass.getName(), connector);
        }
        return herder;
    }

    private AbstractHerder testHerder() {
        return testHerder(noneConnectorClientConfigOverridePolicy);
    }

    private AbstractHerder testHerder(ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        return mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, connectorClientConfigOverridePolicy, Time.SYSTEM)
                .defaultAnswer(CALLS_REAL_METHODS));
    }

    private void mockValidationIsolation(String connectorClass, Connector connector) {
        when(plugins.newConnector(connectorClass)).thenReturn(connector);
        when(plugins.connectorLoader(connectorClass)).thenReturn(classLoader);
        when(plugins.withClassLoader(classLoader)).thenReturn(loaderSwap);
    }

    private void verifyValidationIsolation() {
        verify(plugins).newConnector(anyString());
        verify(plugins).withClassLoader(classLoader);
        verify(loaderSwap).close();
    }


    private static String producerOverrideKey(String config) {
        return ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + config;
    }
}
