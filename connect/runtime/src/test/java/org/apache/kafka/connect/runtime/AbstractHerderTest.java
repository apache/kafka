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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.provider.DirectoryConfigProvider;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.MockStrict;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.AbstractHerder.keysWithVariableValues;
import static org.easymock.EasyMock.anyString;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.powermock.api.easymock.PowerMock.verifyAll;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractHerder.class})
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
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
        CONN1_CONFIG.put(TEST_KEY, TEST_REF);
        CONN1_CONFIG.put(TEST_KEY2, TEST_REF2);
        CONN1_CONFIG.put(TEST_KEY3, TEST_REF3);
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
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
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_NO_TASKS = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            Collections.emptyMap(), Collections.emptySet());

    private final String workerId = "workerId";
    private final String kafkaClusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private final int generation = 5;
    private final String connector = "connector";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();

    @MockStrict private Worker worker;
    @MockStrict private WorkerConfigTransformer transformer;
    @MockStrict private Plugins plugins;
    @MockStrict private ClassLoader classLoader;
    @MockStrict private ConfigBackingStore configStore;
    @MockStrict private StatusBackingStore statusStore;

    @Test
    public void testConnectors() {
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
            .withConstructor(
                Worker.class,
                String.class,
                String.class,
                StatusBackingStore.class,
                ConfigBackingStore.class,
                ConnectorClientConfigOverridePolicy.class
            )
            .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
            .addMockedMethod("generation")
            .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.rawConfig(connector)).andReturn(null);
        EasyMock.expect(configStore.snapshot()).andReturn(SNAPSHOT);
        replayAll();
        assertEquals(Collections.singleton(CONN1), new HashSet<>(herder.connectors()));
        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
            .withConstructor(
                Worker.class,
                String.class,
                String.class,
                StatusBackingStore.class,
                ConfigBackingStore.class,
                ConnectorClientConfigOverridePolicy.class
            )
            .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
            .addMockedMethod("generation")
            .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.rawConfig(connector)).andReturn(null);
        EasyMock.expect(statusStore.get(connector))
            .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));
        EasyMock.expect(statusStore.getAll(connector))
            .andReturn(Collections.singletonList(
                new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        replayAll();
        ConnectorStateInfo csi = herder.connectorStatus(connector);
        PowerMock.verifyAll();
    }

    @Test
    public void connectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.rawConfig(connector)).andReturn(null);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);

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

        PowerMock.verifyAll();
    }

    @Test
    public void taskStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        String workerId = "workerId";

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(5);

        final Capture<TaskStatus> statusCapture = EasyMock.newCapture();
        statusStore.putSafe(EasyMock.capture(statusCapture));
        EasyMock.expectLastCall();

        EasyMock.expect(statusStore.get(taskId)).andAnswer(statusCapture::getValue);

        replayAll();

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());

        verifyAll();
    }

    @Test
    public void testBuildRestartPlanForUnknownConnector() {
        String connectorName = "UnknownConnector";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);

        EasyMock.expect(statusStore.get(connectorName)).andReturn(null);
        replayAll();

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertFalse(mayBeRestartPlan.isPresent());
    }

    @Test()
    public void testConfigValidationNullConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put("name", "somename");
        config.put("required", "value");
        config.put("testKey", null);

        final ConfigInfos configInfos = herder.validateConnectorConfig(config, false);

        assertEquals(1, configInfos.errorCount());
        assertErrorForKey(configInfos, "testKey");

        verifyAll();
    }

    @Test
    public void testConfigValidationMultipleNullConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put("name", "somename");
        config.put("required", "value");
        config.put("testKey", null);
        config.put("secondTestKey", null);

        final ConfigInfos configInfos = herder.validateConnectorConfig(config, false);

        assertEquals(2, configInfos.errorCount());
        assertErrorForKey(configInfos, "testKey");
        assertErrorForKey(configInfos, "secondTestKey");

        verifyAll();
    }

    @Test
    public void testBuildRestartPlanForConnectorAndTasks() {
        RestartRequest restartRequest = new RestartRequest(connector, false, true);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connector, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connector, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.rawConfig(connector)).andReturn(null);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(taskStatuses);
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);

        replayAll();

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertTrue(restartPlan.shouldRestartConnector());
        assertTrue(restartPlan.shouldRestartTasks());
        assertEquals(2, restartPlan.taskIdsToRestart().size());
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId1));
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId2));

        PowerMock.verifyAll();
    }

    @Test
    public void testBuildRestartPlanForNoRestart() {
        RestartRequest restartRequest = new RestartRequest(connector, true, false);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connector, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connector, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.rawConfig(connector)).andReturn(null);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(taskStatuses);
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);

        replayAll();

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertFalse(restartPlan.shouldRestartConnector());
        assertFalse(restartPlan.shouldRestartTasks());
        assertTrue(restartPlan.taskIdsToRestart().isEmpty());

        PowerMock.verifyAll();
    }

    @Test
    public void testConfigValidationEmptyConfig() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy, 0);
        replayAll();

        assertThrows(BadRequestException.class, () -> herder.validateConnectorConfig(Collections.emptyMap(), false));

        verifyAll();
    }

    @Test()
    public void testConfigValidationMissingName() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        ConfigInfos result = herder.validateConnectorConfig(config, false);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(SampleSourceConnector.class.getName(), result.name());
        assertEquals(Arrays.asList(ConnectorConfig.COMMON_GROUP, ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP, ConnectorConfig.ERROR_GROUP, SourceConnectorConfig.TOPIC_CREATION_GROUP), result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        // Base connector config has 14 fields, connector's configs add 2
        assertEquals(17, infos.size());
        // Missing name should generate an error
        assertEquals(ConnectorConfig.NAME_CONFIG,
                infos.get(ConnectorConfig.NAME_CONFIG).configValue().name());
        assertEquals(1, infos.get(ConnectorConfig.NAME_CONFIG).configValue().errors().size());
        // "required" config from connector should generate an error
        assertEquals("required", infos.get("required").configValue().name());
        assertEquals(1, infos.get("required").configValue().errors().size());

        verifyAll();
    }

    @Test
    public void testConfigValidationInvalidTopics() {
        AbstractHerder herder = createConfigValidationHerder(SampleSinkConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1,topic2");
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verifyAll();
    }

    @Test
    public void testConfigValidationTopicsWithDlq() {
        AbstractHerder herder = createConfigValidationHerder(SampleSinkConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verifyAll();
    }

    @Test
    public void testConfigValidationTopicsRegexWithDlq() {
        AbstractHerder herder = createConfigValidationHerder(SampleSinkConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verifyAll();
    }

    @Test()
    public void testConfigValidationTransformsExtendResults() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation<?>>> transformations = new HashSet<>();
        transformations.add(transformationPluginDesc());
        EasyMock.expect(plugins.transformations()).andReturn(transformations).times(2);

        replayAll();

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA,xformB");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(SampleSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                "Transforms: xformA",
                "Transforms: xformB"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(22, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type",
                infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig",
                infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformB.type", infos.get("transforms.xformB.type").configValue().name());
        assertFalse(infos.get("transforms.xformB.type").configValue().errors().isEmpty());

        verifyAll();
    }

    @Test()
    public void testConfigValidationPredicatesExtendResults() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation<?>>> transformations = new HashSet<>();
        transformations.add(transformationPluginDesc());
        EasyMock.expect(plugins.transformations()).andReturn(transformations).times(1);

        Set<PluginDesc<Predicate<?>>> predicates = new HashSet<>();
        predicates.add(predicatePluginDesc());
        EasyMock.expect(plugins.predicates()).andReturn(predicates).times(2);

        replayAll();

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.predicate", "predX");
        config.put(ConnectorConfig.PREDICATES_CONFIG, "predX,predY");
        config.put(ConnectorConfig.PREDICATES_CONFIG + ".predX.type", SamplePredicate.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(SampleSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                "Transforms: xformA",
                "Predicates: predX",
                "Predicates: predY"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(24, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type",
                infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig",
                infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformA.predicate",
                infos.get("transforms.xformA.predicate").configValue().name());
        assertTrue(infos.get("transforms.xformA.predicate").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.negate",
                infos.get("transforms.xformA.negate").configValue().name());
        assertTrue(infos.get("transforms.xformA.negate").configValue().errors().isEmpty());
        assertEquals("predicates.predX.type",
                infos.get("predicates.predX.type").configValue().name());
        assertEquals("predicates.predX.predconfig",
                infos.get("predicates.predX.predconfig").configValue().name());
        assertEquals("predicates.predY.type",
                infos.get("predicates.predY.type").configValue().name());
        assertFalse(
                infos.get("predicates.predY.type").configValue().errors().isEmpty());

        verifyAll();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Predicate<?>> predicatePluginDesc() {
        return new PluginDesc(SamplePredicate.class, "1.0", classLoader);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Transformation<?>> transformationPluginDesc() {
        return new PluginDesc(SampleTransformation.class, "1.0", classLoader);
    }

    @Test()
    public void testConfigValidationPrincipalOnlyOverride() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, new PrincipalConnectorClientConfigOverridePolicy());
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        String ackConfigKey = producerOverrideKey(ProducerConfig.ACKS_CONFIG);
        String saslConfigKey = producerOverrideKey(SaslConfigs.SASL_JAAS_CONFIG);
        config.put(ackConfigKey, "none");
        config.put(saslConfigKey, "jaas_config");

        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to now allowed override policy for ACKS.... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(SampleSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
            ConnectorConfig.COMMON_GROUP,
            ConnectorConfig.TRANSFORMS_GROUP,
            ConnectorConfig.PREDICATES_GROUP,
            ConnectorConfig.ERROR_GROUP,
            SourceConnectorConfig.TOPIC_CREATION_GROUP
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        // Base connector config has 14 fields, connector's configs add 2, and 2 producer overrides
        assertEquals(19, result.values().size());
        assertTrue(result.values().stream().anyMatch(
            configInfo -> ackConfigKey.equals(configInfo.configValue().name()) && !configInfo.configValue().errors().isEmpty()));
        assertTrue(result.values().stream().anyMatch(
            configInfo -> saslConfigKey.equals(configInfo.configValue().name()) && configInfo.configValue().errors().isEmpty()));

        verifyAll();
    }

    @Test
    public void testConfigValidationAllOverride() {
        AbstractHerder herder = createConfigValidationHerder(SampleSourceConnector.class, new AllConnectorClientConfigOverridePolicy());
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
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

        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

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
        verifyAll();
    }

    @Test
    public void testReverseTransformConfigs() {
        // Construct a task config with constant values for TEST_KEY and TEST_KEY2
        Map<String, String> newTaskConfig = new HashMap<>();
        newTaskConfig.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
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
    public void testConnectorPluginConfig() throws Exception {
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(
                        Worker.class,
                        String.class,
                        String.class,
                        StatusBackingStore.class,
                        ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class
                )
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(plugins.newPlugin(EasyMock.anyString())).andAnswer(() -> {
            String name = (String) EasyMock.getCurrentArguments()[0];
            switch (name) {
                case "sink": return new SampleSinkConnector();
                case "source": return new SampleSourceConnector();
                case "converter": return new SampleConverterWithHeaders();
                case "header-converter": return new SampleHeaderConverter();
                case "predicate": return new SamplePredicate();
                default: return new SampleTransformation<>();
            }
        }).anyTimes();
        EasyMock.expect(herder.plugins()).andStubReturn(plugins);
        replayAll();

        List<ConfigKeyInfo> sinkConnectorConfigs = herder.connectorPluginConfig("sink");
        assertNotNull(sinkConnectorConfigs);
        assertEquals(new SampleSinkConnector().config().names().size(), sinkConnectorConfigs.size());

        List<ConfigKeyInfo> sourceConnectorConfigs = herder.connectorPluginConfig("source");
        assertNotNull(sourceConnectorConfigs);
        assertEquals(new SampleSourceConnector().config().names().size(), sourceConnectorConfigs.size());

        List<ConfigKeyInfo> converterConfigs = herder.connectorPluginConfig("converter");
        assertNotNull(converterConfigs);
        assertEquals(new SampleConverterWithHeaders().config().names().size(), converterConfigs.size());

        List<ConfigKeyInfo> headerConverterConfigs = herder.connectorPluginConfig("header-converter");
        assertNotNull(headerConverterConfigs);
        assertEquals(new SampleHeaderConverter().config().names().size(), headerConverterConfigs.size());

        List<ConfigKeyInfo> predicateConfigs = herder.connectorPluginConfig("predicate");
        assertNotNull(predicateConfigs);
        assertEquals(new SamplePredicate().config().names().size(), predicateConfigs.size());

        List<ConfigKeyInfo> transformationConfigs = herder.connectorPluginConfig("transformation");
        assertNotNull(transformationConfigs);
        assertEquals(new SampleTransformation<>().config().names().size(), transformationConfigs.size());
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorConfigDefWithBadName() throws Exception {
        String connName = "AnotherPlugin";
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        EasyMock.expect(plugins.newPlugin(anyString())).andThrow(new ClassNotFoundException());
        replayAll();
        herder.connectorPluginConfig(connName);
    }

    @Test(expected = BadRequestException.class)
    public void testGetConnectorConfigDefWithInvalidPluginType() throws Exception {
        String connName = "AnotherPlugin";
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                        ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        EasyMock.expect(plugins.newPlugin(anyString())).andReturn(new DirectoryConfigProvider());
        replayAll();
        herder.connectorPluginConfig(connName);
    }

    protected void addConfigKey(Map<String, ConfigDef.ConfigKey> keys, String name, String group) {
        keys.put(name, new ConfigDef.ConfigKey(name, ConfigDef.Type.STRING, null, null,
                ConfigDef.Importance.HIGH, "doc", group, 10,
                ConfigDef.Width.MEDIUM, "display name", Collections.emptyList(), null, false));
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


        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, connectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();
        EasyMock.expect(herder.generation()).andStubReturn(generation);

        // Call to validateConnectorConfig
        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        final Connector connector;
        try {
            connector = connectorClass.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Couldn't create connector", e);
        }
        if (countOfCallingNewConnector > 0) {
            EasyMock.expect(plugins.newConnector(connectorClass.getName())).andReturn(connector).times(countOfCallingNewConnector);
            EasyMock.expect(plugins.compareAndSwapLoaders(connector)).andReturn(classLoader).times(countOfCallingNewConnector);
        }

        return herder;
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private static String producerOverrideKey(String config) {
        return ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + config;
    }
}
