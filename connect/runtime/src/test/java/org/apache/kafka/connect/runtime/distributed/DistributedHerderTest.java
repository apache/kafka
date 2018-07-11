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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder.HerderMetrics;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DistributedHerder.class, Plugins.class})
@PowerMockIgnore("javax.management.*")
public class DistributedHerderTest {
    private static final Map<String, String> HERDER_CONFIG = new HashMap<>();
    static {
        HERDER_CONFIG.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        HERDER_CONFIG.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        HERDER_CONFIG.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        HERDER_CONFIG.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
        // The WorkerConfig base class has some required settings without defaults
        HERDER_CONFIG.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
    }
    private static final String MEMBER_URL = "memberUrl";

    private static final String CONN1 = "sourceA";
    private static final String CONN2 = "sourceB";
    private static final ConnectorTaskId TASK0 = new ConnectorTaskId(CONN1, 0);
    private static final ConnectorTaskId TASK1 = new ConnectorTaskId(CONN1, 1);
    private static final ConnectorTaskId TASK2 = new ConnectorTaskId(CONN1, 2);
    private static final Integer MAX_TASKS = 3;
    private static final Map<String, String> CONN1_CONFIG = new HashMap<>();
    static {
        CONN1_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN1);
        CONN1_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN1_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final Map<String, String> CONN1_CONFIG_UPDATED = new HashMap<>(CONN1_CONFIG);
    static {
        CONN1_CONFIG_UPDATED.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar,baz");
    }
    private static final Map<String, String> CONN2_CONFIG = new HashMap<>();
    static {
        CONN2_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN2);
        CONN2_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN2_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN2_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
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
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.<String>emptySet());
    private static final ClusterConfigState SNAPSHOT_PAUSED_CONN1 = new ClusterConfigState(1, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.PAUSED),
            TASK_CONFIGS_MAP, Collections.<String>emptySet());
    private static final ClusterConfigState SNAPSHOT_UPDATED_CONN1_CONFIG = new ClusterConfigState(1, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG_UPDATED), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.<String>emptySet());

    private static final String WORKER_ID = "localhost:8083";
    private static final String KAFKA_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    @Mock private ConfigBackingStore configBackingStore;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private WorkerGroupMember member;
    private MockTime time;
    private DistributedHerder herder;
    private MockConnectMetrics metrics;
    @Mock private Worker worker;
    @Mock private Callback<Herder.Created<ConnectorInfo>> putConnectorCallback;
    @Mock
    private Plugins plugins;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private DelegatingClassLoader delegatingLoader;

    private ConfigBackingStore.UpdateListener configUpdateListener;
    private WorkerRebalanceListener rebalanceListener;

    private SinkConnectorConfig conn1SinkConfig;
    private SinkConnectorConfig conn1SinkConfigUpdated;

    @Before
    public void setUp() throws Exception {
        time = new MockTime();
        metrics = new MockConnectMetrics(time);
        worker = PowerMock.createMock(Worker.class);
        EasyMock.expect(worker.isSinkConnector(CONN1)).andStubReturn(Boolean.TRUE);

        herder = PowerMock.createPartialMock(DistributedHerder.class,
                new String[]{"backoff", "connectorTypeForClass", "updateDeletedConnectorStatus"},
                new DistributedConfig(HERDER_CONFIG), worker, WORKER_ID, KAFKA_CLUSTER_ID,
                statusBackingStore, configBackingStore, member, MEMBER_URL, metrics, time);

        configUpdateListener = herder.new ConfigUpdateListener();
        rebalanceListener = herder.new RebalanceListener();
        plugins = PowerMock.createMock(Plugins.class);
        conn1SinkConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);
        conn1SinkConfigUpdated = new SinkConnectorConfig(plugins, CONN1_CONFIG_UPDATED);
        EasyMock.expect(herder.connectorTypeForClass(BogusSourceConnector.class.getName())).andReturn(ConnectorType.SOURCE).anyTimes();
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        delegatingLoader = PowerMock.createMock(DelegatingClassLoader.class);
        PowerMock.mockStatic(Plugins.class);
        PowerMock.expectPrivate(herder, "updateDeletedConnectorStatus").andVoid().anyTimes();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testJoinAssignment() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);

        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testRebalance() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.<ConnectorTaskId>asList());

        // and the new assignment started
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);
        herder.tick();

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);
        herder.tick();

        time.sleep(3000L);
        assertStatistics(3, 2, 100, 3000);

        PowerMock.verifyAll();
    }

    @Test
    public void testRebalanceFailedConnector() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.<ConnectorTaskId>asList());

        // and the new assignment started
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(false);

        // worker is not running, so we should see no call to connectorTaskConfigs()

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        herder.tick();
        time.sleep(2000L);
        assertStatistics(3, 2, 100, 2000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testHaltCleansUpWorker() {
        EasyMock.expect(worker.connectorNames()).andReturn(Collections.singleton(CONN1));
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.taskIds()).andReturn(Collections.singleton(TASK1));
        worker.stopAndAwaitTask(TASK1);
        PowerMock.expectLastCall();
        member.stop();
        PowerMock.expectLastCall();
        configBackingStore.stop();
        PowerMock.expectLastCall();
        statusBackingStore.stop();
        PowerMock.expectLastCall();
        worker.stop();
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.halt();

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();

        // config validation
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);
        EasyMock.expect(connectorMock.config()).andReturn(new ConfigDef());
        EasyMock.expect(connectorMock.validate(CONN2_CONFIG)).andReturn(new Config(Collections.<ConfigValue>emptyList()));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        // CONN2 is new, should succeed
        configBackingStore.putConnectorConfig(CONN2, CONN2_CONFIG);
        PowerMock.expectLastCall();
        ConnectorInfo info = new ConnectorInfo(CONN2, CONN2_CONFIG, Collections.<ConnectorTaskId>emptyList(),
            ConnectorType.SOURCE);
        putConnectorCallback.onCompletion(null, new Herder.Created<>(true, info));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        herder.tick();

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorFailedBasicValidation() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);

        HashMap<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.remove(ConnectorConfig.NAME_CONFIG);

        member.wakeup();
        PowerMock.expectLastCall();

        // config validation
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);

        EasyMock.expect(connectorMock.config()).andStubReturn(new ConfigDef());
        ConfigValue validatedValue = new ConfigValue("foo.bar");
        EasyMock.expect(connectorMock.validate(config)).andReturn(new Config(singletonList(validatedValue)));

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        // CONN2 creation should fail

        Capture<Throwable> error = EasyMock.newCapture();
        putConnectorCallback.onCompletion(EasyMock.capture(error), EasyMock.<Herder.Created<ConnectorInfo>>isNull());
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, config, false, putConnectorCallback);
        herder.tick();

        assertTrue(error.hasCaptured());
        assertTrue(error.getValue() instanceof BadRequestException);

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorFailedCustomValidation() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();

        // config validation
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);

        ConfigDef configDef = new ConfigDef();
        configDef.define("foo.bar", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "foo.bar doc");
        EasyMock.expect(connectorMock.config()).andReturn(configDef);

        ConfigValue validatedValue = new ConfigValue("foo.bar");
        validatedValue.addErrorMessage("Failed foo.bar validation");
        EasyMock.expect(connectorMock.validate(CONN2_CONFIG)).andReturn(new Config(singletonList(validatedValue)));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        // CONN2 creation should fail

        Capture<Throwable> error = EasyMock.newCapture();
        putConnectorCallback.onCompletion(EasyMock.capture(error), EasyMock.<Herder.Created<ConnectorInfo>>isNull());
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        herder.tick();

        assertTrue(error.hasCaptured());
        assertTrue(error.getValue() instanceof BadRequestException);

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorNameConflictsWithWorkerGroupId() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();

        Map<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.put(ConnectorConfig.NAME_CONFIG, "test-group");

        // config validation
        Connector connectorMock = PowerMock.createMock(SinkConnector.class);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).times(3);
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);
        EasyMock.expect(connectorMock.config()).andReturn(new ConfigDef());
        EasyMock.expect(connectorMock.validate(config)).andReturn(new Config(Collections.<ConfigValue>emptyList()));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        // CONN2 creation should fail because the worker group id (connect-test-group) conflicts with
        // the consumer group id we would use for this sink

        Capture<Throwable> error = EasyMock.newCapture();
        putConnectorCallback.onCompletion(EasyMock.capture(error), EasyMock.isNull(Herder.Created.class));
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, config, false, putConnectorCallback);
        herder.tick();

        assertTrue(error.hasCaptured());
        assertTrue(error.getValue() instanceof BadRequestException);

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(null);
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();
        // CONN1 already exists
        putConnectorCallback.onCompletion(EasyMock.<AlreadyExistsException>anyObject(), EasyMock.<Herder.Created<ConnectorInfo>>isNull());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN1, CONN1_CONFIG, false, putConnectorCallback);
        herder.tick();

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        // Start with one connector
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);

        // And delete the connector
        member.wakeup();
        PowerMock.expectLastCall();
        configBackingStore.removeConnectorConfig(CONN1);
        PowerMock.expectLastCall();
        putConnectorCallback.onCompletion(null, new Herder.Created<ConnectorInfo>(false, null));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.deleteConnectorConfig(CONN1, putConnectorCallback);
        herder.tick();

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnector() throws Exception {
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andStubReturn(TASK_CONFIGS);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        expectRebalance(1, singletonList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);

        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        worker.stopConnector(CONN1);
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
        herder.tick();
        callback.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartUnknownConnector() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();


        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN2, callback);
        herder.tick();
        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorRedirectToLeader() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
        herder.tick();

        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotLeaderException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorRedirectToOwner() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        String ownerUrl = "ownerUrl";
        EasyMock.expect(member.ownerUrl(CONN1)).andReturn(ownerUrl);

        PowerMock.replayAll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
        herder.tick();

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 3000L);

        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotAssignedException);
            NotAssignedException notAssignedException = (NotAssignedException) e.getCause();
            assertEquals(ownerUrl, notAssignedException.forwardUrl());
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTask() throws Exception {
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andStubReturn(TASK_CONFIGS);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), singletonList(TASK0));
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);

        // now handle the task restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        worker.stopAndAwaitTask(TASK0);
        PowerMock.expectLastCall();
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();
        callback.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartUnknownTask() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.tick();
        herder.restartTask(new ConnectorTaskId("blah", 0), callback);
        herder.tick();

        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRequestProcessingOrder() throws Exception {
        final DistributedHerder.DistributedHerderRequest req1 = herder.addRequest(100, null, null);
        final DistributedHerder.DistributedHerderRequest req2 = herder.addRequest(10, null, null);
        final DistributedHerder.DistributedHerderRequest req3 = herder.addRequest(200, null, null);
        final DistributedHerder.DistributedHerderRequest req4 = herder.addRequest(200, null, null);

        assertEquals(req2, herder.requests.pollFirst()); // lowest delay
        assertEquals(req1, herder.requests.pollFirst()); // next lowest delay
        assertEquals(req3, herder.requests.pollFirst()); // same delay as req4, but added first
        assertEquals(req4, herder.requests.pollFirst());
    }

    @Test
    public void testRestartTaskRedirectToLeader() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // now handle the task restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();

        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotLeaderException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskRedirectToOwner() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // now handle the task restart
        String ownerUrl = "ownerUrl";
        EasyMock.expect(member.ownerUrl(TASK0)).andReturn(ownerUrl);
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();

        try {
            callback.get(1000L, TimeUnit.MILLISECONDS);
            fail("Expected NotLeaderException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotAssignedException);
            NotAssignedException notAssignedException = (NotAssignedException) e.getCause();
            assertEquals(ownerUrl, notAssignedException.forwardUrl());
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorConfigAdded() {
        // If a connector was added, we need to rebalance
        EasyMock.expect(member.memberId()).andStubReturn("member");

        // join, no configs so no need to catch up on config topic
        expectRebalance(-1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();
        // Checks for config updates and starts rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick(); // do rebalance

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorConfigUpdate() throws Exception {
        // Connector config can be applied without any rebalance

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT); // for this test, it doesn't matter if we use the same config snapshot
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall().andReturn(true);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorPaused() throws Exception {
        // ensure that target state changes are propagated to the worker

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_PAUSED_CONN1);
        PowerMock.expectLastCall();

        worker.setTargetState(CONN1, TargetState.PAUSED);
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // worker should apply the state change

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorResumed() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // start with the connector paused
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT_PAUSED_CONN1);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.PAUSED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        PowerMock.expectLastCall();

        // we expect reconfiguration after resuming
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);

        worker.setTargetState(CONN1, TargetState.STARTED);
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to started
        herder.tick(); // apply state change

        PowerMock.verifyAll();
    }

    @Test
    public void testUnknownConnectorPaused() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Collections.<String>emptyList(), singletonList(TASK0));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // state change is ignored since we have no target state
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange("unknown-connector");
        herder.tick(); // continue

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorPausedRunningTaskOnly() throws Exception {
        // even if we don't own the connector, we should still propagate target state
        // changes to the worker so that tasks will transition correctly

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.<String>emptySet());

        // join
        expectRebalance(1, Collections.<String>emptyList(), singletonList(TASK0));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_PAUSED_CONN1);
        PowerMock.expectLastCall();

        worker.setTargetState(CONN1, TargetState.PAUSED);
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // apply state change

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorResumedRunningTaskOnly() throws Exception {
        // even if we don't own the connector, we should still propagate target state
        // changes to the worker so that tasks will transition correctly

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.<String>emptySet());

        // join
        expectRebalance(1, Collections.<String>emptyList(), singletonList(TASK0));
        expectPostRebalanceCatchup(SNAPSHOT_PAUSED_CONN1);
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.PAUSED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        PowerMock.expectLastCall();

        worker.setTargetState(CONN1, TargetState.STARTED);
        PowerMock.expectLastCall();

        EasyMock.expect(worker.isRunning(CONN1)).andReturn(false);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // apply state change

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskConfigAdded() {
        // Task config always requires rebalance
        EasyMock.expect(member.memberId()).andStubReturn("member");

        // join
        expectRebalance(-1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();
        // Checks for config updates and starts rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Collections.<String>emptyList(),
                Arrays.asList(TASK0));
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK0, TASK1, TASK2)); // read updated config
        herder.tick(); // apply config
        herder.tick(); // do rebalance

        PowerMock.verifyAll();
    }

    @Test
    public void testJoinLeaderCatchUpFails() throws Exception {
        // Join group and as leader fail to do assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, Collections.<String>emptyList(),
                Collections.<ConnectorTaskId>emptyList());
        // Reading to end of log times out
        configBackingStore.refresh(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall().andThrow(new TimeoutException());
        member.maybeLeaveGroup();
        EasyMock.expectLastCall();
        PowerMock.expectPrivate(herder, "backoff", DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT);
        member.requestRejoin();

        // After backoff, restart the process and this time succeed
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);

        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<ClusterConfigState>anyObject(), EasyMock.<Map<String, String>>anyObject(), EasyMock.<Map<String, String>>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics("leaderUrl", true, 3, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

        herder.tick();
        time.sleep(2000L);
        assertStatistics("leaderUrl", false, 3, 1, 100, 2000L);


        PowerMock.verifyAll();
    }

    @Test
    public void testAccessors() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).anyTimes();
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);


        member.wakeup();
        PowerMock.expectLastCall().anyTimes();
        // list connectors, get connector info, get connector config, get task configs
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();


        PowerMock.replayAll();

        FutureCallback<Collection<String>> listConnectorsCb = new FutureCallback<>();
        herder.connectors(listConnectorsCb);
        FutureCallback<ConnectorInfo> connectorInfoCb = new FutureCallback<>();
        herder.connectorInfo(CONN1, connectorInfoCb);
        FutureCallback<Map<String, String>> connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        FutureCallback<List<TaskInfo>> taskConfigsCb = new FutureCallback<>();
        herder.taskConfigs(CONN1, taskConfigsCb);

        herder.tick();
        assertTrue(listConnectorsCb.isDone());
        assertEquals(Collections.singleton(CONN1), listConnectorsCb.get());
        assertTrue(connectorInfoCb.isDone());
        ConnectorInfo info = new ConnectorInfo(CONN1, CONN1_CONFIG, Arrays.asList(TASK0, TASK1, TASK2),
            ConnectorType.SOURCE);
        assertEquals(info, connectorInfoCb.get());
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG, connectorConfigCb.get());
        assertTrue(taskConfigsCb.isDone());
        assertEquals(Arrays.asList(
                        new TaskInfo(TASK0, TASK_CONFIG),
                        new TaskInfo(TASK1, TASK_CONFIG),
                        new TaskInfo(TASK2, TASK_CONFIG)),
                taskConfigsCb.get());

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andReturn(TASK_CONFIGS);

        // list connectors, get connector info, get connector config, get task configs
        member.wakeup();
        PowerMock.expectLastCall().anyTimes();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Poll loop for second round of calls
        member.ensureActive();
        PowerMock.expectLastCall();

        // config validation
        Connector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).anyTimes();
        EasyMock.expect(plugins.compareAndSwapLoaders(connectorMock)).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString())).andReturn(connectorMock);
        EasyMock.expect(connectorMock.config()).andReturn(new ConfigDef());
        EasyMock.expect(connectorMock.validate(CONN1_CONFIG_UPDATED)).andReturn(new Config(Collections.<ConfigValue>emptyList()));
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        configBackingStore.putConnectorConfig(CONN1, CONN1_CONFIG_UPDATED);
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                // Simulate response to writing config + waiting until end of log to be read
                configUpdateListener.onConnectorConfigUpdate(CONN1);
                return null;
            }
        });
        // As a result of reconfig, should need to update snapshot. With only connector updates, we'll just restart
        // connector without rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_UPDATED_CONN1_CONFIG);
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall().andReturn(true);
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.<Map<String, String>>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfigUpdated)).andReturn(TASK_CONFIGS);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Third tick just to read the config
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        // Should pick up original config
        FutureCallback<Map<String, String>> connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        herder.tick();
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG, connectorConfigCb.get());

        // Apply new config.
        FutureCallback<Herder.Created<ConnectorInfo>> putConfigCb = new FutureCallback<>();
        herder.putConnectorConfig(CONN1, CONN1_CONFIG_UPDATED, true, putConfigCb);
        herder.tick();
        assertTrue(putConfigCb.isDone());
        ConnectorInfo updatedInfo = new ConnectorInfo(CONN1, CONN1_CONFIG_UPDATED, Arrays.asList(TASK0, TASK1, TASK2),
            ConnectorType.SOURCE);
        assertEquals(new Herder.Created<>(false, updatedInfo), putConfigCb.get());

        // Check config again to validate change
        connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        herder.tick();
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG_UPDATED, connectorConfigCb.get());
        PowerMock.verifyAll();
    }

    @Test
    public void testInconsistentConfigs() throws Exception {
        // FIXME: if we have inconsistent configs, we need to request forced reconfig + write of the connector's task configs
        // This requires inter-worker communication, so needs the REST API
    }

    private void expectRebalance(final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks) {
        expectRebalance(null, null, ConnectProtocol.Assignment.NO_ERROR, offset, assignedConnectors, assignedTasks);
    }

    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final Collection<String> revokedConnectors,
                                 final List<ConnectorTaskId> revokedTasks,
                                 final short error,
                                 final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks) {
        member.ensureActive();
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                if (revokedConnectors != null)
                    rebalanceListener.onRevoked("leader", revokedConnectors, revokedTasks);
                ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(
                        error, "leader", "leaderUrl", offset, assignedConnectors, assignedTasks);
                rebalanceListener.onAssigned(assignment, 3);
                time.sleep(100L);
                return null;
            }
        });

        if (revokedConnectors != null) {
            for (String connector : revokedConnectors) {
                worker.stopConnector(connector);
                PowerMock.expectLastCall().andReturn(true);
            }
        }

        if (revokedTasks != null && !revokedTasks.isEmpty()) {
            worker.stopAndAwaitTask(EasyMock.anyObject(ConnectorTaskId.class));
            PowerMock.expectLastCall();
        }

        if (revokedConnectors != null) {
            statusBackingStore.flush();
            PowerMock.expectLastCall();
        }

        member.wakeup();
        PowerMock.expectLastCall();
    }

    private void expectPostRebalanceCatchup(final ClusterConfigState readToEndSnapshot) throws TimeoutException {
        configBackingStore.refresh(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();
        EasyMock.expect(configBackingStore.snapshot()).andReturn(readToEndSnapshot);
    }

    private void assertStatistics(int expectedEpoch, int completedRebalances, double rebalanceTime, double millisSinceLastRebalance) {
        String expectedLeader = completedRebalances <= 0 ? null : "leaderUrl";
        assertStatistics(expectedLeader, false, expectedEpoch, completedRebalances, rebalanceTime, millisSinceLastRebalance);
    }

    private void assertStatistics(String expectedLeader, boolean isRebalancing, int expectedEpoch, int completedRebalances, double rebalanceTime, double millisSinceLastRebalance) {
        HerderMetrics herderMetrics = herder.herderMetrics();
        MetricGroup group = herderMetrics.metricGroup();
        double epoch = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "epoch");
        String leader = MockConnectMetrics.currentMetricValueAsString(metrics, group, "leader-name");
        double rebalanceCompletedTotal = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "completed-rebalances-total");
        double rebalancing = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "rebalancing");
        double rebalanceTimeMax = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "rebalance-max-time-ms");
        double rebalanceTimeAvg = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "rebalance-avg-time-ms");
        double rebalanceTimeSinceLast = MockConnectMetrics.currentMetricValueAsDouble(metrics, group, "time-since-last-rebalance-ms");

        assertEquals(expectedEpoch, epoch, 0.0001d);
        assertEquals(expectedLeader, leader);
        assertEquals(completedRebalances, rebalanceCompletedTotal, 0.0001d);
        assertEquals(isRebalancing ? 1.0d : 0.0d, rebalancing, 0.0001d);
        assertEquals(millisSinceLastRebalance, rebalanceTimeSinceLast, 0.0001d);
        if (rebalanceTime <= 0L) {
            assertEquals(Double.NEGATIVE_INFINITY, rebalanceTimeMax, 0.0001d);
            assertEquals(0.0d, rebalanceTimeAvg, 0.0001d);
        } else {
            assertEquals(rebalanceTime, rebalanceTimeMax, 0.0001d);
            assertEquals(rebalanceTime, rebalanceTimeAvg, 0.0001d);
        }
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

}
