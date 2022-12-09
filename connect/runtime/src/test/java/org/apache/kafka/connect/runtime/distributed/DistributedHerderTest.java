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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.runtime.RestartPlan;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder.HerderMetrics;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.easymock.Capture;
import org.easymock.CaptureType;
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
import org.powermock.reflect.Whitebox;

import javax.crypto.SecretKey;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.apache.kafka.connect.runtime.AbstractStatus.State.FAILED;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.ExactlyOnceSupportLevel.REQUIRED;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.CONNECTOR;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.leq;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DistributedHerder.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
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
    private static final String FOO_TOPIC = "foo";
    private static final String BAR_TOPIC = "bar";
    private static final String BAZ_TOPIC = "baz";
    static {
        CONN1_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN1);
        CONN1_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN1_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", FOO_TOPIC, BAR_TOPIC));
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final Map<String, String> CONN1_CONFIG_UPDATED = new HashMap<>(CONN1_CONFIG);
    static {
        CONN1_CONFIG_UPDATED.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", FOO_TOPIC, BAR_TOPIC, BAZ_TOPIC));
    }
    private static final ConfigInfos CONN1_CONFIG_INFOS =
        new ConfigInfos(CONN1, 0, Collections.emptyList(), Collections.emptyList());
    private static final Map<String, String> CONN2_CONFIG = new HashMap<>();
    static {
        CONN2_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN2);
        CONN2_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN2_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", FOO_TOPIC, BAR_TOPIC));
        CONN2_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final ConfigInfos CONN2_CONFIG_INFOS =
        new ConfigInfos(CONN2, 0, Collections.emptyList(), Collections.emptyList());
    private static final ConfigInfos CONN2_INVALID_CONFIG_INFOS =
        new ConfigInfos(CONN2, 1, Collections.emptyList(), Collections.emptyList());
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
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_PAUSED_CONN1 = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.PAUSED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_UPDATED_CONN1_CONFIG = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG_UPDATED), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

    private static final String WORKER_ID = "localhost:8083";
    private static final String KAFKA_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";
    private static final Runnable EMPTY_RUNNABLE = () -> {
    };

    @Mock private ConfigBackingStore configBackingStore;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private WorkerGroupMember member;
    private MockTime time;
    private DistributedHerder herder;
    private MockConnectMetrics metrics;
    @Mock private Worker worker;
    @Mock private Callback<Herder.Created<ConnectorInfo>> putConnectorCallback;
    @Mock private Plugins plugins;
    @Mock private RestClient restClient;
    private CountDownLatch shutdownCalled = new CountDownLatch(1);

    private ConfigBackingStore.UpdateListener configUpdateListener;
    private WorkerRebalanceListener rebalanceListener;
    private ExecutorService herderExecutor;
    private Future<?> herderFuture;

    private SinkConnectorConfig conn1SinkConfig;
    private SinkConnectorConfig conn1SinkConfigUpdated;
    private short connectProtocolVersion;
    private final ConnectorClientConfigOverridePolicy
        noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();


    @Before
    public void setUp() throws Exception {
        time = new MockTime();
        metrics = new MockConnectMetrics(time);
        worker = PowerMock.createMock(Worker.class);
        EasyMock.expect(worker.isSinkConnector(CONN1)).andStubReturn(Boolean.TRUE);
        AutoCloseable uponShutdown = () -> shutdownCalled.countDown();

        // Default to the old protocol unless specified otherwise
        connectProtocolVersion = CONNECT_PROTOCOL_V0;

        herder = PowerMock.createPartialMock(DistributedHerder.class,
                new String[]{"connectorType", "updateDeletedConnectorStatus", "updateDeletedTaskStatus", "validateConnectorConfig", "buildRestartPlan", "recordRestarting"},
                new DistributedConfig(HERDER_CONFIG), worker, WORKER_ID, KAFKA_CLUSTER_ID,
                statusBackingStore, configBackingStore, member, MEMBER_URL, restClient, metrics, time, noneConnectorClientConfigOverridePolicy,
                new AutoCloseable[]{uponShutdown});

        configUpdateListener = herder.new ConfigUpdateListener();
        rebalanceListener = herder.new RebalanceListener(time);
        plugins = PowerMock.createMock(Plugins.class);
        conn1SinkConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);
        conn1SinkConfigUpdated = new SinkConnectorConfig(plugins, CONN1_CONFIG_UPDATED);
        EasyMock.expect(herder.connectorType(EasyMock.anyObject())).andReturn(ConnectorType.SOURCE).anyTimes();
        PowerMock.expectPrivate(herder, "updateDeletedConnectorStatus").andVoid().anyTimes();
        PowerMock.expectPrivate(herder, "updateDeletedTaskStatus").andVoid().anyTimes();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
        if (herderExecutor != null) {
            herderExecutor.shutdownNow();
            herderExecutor = null;
        }
    }

    @Test
    public void testJoinAssignment() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.asList());

        // and the new assignment started
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
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
    public void testIncrementalCooperativeRebalanceForNewMember() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains revocations from other members. For the new
        // member the assignment should be empty
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // The new member got its assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.asList(TASK1), 0);

        // and the new assignment started
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
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
    public void testIncrementalCooperativeRebalanceForExistingMember() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains revocations because a new member joined.
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                Collections.emptyList(), Collections.emptyList(), 0);
        member.requestRejoin();
        PowerMock.expectLastCall();

        // In the second rebalance the new member gets its assignment and this member has no
        // assignments or revocations
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.configState = SNAPSHOT;
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
    public void testIncrementalCooperativeRebalanceWithDelay() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains some assignments but also a delay, because a
        // member was detected missing
        int rebalanceDelay = 10_000;

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                Collections.emptyList(), Arrays.asList(TASK2),
                rebalanceDelay);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        worker.startSourceTask(EasyMock.eq(TASK2), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall().andAnswer(() -> {
            time.sleep(9900L);
            return null;
        });

        // Request to re-join because the scheduled rebalance delay has been reached
        member.requestRejoin();
        PowerMock.expectLastCall();

        // The member got its assignment and revocation
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.asList(TASK1), 0);

        // and the new assignment started
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);
        herder.tick();

        herder.tick();

        time.sleep(2000L);
        assertStatistics(3, 2, 100, 2000);

        PowerMock.verifyAll();
    }

    @Test
    public void testRebalanceFailedConnector() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.asList());

        // and the new assignment started
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(false, null, null);

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
    public void testRevoke() throws TimeoutException {
        revokeAndReassign(false);
    }

    @Test
    public void testIncompleteRebalanceBeforeRevoke() throws TimeoutException {
        revokeAndReassign(true);
    }

    public void revokeAndReassign(boolean incompleteRebalance) throws TimeoutException {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        int configOffset = 1;

        // Join group and get initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(connectProtocolVersion);
        // The lists need to be mutable because assignments might be removed
        expectRebalance(configOffset, new ArrayList<>(singletonList(CONN1)), new ArrayList<>(singletonList(TASK1)));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
            EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // worker is stable with an existing set of tasks

        if (incompleteRebalance) {
            // Perform a partial re-balance just prior to the revocation
            // bump the configOffset to trigger reading the config topic to the end
            configOffset++;
            expectRebalance(configOffset, Arrays.asList(), Arrays.asList());
            // give it the wrong snapshot, as if we're out of sync/can't reach the broker
            expectConfigRefreshAndSnapshot(SNAPSHOT);
            member.requestRejoin();
            PowerMock.expectLastCall();
            // tick exits early because we failed, and doesn't do the poll at the end of the method
            // the worker did not startWork or reset the rebalanceResolved flag
        }

        // Revoke the connector in the next rebalance
        expectRebalance(Arrays.asList(CONN1), Arrays.asList(),
            ConnectProtocol.Assignment.NO_ERROR, configOffset, Arrays.asList(),
            Arrays.asList());

        if (incompleteRebalance) {
            // Same as SNAPSHOT, except with an updated offset
            // Allow the task to read to the end of the topic and complete the rebalance
            ClusterConfigState secondSnapshot = new ClusterConfigState(
                configOffset, null, Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());
            expectConfigRefreshAndSnapshot(secondSnapshot);
        }
        member.requestRejoin();
        PowerMock.expectLastCall();

        // re-assign the connector back to the same worker to ensure state was cleaned up
        expectRebalance(configOffset, Arrays.asList(CONN1), Arrays.asList());
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        if (incompleteRebalance) {
            herder.tick();
        }
        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testHaltCleansUpWorker() {
        EasyMock.expect(worker.connectorNames()).andReturn(Collections.singleton(CONN1));
        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        Capture<Callback<ConfigInfos>> validateCallback = newCapture();
        herder.validateConnectorConfig(EasyMock.eq(CONN2_CONFIG), capture(validateCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            validateCallback.getValue().onCompletion(null, CONN2_CONFIG_INFOS);
            return null;
        });

        // CONN2 is new, should succeed
        configBackingStore.putConnectorConfig(CONN2, CONN2_CONFIG);
        PowerMock.expectLastCall();
        ConnectorInfo info = new ConnectorInfo(CONN2, CONN2_CONFIG, Collections.emptyList(),
            ConnectorType.SOURCE);
        putConnectorCallback.onCompletion(null, new Herder.Created<>(true, info));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // These will occur just before/during the second tick
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        // First tick runs the initial herder request, which issues an asynchronous request for
        // connector validation
        herder.tick();

        // Once that validation is complete, another request is added to the herder request queue
        // for actually performing the config write; this tick is for that request
        herder.tick();

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorFailedValidation() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        HashMap<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.remove(ConnectorConfig.NAME_CONFIG);

        member.wakeup();
        PowerMock.expectLastCall();

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        Capture<Callback<ConfigInfos>> validateCallback = newCapture();
        herder.validateConnectorConfig(EasyMock.eq(config), capture(validateCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            // CONN2 creation should fail
            validateCallback.getValue().onCompletion(null, CONN2_INVALID_CONFIG_INFOS);
            return null;
        });

        Capture<Throwable> error = newCapture();
        putConnectorCallback.onCompletion(capture(error), EasyMock.isNull());
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the second tick
        member.wakeup();
        PowerMock.expectLastCall();

        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, config, false, putConnectorCallback);
        herder.tick();
        herder.tick();

        assertTrue(error.hasCaptured());
        assertTrue(error.getValue() instanceof BadRequestException);

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorNameConflictsWithWorkerGroupId() {
        Map<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.put(ConnectorConfig.NAME_CONFIG, "test-group");

        SinkConnector connectorMock = PowerMock.createMock(SinkConnector.class);

        PowerMock.replayAll(connectorMock);

        // CONN2 creation should fail because the worker group id (connect-test-group) conflicts with
        // the consumer group id we would use for this sink
        Map<String, ConfigValue> validatedConfigs = herder.validateSinkConnectorConfig(
                connectorMock, SinkConnectorConfig.configDef(), config);

        ConfigValue nameConfig = validatedConfigs.get(ConnectorConfig.NAME_CONFIG);
        assertEquals(
                Collections.singletonList("Consumer group for sink connector named test-group conflicts with Connect worker group connect-test-group"),
                nameConfig.errorMessages());

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidation() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.exactlyOnceSupport(EasyMock.eq(config)))
                .andReturn(ExactlyOnceSupport.SUPPORTED);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(Collections.emptyList(), errors);

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidationOnUnsupportedConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.exactlyOnceSupport(EasyMock.eq(config)))
                .andReturn(ExactlyOnceSupport.UNSUPPORTED);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(
                Collections.singletonList("The connector does not support exactly-once delivery guarantees with the provided configuration."),
                errors);

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidationOnUnknownConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.exactlyOnceSupport(EasyMock.eq(config)))
                .andReturn(null);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains("The connector does not implement the API required for preflight validation of exactly-once source support."));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidationHandlesConnectorErrorsGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        String errorMessage = "time to add a new unit test :)";
        EasyMock.expect(connectorMock.exactlyOnceSupport(EasyMock.eq(config)))
                .andThrow(new NullPointerException(errorMessage));

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains(errorMessage));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidationWhenExactlyOnceNotEnabledOnWorker() {
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.exactlyOnceSupport(EasyMock.eq(config)))
                .andReturn(ExactlyOnceSupport.SUPPORTED);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(
                Collections.singletonList("This worker does not have exactly-once source support enabled."),
                errors);

        PowerMock.verifyAll();
    }

    @Test
    public void testExactlyOnceSourceSupportValidationHandlesInvalidValuesGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, "invalid");

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains("String must be one of (case insensitive): "));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorTransactionBoundaryValidation() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.canDefineTransactionBoundaries(EasyMock.eq(config)))
                .andReturn(ConnectorTransactionBoundaries.SUPPORTED);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertEquals(Collections.emptyList(), errors);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorTransactionBoundaryValidationOnUnsupportedConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        EasyMock.expect(connectorMock.canDefineTransactionBoundaries(EasyMock.eq(config)))
                .andReturn(ConnectorTransactionBoundaries.UNSUPPORTED);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains("The connector does not support connector-defined transaction boundaries with the given configuration."));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorTransactionBoundaryValidationHandlesConnectorErrorsGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);
        String errorMessage = "Wait I thought we tested for this?";
        EasyMock.expect(connectorMock.canDefineTransactionBoundaries(EasyMock.eq(config)))
                .andThrow(new ConnectException(errorMessage));

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains(errorMessage));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorTransactionBoundaryValidationHandlesInvalidValuesGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, "CONNECTOR.toString()");

        SourceConnector connectorMock = PowerMock.createMock(SourceConnector.class);

        PowerMock.replayAll(connectorMock);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
                "Error message did not contain expected text: " + errors.get(0),
                errors.get(0).contains("String must be one of (case insensitive): "));
        assertEquals(1, errors.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        Capture<Callback<ConfigInfos>> validateCallback = newCapture();
        herder.validateConnectorConfig(EasyMock.eq(CONN1_CONFIG), capture(validateCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            validateCallback.getValue().onCompletion(null, CONN1_CONFIG_INFOS);
            return null;
        });

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        member.wakeup();
        PowerMock.expectLastCall();
        // CONN1 already exists
        putConnectorCallback.onCompletion(EasyMock.<AlreadyExistsException>anyObject(), EasyMock.isNull());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the second tick
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN1, CONN1_CONFIG, false, putConnectorCallback);
        herder.tick();
        herder.tick();

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        // Start with one connector
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        // And delete the connector
        configBackingStore.removeConnectorConfig(CONN1);
        PowerMock.expectLastCall();
        putConnectorCallback.onCompletion(null, new Herder.Created<>(false, null));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // The change eventually is reflected to the config topic and the deleted connector and
        // tasks are revoked
        member.wakeup();
        PowerMock.expectLastCall();
        TopicStatus fooStatus = new TopicStatus(FOO_TOPIC, CONN1, 0, time.milliseconds());
        TopicStatus barStatus = new TopicStatus(BAR_TOPIC, CONN1, 0, time.milliseconds());
        EasyMock.expect(statusBackingStore.getAllTopics(EasyMock.eq(CONN1))).andReturn(new HashSet<>(Arrays.asList(fooStatus, barStatus))).times(2);
        statusBackingStore.deleteTopic(EasyMock.eq(CONN1), EasyMock.eq(FOO_TOPIC));
        PowerMock.expectLastCall().times(2);
        statusBackingStore.deleteTopic(EasyMock.eq(CONN1), EasyMock.eq(BAR_TOPIC));
        PowerMock.expectLastCall().times(2);
        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1),
                ConnectProtocol.Assignment.NO_ERROR, 2, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);
        expectConfigRefreshAndSnapshot(ClusterConfigState.EMPTY);
        member.requestRejoin();
        PowerMock.expectLastCall();
        PowerMock.replayAll();

        herder.deleteConnectorConfig(CONN1, putConnectorCallback);
        herder.tick();

        time.sleep(1000L);
        assertStatistics("leaderUrl", false, 3, 1, 100, 1000L);

        configUpdateListener.onConnectorConfigRemove(CONN1); // read updated config that removes the connector
        herder.configState = ClusterConfigState.EMPTY;
        herder.tick();
        time.sleep(1000L);
        assertStatistics("leaderUrl", true, 3, 1, 100, 2100L);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnector() throws Exception {

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
            fail("Expected NotFoundException to be raised");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorRedirectToLeader() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
    public void testRestartConnectorAndTasksUnknownConnector() throws Exception {
        String connectorName = "UnknownConnector";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(ee.getCause() instanceof NotFoundException);
        assertTrue(ee.getMessage().contains("Unknown connector:"));

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksNotLeader() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(ee.getCause() instanceof NotLeaderException);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksUnknownStatus() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.empty()).anyTimes();

        configBackingStore.putRestartRequest(restartRequest);
        PowerMock.expectLastCall();

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(ee.getCause() instanceof NotFoundException);
        assertTrue(ee.getMessage().contains("Status for connector"));
        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksSuccess() throws Exception {
        RestartPlan restartPlan = PowerMock.createMock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = PowerMock.createMock(ConnectorStateInfo.class);
        EasyMock.expect(restartPlan.restartConnectorStateInfo()).andReturn(connectorStateInfo).anyTimes();

        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.of(restartPlan)).anyTimes();

        configBackingStore.putRestartRequest(restartRequest);
        PowerMock.expectLastCall();

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        assertEquals(connectorStateInfo,  callback.get(1000L, TimeUnit.MILLISECONDS));
        PowerMock.verifyAll();
    }

    @Test
    public void testDoRestartConnectorAndTasksEmptyPlan() {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.empty()).anyTimes();

        PowerMock.replayAll();

        herder.doRestartConnectorAndTasks(restartRequest);
        PowerMock.verifyAll();
    }

    @Test
    public void testDoRestartConnectorAndTasksNoAssignments() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = PowerMock.createMock(RestartPlan.class);
        EasyMock.expect(restartPlan.shouldRestartConnector()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.shouldRestartTasks()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.taskIdsToRestart()).andReturn(Collections.singletonList(taskId)).anyTimes();

        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.of(restartPlan)).anyTimes();

        PowerMock.replayAll();
        herder.assignment = ExtendedAssignment.empty();
        herder.doRestartConnectorAndTasks(restartRequest);
        PowerMock.verifyAll();
    }

    @Test
    public void testDoRestartConnectorAndTasksOnlyConnector() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = PowerMock.createMock(RestartPlan.class);
        EasyMock.expect(restartPlan.shouldRestartConnector()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.shouldRestartTasks()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.taskIdsToRestart()).andReturn(Collections.singletonList(taskId)).anyTimes();

        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.of(restartPlan)).anyTimes();

        herder.assignment = PowerMock.createMock(ExtendedAssignment.class);
        EasyMock.expect(herder.assignment.connectors()).andReturn(Collections.singletonList(CONN1)).anyTimes();
        EasyMock.expect(herder.assignment.tasks()).andReturn(Collections.emptyList()).anyTimes();

        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>>  stateCallback = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.anyObject(TargetState.class), capture(stateCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            stateCallback.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();


        herder.onRestart(CONN1);
        EasyMock.expectLastCall();

        PowerMock.replayAll();
        herder.doRestartConnectorAndTasks(restartRequest);
        PowerMock.verifyAll();
    }

    @Test
    public void testDoRestartConnectorAndTasksOnlyTasks() {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = PowerMock.createMock(RestartPlan.class);
        EasyMock.expect(restartPlan.shouldRestartConnector()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.shouldRestartTasks()).andReturn(true).anyTimes();
        // The connector has three tasks
        EasyMock.expect(restartPlan.taskIdsToRestart()).andReturn(Arrays.asList(TASK0, TASK1, TASK2)).anyTimes();
        EasyMock.expect(restartPlan.restartTaskCount()).andReturn(3).anyTimes();
        EasyMock.expect(restartPlan.totalTaskCount()).andReturn(3).anyTimes();
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.of(restartPlan)).anyTimes();

        herder.assignment = PowerMock.createMock(ExtendedAssignment.class);
        EasyMock.expect(herder.assignment.connectors()).andReturn(Collections.emptyList()).anyTimes();
        // But only one task is assigned to this worker
        EasyMock.expect(herder.assignment.tasks()).andReturn(Collections.singletonList(TASK0)).anyTimes();

        herder.configState = SNAPSHOT;

        worker.stopAndAwaitTasks(Collections.singletonList(TASK0));
        PowerMock.expectLastCall();

        herder.onRestart(TASK0);
        EasyMock.expectLastCall();

        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.anyObject(TargetState.class));
        PowerMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();
        herder.doRestartConnectorAndTasks(restartRequest);
        PowerMock.verifyAll();
    }

    @Test
    public void testDoRestartConnectorAndTasksBoth() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = PowerMock.createMock(RestartPlan.class);
        EasyMock.expect(restartPlan.shouldRestartConnector()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.shouldRestartTasks()).andReturn(true).anyTimes();
        EasyMock.expect(restartPlan.taskIdsToRestart()).andReturn(Collections.singletonList(taskId)).anyTimes();
        EasyMock.expect(restartPlan.restartTaskCount()).andReturn(1).anyTimes();
        EasyMock.expect(restartPlan.totalTaskCount()).andReturn(1).anyTimes();
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andReturn(Optional.of(restartPlan)).anyTimes();

        herder.assignment = PowerMock.createMock(ExtendedAssignment.class);
        EasyMock.expect(herder.assignment.connectors()).andReturn(Collections.singletonList(CONN1)).anyTimes();
        EasyMock.expect(herder.assignment.tasks()).andReturn(Collections.singletonList(taskId)).anyTimes();

        herder.configState = SNAPSHOT;

        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>>  stateCallback = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.anyObject(TargetState.class), capture(stateCallback));


        herder.onRestart(CONN1);
        EasyMock.expectLastCall();

        worker.stopAndAwaitTasks(Collections.singletonList(taskId));
        PowerMock.expectLastCall();

        herder.onRestart(taskId);
        EasyMock.expectLastCall();

        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.anyObject(TargetState.class));
        PowerMock.expectLastCall().andReturn(true);

        PowerMock.replayAll();
        herder.doRestartConnectorAndTasks(restartRequest);
        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTask() throws Exception {
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, conn1SinkConfig)).andStubReturn(TASK_CONFIGS);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
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
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
    public void testRequestProcessingOrder() {
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);

        // join, no configs so no need to catch up on config topic
        expectRebalance(-1, Collections.emptyList(), Collections.emptyList());
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        // Checks for config updates and starts rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Arrays.asList(CONN1), Collections.emptyList());
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT); // for this test, it doesn't matter if we use the same config snapshot
        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the third tick
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorConfigUpdateFailedTransformation() throws Exception {
        // Connector config can be applied without any rebalance

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        WorkerConfigTransformer configTransformer = EasyMock.mock(WorkerConfigTransformer.class);
        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
            EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        // During the next tick, throw an error from the transformer
        ClusterConfigState snapshotWithTransform = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet(), configTransformer);
        EasyMock.expect(configBackingStore.snapshot()).andReturn(snapshotWithTransform);
        EasyMock.expect(configTransformer.transform(EasyMock.eq(CONN1), EasyMock.anyObject()))
            .andThrow(new ConfigException("Simulated exception thrown during config transformation"));
        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
        Capture<ConnectorStatus> failedStatus = newCapture();
        statusBackingStore.putSafe(capture(failedStatus));
        PowerMock.expectLastCall();
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the third tick
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        EasyMock.replay(configTransformer);
        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick();

        PowerMock.verifyAll();

        assertEquals(CONN1, failedStatus.getValue().id());
        assertEquals(FAILED, failedStatus.getValue().state());
    }

    @Test
    public void testConnectorPaused() throws Exception {
        // ensure that target state changes are propagated to the worker

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_PAUSED_CONN1);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>> onPause = newCapture();
        worker.setTargetState(EasyMock.eq(CONN1), EasyMock.eq(TargetState.PAUSED), capture(onPause));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.PAUSED);
            return null;
        });

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the third tick
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // worker should apply the state change
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorResumed() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // start with the connector paused
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT_PAUSED_CONN1);
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.PAUSED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.PAUSED);
            return true;
        });
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>> onResume = newCapture();
        worker.setTargetState(EasyMock.eq(CONN1), EasyMock.eq(TargetState.STARTED), capture(onResume));
        PowerMock.expectLastCall().andAnswer(() -> {
            onResume.getValue().onCompletion(null, TargetState.STARTED);
            return null;
        });
        member.wakeup();
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // we expect reconfiguration after resuming
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        // These will occur just before/during the third tick
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to started
        herder.tick(); // apply state change
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testUnknownConnectorPaused() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // state change is ignored since we have no target state
        member.wakeup();
        PowerMock.expectLastCall();
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.emptySet());

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_PAUSED_CONN1);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>> onPause = newCapture();
        worker.setTargetState(EasyMock.eq(CONN1), EasyMock.eq(TargetState.PAUSED), capture(onPause));
        PowerMock.expectLastCall().andAnswer(() -> {
            onPause.getValue().onCompletion(null, TargetState.PAUSED);
            return null;
        });

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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.emptySet());

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT_PAUSED_CONN1);
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.PAUSED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // handle the state change
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        PowerMock.expectLastCall();

        Capture<Callback<TargetState>> onStart = newCapture();
        worker.setTargetState(EasyMock.eq(CONN1), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return null;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(false, null, null);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // These will occur just before/during the third tick
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // apply state change
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskConfigAdded() {
        // Task config always requires rebalance
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);

        // join
        expectRebalance(-1, Collections.emptyList(), Collections.emptyList());
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        // Checks for config updates and starts rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Collections.emptyList(),
                Arrays.asList(TASK0));
        worker.startSourceTask(EasyMock.eq(TASK0), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
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
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl", Collections.emptyList(),
                Collections.emptyList(), 0, true);
        // Reading to end of log times out
        configBackingStore.refresh(anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall().andThrow(new TimeoutException());
        member.maybeLeaveGroup(EasyMock.eq("taking too long to read the log"));
        EasyMock.expectLastCall();
        member.requestRejoin();

        // After backoff, restart the process and this time succeed
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // one more tick, to make sure we don't keep trying to read to the config topic unnecessarily
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        long before = time.milliseconds();
        int workerUnsyncBackoffMs = DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT;
        int coordinatorDiscoveryTimeoutMs = 100;
        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());

        time.sleep(1000L);
        assertStatistics("leaderUrl", true, 3, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

        before = time.milliseconds();
        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs, time.milliseconds());
        time.sleep(2000L);
        assertStatistics("leaderUrl", false, 3, 1, 100, 2000L);

        // tick once more to ensure that the successful read to the end of the config topic was 
        // tracked and no further unnecessary attempts were made
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testJoinLeaderCatchUpRetriesForIncrementalCooperative() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;

        // Join group and as leader fail to do assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // The leader got its assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", Arrays.asList(CONN1), Arrays.asList(TASK1), 0, true);

        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Another rebalance is triggered but this time it fails to read to the max offset and
        // triggers a re-sync
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);

        // The leader will retry a few times to read to the end of the config log
        int retries = 2;
        member.requestRejoin();
        for (int i = retries; i >= 0; --i) {
            // Reading to end of log times out
            configBackingStore.refresh(anyLong(), EasyMock.anyObject(TimeUnit.class));
            EasyMock.expectLastCall().andThrow(new TimeoutException());
            member.maybeLeaveGroup(EasyMock.eq("taking too long to read the log"));
            EasyMock.expectLastCall();
        }

        // After a few retries succeed to read the log to the end
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", Arrays.asList(CONN1), Arrays.asList(TASK1), 0, true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);
        herder.tick();

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);
        herder.tick();

        long before;
        int coordinatorDiscoveryTimeoutMs = 100;
        int maxRetries = 5;
        for (int i = maxRetries; i >= maxRetries - retries; --i) {
            before = time.milliseconds();
            int workerUnsyncBackoffMs =
                    DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT / 10 / i;
            herder.tick();
            assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());
            coordinatorDiscoveryTimeoutMs = 0;
        }

        before = time.milliseconds();
        coordinatorDiscoveryTimeoutMs = 100;
        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs, time.milliseconds());

        PowerMock.verifyAll();
    }

    @Test
    public void testJoinLeaderCatchUpFailsForIncrementalCooperative() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;

        // Join group and as leader fail to do assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // The leader got its assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                "leader", "leaderUrl", Arrays.asList(CONN1), Arrays.asList(TASK1), 0, true);

        // and the new assignment started
        Capture<Callback<TargetState>> onStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        worker.startSourceTask(EasyMock.eq(TASK1), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall().andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Another rebalance is triggered but this time it fails to read to the max offset and
        // triggers a re-sync
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);

        // The leader will exhaust the retries while trying to read to the end of the config log
        int maxRetries = 5;
        member.requestRejoin();
        for (int i = maxRetries; i >= 0; --i) {
            // Reading to end of log times out
            configBackingStore.refresh(anyLong(), EasyMock.anyObject(TimeUnit.class));
            EasyMock.expectLastCall().andThrow(new TimeoutException());
            member.maybeLeaveGroup(EasyMock.eq("taking too long to read the log"));
            EasyMock.expectLastCall();
        }

        Capture<ExtendedAssignment> assignmentCapture = newCapture();
        member.revokeAssignment(capture(assignmentCapture));
        PowerMock.expectLastCall();

        // After a complete backoff and a revocation of running tasks rejoin and this time succeed
        // The worker gets back the assignment that had given up
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", Arrays.asList(CONN1), Arrays.asList(TASK1),
                0, true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);
        herder.tick();

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);
        herder.tick();

        long before;
        int coordinatorDiscoveryTimeoutMs = 100;
        for (int i = maxRetries; i > 0; --i) {
            before = time.milliseconds();
            int workerUnsyncBackoffMs =
                    DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT / 10 / i;
            herder.tick();
            assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());
            coordinatorDiscoveryTimeoutMs = 0;
        }

        before = time.milliseconds();
        herder.tick();
        assertEquals(before, time.milliseconds());
        assertEquals(Collections.singleton(CONN1), assignmentCapture.getValue().connectors());
        assertEquals(Collections.singleton(TASK1), assignmentCapture.getValue().tasks());
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testAccessors() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        EasyMock.expect(worker.getPlugins()).andReturn(plugins).anyTimes();
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT).times(2);

        WorkerConfigTransformer configTransformer = EasyMock.mock(WorkerConfigTransformer.class);
        EasyMock.expect(configTransformer.transform(EasyMock.eq(CONN1), EasyMock.anyObject()))
            .andThrow(new AssertionError("Config transformation should not occur when requesting connector or task info"));
        EasyMock.replay(configTransformer);
        ClusterConfigState snapshotWithTransform = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet(), configTransformer);

        expectConfigRefreshAndSnapshot(snapshotWithTransform);


        member.wakeup();
        PowerMock.expectLastCall().anyTimes();
        // list connectors, get connector info, get connector config, get task configs
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();


        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
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
        // connectorConfig uses an async request
        member.wakeup();
        PowerMock.expectLastCall();

        // putConnectorConfig uses an async request
        member.wakeup();
        PowerMock.expectLastCall();

        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Arrays.asList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Poll loop for second round of calls
        member.ensureActive();
        PowerMock.expectLastCall();

        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT);

        Capture<Callback<ConfigInfos>> validateCallback = newCapture();
        herder.validateConnectorConfig(EasyMock.eq(CONN1_CONFIG_UPDATED), capture(validateCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            validateCallback.getValue().onCompletion(null, CONN1_CONFIG_INFOS);
            return null;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        configBackingStore.putConnectorConfig(CONN1, CONN1_CONFIG_UPDATED);
        PowerMock.expectLastCall().andAnswer(() -> {
            // Simulate response to writing config + waiting until end of log to be read
            configUpdateListener.onConnectorConfigUpdate(CONN1);
            return null;
        });
        // As a result of reconfig, should need to update snapshot. With only connector updates, we'll just restart
        // connector without rebalance
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_UPDATED_CONN1_CONFIG).times(2);
        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V0);
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectExecuteTaskReconfiguration(true, conn1SinkConfigUpdated, () -> TASK_CONFIGS);

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
    public void testKeyRotationWhenWorkerBecomesLeader() throws Exception {
        long rotationTtlDelay = DistributedConfig.INTER_WORKER_KEY_TTL_MS_MS_DEFAULT;
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        // First rebalance: poll indefinitely as no key has been read yet, so expiration doesn't come into play
        member.poll(Long.MAX_VALUE);
        EasyMock.expectLastCall();

        expectRebalance(2, Collections.emptyList(), Collections.emptyList());
        SessionKey initialKey = new SessionKey(EasyMock.mock(SecretKey.class), 0);
        ClusterConfigState snapshotWithKey =  new ClusterConfigState(2, initialKey, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());
        expectConfigRefreshAndSnapshot(snapshotWithKey);
        // Second rebalance: poll indefinitely as worker is follower, so expiration still doesn't come into play
        member.poll(Long.MAX_VALUE);
        EasyMock.expectLastCall();

        expectRebalance(2, Collections.emptyList(), Collections.emptyList(), "member", MEMBER_URL, true);
        Capture<SessionKey> updatedKey = EasyMock.newCapture();
        configBackingStore.putSessionKey(EasyMock.capture(updatedKey));
        EasyMock.expectLastCall().andAnswer(() -> {
            configUpdateListener.onSessionKeyUpdate(updatedKey.getValue());
            return null;
        });
        // Third rebalance: poll for a limited time as worker has become leader and must wake up for key expiration
        member.poll(leq(rotationTtlDelay));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        configUpdateListener.onSessionKeyUpdate(initialKey);
        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testKeyRotationDisabledWhenWorkerBecomesFollower() throws Exception {
        long rotationTtlDelay = DistributedConfig.INTER_WORKER_KEY_TTL_MS_MS_DEFAULT;
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), "member", MEMBER_URL, true);
        SecretKey initialSecretKey = EasyMock.mock(SecretKey.class);
        EasyMock.expect(initialSecretKey.getAlgorithm()).andReturn(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT).anyTimes();
        EasyMock.expect(initialSecretKey.getEncoded()).andReturn(new byte[32]).anyTimes();
        SessionKey initialKey = new SessionKey(initialSecretKey, time.milliseconds());
        ClusterConfigState snapshotWithKey =  new ClusterConfigState(1, initialKey, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());
        expectConfigRefreshAndSnapshot(snapshotWithKey);
        // First rebalance: poll for a limited time as worker is leader and must wake up for key expiration
        member.poll(leq(rotationTtlDelay));
        EasyMock.expectLastCall();

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        // Second rebalance: poll indefinitely as worker is no longer leader, so key expiration doesn't come into play
        member.poll(Long.MAX_VALUE);
        EasyMock.expectLastCall();

        PowerMock.replayAll(initialSecretKey);

        configUpdateListener.onSessionKeyUpdate(initialKey);
        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigsSignatureNotRequiredV0() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);

        member.wakeup();
        EasyMock.expectLastCall().once();
        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V0).anyTimes();
        PowerMock.replayAll(taskConfigCb);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        PowerMock.verifyAll();
    }
    @Test
    public void testPutTaskConfigsSignatureNotRequiredV1() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);

        member.wakeup();
        EasyMock.expectLastCall().once();
        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V1).anyTimes();
        PowerMock.replayAll(taskConfigCb);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigsMissingRequiredSignature() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);
        Capture<Throwable> errorCapture = Capture.newInstance();
        taskConfigCb.onCompletion(capture(errorCapture), EasyMock.eq(null));
        EasyMock.expectLastCall().once();

        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();
        PowerMock.replayAll(taskConfigCb);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        PowerMock.verifyAll();
        assertTrue(errorCapture.getValue() instanceof BadRequestException);
    }

    @Test
    public void testPutTaskConfigsDisallowedSignatureAlgorithm() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);
        Capture<Throwable> errorCapture = Capture.newInstance();
        taskConfigCb.onCompletion(capture(errorCapture), EasyMock.eq(null));
        EasyMock.expectLastCall().once();

        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();

        InternalRequestSignature signature = EasyMock.mock(InternalRequestSignature.class);
        EasyMock.expect(signature.keyAlgorithm()).andReturn("HmacSHA489").anyTimes();

        PowerMock.replayAll(taskConfigCb, signature);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        PowerMock.verifyAll();
        assertTrue(errorCapture.getValue() instanceof BadRequestException);
    }

    @Test
    public void testPutTaskConfigsInvalidSignature() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);
        Capture<Throwable> errorCapture = Capture.newInstance();
        taskConfigCb.onCompletion(capture(errorCapture), EasyMock.eq(null));
        EasyMock.expectLastCall().once();

        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();

        InternalRequestSignature signature = EasyMock.mock(InternalRequestSignature.class);
        EasyMock.expect(signature.keyAlgorithm()).andReturn("HmacSHA256").anyTimes();
        EasyMock.expect(signature.isValid(EasyMock.anyObject())).andReturn(false).anyTimes();

        SessionKey sessionKey = EasyMock.mock(SessionKey.class);
        SecretKey secretKey = EasyMock.niceMock(SecretKey.class);
        EasyMock.expect(sessionKey.key()).andReturn(secretKey);
        EasyMock.expect(sessionKey.creationTimestamp()).andReturn(time.milliseconds());

        PowerMock.replayAll(taskConfigCb, signature, sessionKey, secretKey);

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        PowerMock.verifyAll();
        assertTrue(errorCapture.getValue() instanceof ConnectRestException);
        assertEquals(FORBIDDEN.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());
    }

    @Test
    public void putTaskConfigsWorkerStillStarting() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);
        Capture<Throwable> errorCapture = Capture.newInstance();
        taskConfigCb.onCompletion(capture(errorCapture), EasyMock.eq(null));
        EasyMock.expectLastCall().once();

        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();

        InternalRequestSignature signature = EasyMock.mock(InternalRequestSignature.class);
        EasyMock.expect(signature.keyAlgorithm()).andReturn("HmacSHA256").anyTimes();
        EasyMock.expect(signature.isValid(EasyMock.anyObject())).andReturn(true).anyTimes();

        PowerMock.replayAll(taskConfigCb, signature);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        PowerMock.verifyAll();
        assertTrue(errorCapture.getValue() instanceof ConnectRestException);
        assertEquals(SERVICE_UNAVAILABLE.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());
    }

    @Test
    public void testPutTaskConfigsValidRequiredSignature() {
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);

        member.wakeup();
        EasyMock.expectLastCall().once();
        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();

        InternalRequestSignature signature = EasyMock.mock(InternalRequestSignature.class);
        EasyMock.expect(signature.keyAlgorithm()).andReturn("HmacSHA256").anyTimes();
        EasyMock.expect(signature.isValid(EasyMock.anyObject())).andReturn(true).anyTimes();

        SessionKey sessionKey = EasyMock.mock(SessionKey.class);
        SecretKey secretKey = EasyMock.niceMock(SecretKey.class);
        EasyMock.expect(sessionKey.key()).andReturn(secretKey);
        EasyMock.expect(sessionKey.creationTimestamp()).andReturn(time.milliseconds());

        PowerMock.replayAll(taskConfigCb, signature, sessionKey, secretKey);

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailedToWriteSessionKey() throws Exception {
        // First tick -- after joining the group, we try to write a new
        // session key to the config topic, and fail
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        configBackingStore.putSessionKey(anyObject(SessionKey.class));
        EasyMock.expectLastCall().andThrow(new ConnectException("Oh no!"));

        // Second tick -- we read to the end of the config topic first,
        // then ensure we're still active in the group
        // then try a second time to write a new session key,
        // then finally begin polling for group activity
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        member.ensureActive();
        PowerMock.expectLastCall();
        configBackingStore.putSessionKey(anyObject(SessionKey.class));
        EasyMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testFailedToReadBackNewlyWrittenSessionKey() throws Exception {
        SecretKey secretKey = EasyMock.niceMock(SecretKey.class);
        EasyMock.expect(secretKey.getAlgorithm()).andReturn(INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
        EasyMock.expect(secretKey.getEncoded()).andReturn(new byte[32]);
        SessionKey sessionKey = new SessionKey(secretKey, time.milliseconds());
        ClusterConfigState snapshotWithSessionKey = new ClusterConfigState(1, sessionKey, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

        // First tick -- after joining the group, we try to write a new session key to
        // the config topic, and fail (in this case, we're trying to simulate that we've
        // actually written the key successfully, but haven't been able to read it back
        // from the config topic, so to the herder it looks the same as if it'd just failed
        // to write the key)
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        configBackingStore.putSessionKey(anyObject(SessionKey.class));
        EasyMock.expectLastCall().andThrow(new ConnectException("Oh no!"));

        // Second tick -- we read to the end of the config topic first, and pick up
        // the session key that we were able to write the last time,
        // then ensure we're still active in the group
        // then finally begin polling for group activity
        // Importantly, we do not try to write a new session key this time around
        configBackingStore.refresh(anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            configUpdateListener.onSessionKeyUpdate(sessionKey);
            return null;
        });
        EasyMock.expect(configBackingStore.snapshot()).andReturn(snapshotWithSessionKey);
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll(secretKey);

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testFenceZombiesInvalidSignature() {
        // Don't have to run the whole gamut of scenarios (invalid signature, missing signature, earlier protocol that doesn't require signatures)
        // since the task config tests cover that pretty well. One sanity check to ensure that this method is guarded should be sufficient.
        Callback<Void> taskConfigCb = EasyMock.mock(Callback.class);
        Capture<Throwable> errorCapture = Capture.newInstance();
        taskConfigCb.onCompletion(capture(errorCapture), EasyMock.eq(null));
        EasyMock.expectLastCall().once();

        EasyMock.expect(member.currentProtocolVersion()).andReturn(CONNECT_PROTOCOL_V2).anyTimes();

        InternalRequestSignature signature = EasyMock.mock(InternalRequestSignature.class);
        EasyMock.expect(signature.keyAlgorithm()).andReturn("HmacSHA256").anyTimes();
        EasyMock.expect(signature.isValid(EasyMock.anyObject())).andReturn(false).anyTimes();

        SessionKey sessionKey = EasyMock.mock(SessionKey.class);
        SecretKey secretKey = EasyMock.niceMock(SecretKey.class);
        EasyMock.expect(sessionKey.key()).andReturn(secretKey);
        EasyMock.expect(sessionKey.creationTimestamp()).andReturn(time.milliseconds());

        PowerMock.replayAll(taskConfigCb, signature, sessionKey, secretKey);

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        herder.fenceZombieSourceTasks(CONN1, taskConfigCb, signature);

        PowerMock.verifyAll();
        assertTrue(errorCapture.getValue() instanceof ConnectRestException);
        assertEquals(FORBIDDEN.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());
    }

    @Test
    public void testTaskRequestedZombieFencingForwardedToLeader() throws Exception {
        testTaskRequestedZombieFencingForwardingToLeader(true);
    }

    @Test
    public void testTaskRequestedZombieFencingFailedForwardToLeader() throws Exception {
        testTaskRequestedZombieFencingForwardingToLeader(false);
    }

    private void testTaskRequestedZombieFencingForwardingToLeader(boolean succeed) throws Exception {
        expectHerderStartup();
        ExecutorService forwardRequestExecutor = EasyMock.mock(ExecutorService.class);
        herder.forwardRequestExecutor = forwardRequestExecutor;

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());

        expectAnyTicks();

        member.wakeup();
        EasyMock.expectLastCall();

        org.easymock.IExpectationSetters<RestClient.HttpResponse<Object>> expectRequest = EasyMock.expect(
                restClient.httpRequest(
                        anyObject(), EasyMock.eq("PUT"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), anyObject(), anyObject()
                ));
        if (succeed) {
            expectRequest.andReturn(null);
        } else {
            expectRequest.andThrow(new ConnectRestException(409, "Rebalance :("));
        }

        Capture<Runnable> forwardRequest = EasyMock.newCapture();
        forwardRequestExecutor.execute(EasyMock.capture(forwardRequest));
        EasyMock.expectLastCall().andAnswer(() -> {
            forwardRequest.getValue().run();
            return null;
        });

        expectHerderShutdown(true);
        forwardRequestExecutor.shutdown();
        EasyMock.expectLastCall();
        EasyMock.expect(forwardRequestExecutor.awaitTermination(anyLong(), anyObject())).andReturn(true);

        PowerMock.replayAll(forwardRequestExecutor);


        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(TASK1, fencing);

        if (!succeed) {
            ExecutionException fencingException =
                    assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
            assertTrue(fencingException.getCause() instanceof ConnectException);
        } else {
            fencing.get(10, TimeUnit.SECONDS);
        }

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    @Test
    public void testExternalZombieFencingRequestForAlreadyFencedConnector() throws Exception {
        ClusterConfigState configState = exactlyOnceSnapshot(
                expectNewSessionKey(),
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 12),
                Collections.singletonMap(CONN1, 5),
                Collections.emptySet()
        );
        testExternalZombieFencingRequestThatRequiresNoPhysicalFencing(configState, false);
    }

    @Test
    public void testExternalZombieFencingRequestForSingleTaskConnector() throws Exception {
        ClusterConfigState configState = exactlyOnceSnapshot(
                expectNewSessionKey(),
                Collections.singletonMap(TASK1, TASK_CONFIG),
                Collections.singletonMap(CONN1, 1),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        testExternalZombieFencingRequestThatRequiresNoPhysicalFencing(configState, true);
    }

    @Test
    public void testExternalZombieFencingRequestForFreshConnector() throws Exception {
        ClusterConfigState configState = exactlyOnceSnapshot(
                expectNewSessionKey(),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        testExternalZombieFencingRequestThatRequiresNoPhysicalFencing(configState, true);
    }

    private void testExternalZombieFencingRequestThatRequiresNoPhysicalFencing(
            ClusterConfigState configState, boolean expectTaskCountRecord
    ) throws Exception {
        expectHerderStartup();

        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);

        expectAnyTicks();

        member.wakeup();
        EasyMock.expectLastCall().anyTimes();

        expectConfigRefreshAndSnapshot(configState);

        if (expectTaskCountRecord) {
            configBackingStore.putTaskCountRecord(CONN1, 1);
            EasyMock.expectLastCall();
        }

        expectHerderShutdown(false);

        PowerMock.replayAll();


        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        fencing.get(10, TimeUnit.SECONDS);

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    /**
     * Tests zombie fencing that completes extremely quickly, and causes all callback-related logic to be invoked
     * effectively as soon as it's put into place. This is not likely to occur in practice, but the test is valuable all the
     * same especially since it may shed light on potential deadlocks when the unlikely-but-not-impossible happens.
     */
    @Test
    public void testExternalZombieFencingRequestImmediateCompletion() throws Exception {
        expectHerderStartup();
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        expectAnyTicks();

        member.wakeup();
        EasyMock.expectLastCall();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        // The future returned by Worker::fenceZombies
        KafkaFuture<Void> workerFencingFuture = EasyMock.mock(KafkaFuture.class);
        // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic) 
        KafkaFuture<Void> herderFencingFuture = EasyMock.mock(KafkaFuture.class);

        // Immediately invoke callbacks that the herder sets up for when the worker fencing and writes to the config topic have completed
        for (int i = 0; i < 2; i++) {
            Capture<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = EasyMock.newCapture();
            EasyMock.expect(herderFencingFuture.whenComplete(EasyMock.capture(herderFencingCallback))).andAnswer(() -> {
                herderFencingCallback.getValue().accept(null, null);
                return null;
            });
        }

        Capture<KafkaFuture.BaseFunction<Void, Void>> fencingFollowup = EasyMock.newCapture();
        EasyMock.expect(workerFencingFuture.thenApply(EasyMock.capture(fencingFollowup))).andAnswer(() -> {
            fencingFollowup.getValue().apply(null);
            return herderFencingFuture;
        });
        EasyMock.expect(worker.fenceZombies(EasyMock.eq(CONN1), EasyMock.eq(2), EasyMock.eq(CONN1_CONFIG)))
                .andReturn(workerFencingFuture);

        expectConfigRefreshAndSnapshot(configState);

        configBackingStore.putTaskCountRecord(CONN1, 1);
        EasyMock.expectLastCall();

        expectHerderShutdown(true);

        PowerMock.replayAll(workerFencingFuture, herderFencingFuture);


        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        fencing.get(10, TimeUnit.SECONDS);

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    /**
     * The herder tries to perform a round of fencing, but fails synchronously while invoking Worker::fenceZombies
     */
    @Test
    public void testExternalZombieFencingRequestSynchronousFailure() throws Exception {
        expectHerderStartup();
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        expectAnyTicks();

        member.wakeup();
        EasyMock.expectLastCall();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        Exception fencingException = new KafkaException("whoops!");
        EasyMock.expect(worker.fenceZombies(EasyMock.eq(CONN1), EasyMock.eq(2), EasyMock.eq(CONN1_CONFIG)))
                .andThrow(fencingException);

        expectHerderShutdown(true);

        PowerMock.replayAll();


        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
        assertEquals(fencingException, exception.getCause());

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    /**
     * The herder tries to perform a round of fencing and is able to retrieve a future from worker::fenceZombies, but the attempt
     * fails at a later point.
     */
    @Test
    public void testExternalZombieFencingRequestAsynchronousFailure() throws Exception {
        expectHerderStartup();
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        expectAnyTicks();

        member.wakeup();
        EasyMock.expectLastCall();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        // The future returned by Worker::fenceZombies
        KafkaFuture<Void> workerFencingFuture = EasyMock.mock(KafkaFuture.class);
        // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic) 
        KafkaFuture<Void> herderFencingFuture = EasyMock.mock(KafkaFuture.class);
        // The callbacks that the herder has accrued for outstanding fencing futures
        Capture<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallbacks = EasyMock.newCapture(CaptureType.ALL);

        EasyMock.expect(worker.fenceZombies(EasyMock.eq(CONN1), EasyMock.eq(2), EasyMock.eq(CONN1_CONFIG)))
                .andReturn(workerFencingFuture);

        EasyMock.expect(workerFencingFuture.thenApply(EasyMock.<KafkaFuture.BaseFunction<Void, Void>>anyObject()))
                .andReturn(herderFencingFuture);

        CountDownLatch callbacksInstalled = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            EasyMock.expect(herderFencingFuture.whenComplete(EasyMock.capture(herderFencingCallbacks))).andAnswer(() -> {
                callbacksInstalled.countDown();
                return null;
            });
        }

        expectHerderShutdown(true);

        PowerMock.replayAll(workerFencingFuture, herderFencingFuture);


        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        assertTrue(callbacksInstalled.await(10, TimeUnit.SECONDS));

        Exception fencingException = new AuthorizationException("you didn't say the magic word");
        herderFencingCallbacks.getValues().forEach(cb -> cb.accept(null, fencingException));

        ExecutionException exception = assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
        assertTrue(exception.getCause() instanceof ConnectException);

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    /**
     * Issues multiple rapid fencing requests for a handful of connectors, each of which takes a little while to complete.
     * This mimics what might happen when a few connectors are reconfigured in quick succession and each task for the
     * connector needs to hit the leader with a fencing request during its preflight check.
     */
    @Test
    public void testExternalZombieFencingRequestDelayedCompletion() throws Exception {
        final String conn3 = "SourceC";
        final Map<String, Integer> tasksPerConnector = new HashMap<>();
        tasksPerConnector.put(CONN1, 5);
        tasksPerConnector.put(CONN2, 3);
        tasksPerConnector.put(conn3, 12);

        expectHerderStartup();
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        expectAnyTicks();

        // We invoke the herder's fenceZombies method repeatedly, which adds a new request to the queue.
        // If the queue is empty, the member is woken up; however, if two or more requests are issued in rapid
        // succession, the member won't be woken up. We allow the member to be woken up any number of times
        // here since it's not critical to the testing logic and it's difficult to mock things in order to lead to an
        // exact number of wakeups.
        member.wakeup();
        EasyMock.expectLastCall().anyTimes();

        Map<String, Integer> taskCountRecords = new HashMap<>();
        taskCountRecords.put(CONN1, 2);
        taskCountRecords.put(CONN2, 3);
        taskCountRecords.put(conn3, 5);
        Map<String, Integer> taskConfigGenerations = new HashMap<>();
        taskConfigGenerations.put(CONN1, 3);
        taskConfigGenerations.put(CONN2, 4);
        taskConfigGenerations.put(conn3, 2);
        Set<String> pendingFencing = new HashSet<>(Arrays.asList(CONN1, CONN2, conn3));
        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                taskCountRecords,
                taskConfigGenerations,
                pendingFencing,
                tasksPerConnector
        );
        tasksPerConnector.keySet().forEach(c -> expectConfigRefreshAndSnapshot(configState));

        // The callbacks that the herder has accrued for outstanding fencing futures, which will be completed after
        // a successful round of fencing and a task record write to the config topic
        Map<String, Capture<KafkaFuture.BiConsumer<Void, Throwable>>> herderFencingCallbacks = new HashMap<>();
        // The callbacks that the herder has installed for after a successful round of zombie fencing, but before writing
        // a task record to the config topic
        Map<String, Capture<KafkaFuture.BaseFunction<Void, Void>>> workerFencingFollowups = new HashMap<>();

        Map<String, CountDownLatch> callbacksInstalled = new HashMap<>();
        tasksPerConnector.forEach((connector, numStackedRequests) -> {
            // The future returned by Worker::fenceZombies
            KafkaFuture<Void> workerFencingFuture = EasyMock.mock(KafkaFuture.class);
            // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic) 
            KafkaFuture<Void> herderFencingFuture = EasyMock.mock(KafkaFuture.class);

            Capture<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = EasyMock.newCapture(CaptureType.ALL);
            herderFencingCallbacks.put(connector, herderFencingCallback);

            // Don't immediately invoke callbacks that the herder sets up for when the worker fencing and writes to the config topic have completed
            // Instead, wait for them to be installed, then invoke them explicitly after the fact on a thread separate from the herder's tick thread
            EasyMock.expect(herderFencingFuture.whenComplete(EasyMock.capture(herderFencingCallback)))
                    .andReturn(null)
                    .times(numStackedRequests + 1);

            Capture<KafkaFuture.BaseFunction<Void, Void>> fencingFollowup = EasyMock.newCapture();
            CountDownLatch callbackInstalled = new CountDownLatch(1);
            workerFencingFollowups.put(connector, fencingFollowup);
            callbacksInstalled.put(connector, callbackInstalled);
            EasyMock.expect(workerFencingFuture.thenApply(EasyMock.capture(fencingFollowup))).andAnswer(() -> {
                callbackInstalled.countDown();
                return herderFencingFuture;
            });

            // We should only perform a single physical zombie fencing; all the subsequent requests should be stacked onto the first one
            EasyMock.expect(worker.fenceZombies(
                    EasyMock.eq(connector), EasyMock.eq(taskCountRecords.get(connector)), EasyMock.anyObject())
            ).andReturn(workerFencingFuture);

            for (int i = 0; i < numStackedRequests; i++) {
                expectConfigRefreshAndSnapshot(configState);
            }

            PowerMock.replay(workerFencingFuture, herderFencingFuture);
        });

        tasksPerConnector.forEach((connector, taskCount) -> {
            configBackingStore.putTaskCountRecord(connector, taskCount);
            EasyMock.expectLastCall();
        });

        expectHerderShutdown(false);

        PowerMock.replayAll();


        startBackgroundHerder();

        List<FutureCallback<Void>> stackedFencingRequests = new ArrayList<>();
        tasksPerConnector.forEach((connector, numStackedRequests) -> {
            List<FutureCallback<Void>> connectorFencingRequests = IntStream.range(0, numStackedRequests)
                    .mapToObj(i -> new FutureCallback<Void>())
                    .collect(Collectors.toList());

            connectorFencingRequests.forEach(fencing ->
                    herder.fenceZombieSourceTasks(connector, fencing)
            );

            stackedFencingRequests.addAll(connectorFencingRequests);
        });

        callbacksInstalled.forEach((connector, latch) -> {
            try {
                assertTrue(latch.await(10, TimeUnit.SECONDS));
                workerFencingFollowups.get(connector).getValue().apply(null);
                herderFencingCallbacks.get(connector).getValues().forEach(cb -> cb.accept(null, null));
            } catch (InterruptedException e) {
                fail("Unexpectedly interrupted");
            }
        });

        for (FutureCallback<Void> fencing : stackedFencingRequests) {
            fencing.get(10, TimeUnit.SECONDS);
        }

        stopBackgroundHerder();

        PowerMock.verifyAll();
    }

    @Test
    public void testVerifyTaskGeneration() {
        Map<String, Integer> taskConfigGenerations = new HashMap<>();
        herder.configState = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP, Collections.emptyMap(), taskConfigGenerations, Collections.emptySet(), Collections.emptySet());

        Callback<Void> verifyCallback = EasyMock.mock(Callback.class);
        for (int i = 0; i < 5; i++) {
            verifyCallback.onCompletion(null, null);
            EasyMock.expectLastCall();
        }

        PowerMock.replayAll();

        herder.assignment = new ExtendedAssignment(
                (short) 2, (short) 0, "leader", "leaderUrl", 0,
                Collections.emptySet(), Collections.singleton(TASK1),
                Collections.emptySet(), Collections.emptySet(), 0);

        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 0, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 1, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 2, verifyCallback));

        taskConfigGenerations.put(CONN1, 0);
        herder.verifyTaskGenerationAndOwnership(TASK1, 0, verifyCallback);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 1, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 2, verifyCallback));

        taskConfigGenerations.put(CONN1, 1);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 0, verifyCallback));
        herder.verifyTaskGenerationAndOwnership(TASK1, 1, verifyCallback);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 2, verifyCallback));

        taskConfigGenerations.put(CONN1, 2);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 0, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 1, verifyCallback));
        herder.verifyTaskGenerationAndOwnership(TASK1, 2, verifyCallback);

        taskConfigGenerations.put(CONN1, 3);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 0, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 1, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(TASK1, 2, verifyCallback));

        ConnectorTaskId unassignedTask = new ConnectorTaskId(CONN2, 0);
        taskConfigGenerations.put(unassignedTask.connector(), 1);
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(unassignedTask, 0, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(unassignedTask, 1, verifyCallback));
        assertThrows(ConnectException.class, () -> herder.verifyTaskGenerationAndOwnership(unassignedTask, 2, verifyCallback));

        PowerMock.verifyAll();
    }

    @Test
    public void testKeyExceptionDetection() {
        assertFalse(herder.isPossibleExpiredKeyException(
            time.milliseconds(),
            new RuntimeException()
        ));
        assertFalse(herder.isPossibleExpiredKeyException(
            time.milliseconds(),
            new BadRequestException("")
        ));
        assertFalse(herder.isPossibleExpiredKeyException(
            time.milliseconds() - TimeUnit.MINUTES.toMillis(2),
            new ConnectRestException(FORBIDDEN.getStatusCode(), "")
        ));
        assertTrue(herder.isPossibleExpiredKeyException(
            time.milliseconds(),
            new ConnectRestException(FORBIDDEN.getStatusCode(), "")
        ));
    }

    @Test
    public void testInconsistentConfigs() {
        // FIXME: if we have inconsistent configs, we need to request forced reconfig + write of the connector's task configs
        // This requires inter-worker communication, so needs the REST API
    }


    @Test
    public void testThreadNames() {
        assertTrue(Whitebox.<ThreadPoolExecutor>getInternalState(herder, "herderExecutor").
                getThreadFactory().newThread(EMPTY_RUNNABLE).getName().startsWith(DistributedHerder.class.getSimpleName()));

        assertTrue(Whitebox.<ThreadPoolExecutor>getInternalState(herder, "forwardRequestExecutor").
                getThreadFactory().newThread(EMPTY_RUNNABLE).getName().startsWith("ForwardRequestExecutor"));

        assertTrue(Whitebox.<ThreadPoolExecutor>getInternalState(herder, "startAndStopExecutor").
                getThreadFactory().newThread(EMPTY_RUNNABLE).getName().startsWith("StartAndStopExecutor"));
    }

    @Test
    public void testHerderStopServicesClosesUponShutdown() {
        assertEquals(1, shutdownCalled.getCount());
        herder.stopServices();
        assertEquals(0, shutdownCalled.getCount());
    }

    @Test
    public void testPollDurationOnSlowConnectorOperations() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // If an operation during tick() takes some amount of time, that time should count against the rebalance delay
        final int rebalanceDelayMs = 20000;
        final long operationDelayMs = 10000;
        final long maxPollWaitMs = rebalanceDelayMs - operationDelayMs;
        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(connectProtocolVersion);

        // Assign the connector to this worker, and have it start
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        Capture<Callback<TargetState>> onFirstStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onFirstStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            time.sleep(operationDelayMs);
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, () -> TASK_CONFIGS);
        // We should poll for less than the delay - time to start the connector, meaning that a long connector start
        // does not delay the poll timeout
        member.poll(leq(maxPollWaitMs));
        PowerMock.expectLastCall();

        // Rebalance again due to config update
        member.wakeup();
        PowerMock.expectLastCall();
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        EasyMock.expect(configBackingStore.snapshot()).andReturn(SNAPSHOT_UPDATED_CONN1_CONFIG);

        worker.stopAndAwaitConnector(CONN1);
        PowerMock.expectLastCall();
        EasyMock.expect(member.currentProtocolVersion()).andStubReturn(connectProtocolVersion);
        Capture<Callback<TargetState>> onSecondStart = newCapture();
        worker.startConnector(EasyMock.eq(CONN1), EasyMock.anyObject(), EasyMock.anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED), capture(onSecondStart));
        PowerMock.expectLastCall().andAnswer(() -> {
            time.sleep(operationDelayMs);
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        });
        member.wakeup();
        PowerMock.expectLastCall();
        member.poll(leq(maxPollWaitMs));
        PowerMock.expectLastCall();

        // Third tick should resolve all outstanding requests
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        // which includes querying the connector task configs after the update
        expectExecuteTaskReconfiguration(true, conn1SinkConfigUpdated, () -> {
            time.sleep(operationDelayMs);
            return TASK_CONFIGS;
        });
        member.poll(leq(maxPollWaitMs));
        PowerMock.expectLastCall();

        PowerMock.replayAll();
        herder.tick();
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick();
        herder.tick();
        PowerMock.verifyAll();
    }

    private void expectRebalance(final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks) {
        expectRebalance(offset, assignedConnectors, assignedTasks, false);
    }

    private void expectRebalance(final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks,
                                 final boolean isLeader) {

        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, offset, "leader", "leaderUrl", assignedConnectors, assignedTasks, 0, isLeader);
    }

    private void expectRebalance(final long offset,
                                 final List<String> assignedConnectors, final List<ConnectorTaskId> assignedTasks,
                                 String leader, String leaderUrl, boolean isLeader) {
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, offset, leader, leaderUrl, assignedConnectors, assignedTasks, 0, isLeader);
    }

    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final Collection<String> revokedConnectors,
                                 final List<ConnectorTaskId> revokedTasks,
                                 final short error,
                                 final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks) {
        expectRebalance(revokedConnectors, revokedTasks, error, offset, assignedConnectors, assignedTasks, 0);
    }
    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final Collection<String> revokedConnectors,
                                 final List<ConnectorTaskId> revokedTasks,
                                 final short error,
                                 final long offset,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks,
                                 int delay) {
        expectRebalance(revokedConnectors, revokedTasks, error, offset, "leader", "leaderUrl", assignedConnectors, assignedTasks, delay, false);
    }

    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final Collection<String> revokedConnectors,
                                 final List<ConnectorTaskId> revokedTasks,
                                 final short error,
                                 final long offset,
                                 String leader,
                                 String leaderUrl,
                                 final List<String> assignedConnectors,
                                 final List<ConnectorTaskId> assignedTasks,
                                 int delay,
                                 boolean isLeader) {
        member.ensureActive();
        PowerMock.expectLastCall().andAnswer(() -> {
            ExtendedAssignment assignment;
            if (!revokedConnectors.isEmpty() || !revokedTasks.isEmpty()) {
                rebalanceListener.onRevoked(leader, revokedConnectors, revokedTasks);
            }

            if (connectProtocolVersion == CONNECT_PROTOCOL_V0) {
                assignment = new ExtendedAssignment(
                        connectProtocolVersion, error, leader, leaderUrl, offset,
                        assignedConnectors, assignedTasks,
                        Collections.emptyList(), Collections.emptyList(), 0);
            } else {
                assignment = new ExtendedAssignment(
                        connectProtocolVersion, error, leader, leaderUrl, offset,
                        assignedConnectors, assignedTasks,
                        new ArrayList<>(revokedConnectors), new ArrayList<>(revokedTasks), delay);
            }
            rebalanceListener.onAssigned(assignment, 3);
            time.sleep(100L);
            return null;
        });

        if (isLeader) {
            configBackingStore.claimWritePrivileges();
            EasyMock.expectLastCall();
        }

        if (!revokedConnectors.isEmpty()) {
            for (String connector : revokedConnectors) {
                worker.stopAndAwaitConnector(connector);
                PowerMock.expectLastCall();
            }
        }

        if (!revokedTasks.isEmpty()) {
            worker.stopAndAwaitTask(EasyMock.anyObject(ConnectorTaskId.class));
            PowerMock.expectLastCall();
        }

        if (!revokedConnectors.isEmpty()) {
            statusBackingStore.flush();
            PowerMock.expectLastCall();
        }

        member.wakeup();
        PowerMock.expectLastCall();
    }

    private ClusterConfigState exactlyOnceSnapshot(
            SessionKey sessionKey,
            Map<ConnectorTaskId, Map<String, String>> taskConfigs,
            Map<String, Integer> taskCountRecords,
            Map<String, Integer> taskConfigGenerations,
            Set<String> pendingFencing) {

        Set<String> connectors = new HashSet<>();
        connectors.addAll(taskCountRecords.keySet());
        connectors.addAll(taskConfigGenerations.keySet());
        connectors.addAll(pendingFencing);
        Map<String, Integer> taskCounts = connectors.stream()
                .collect(Collectors.toMap(Function.identity(), c -> 1));

        return exactlyOnceSnapshot(sessionKey, taskConfigs, taskCountRecords, taskConfigGenerations, pendingFencing, taskCounts);
    }

    private ClusterConfigState exactlyOnceSnapshot(
            SessionKey sessionKey,
            Map<ConnectorTaskId, Map<String, String>> taskConfigs,
            Map<String, Integer> taskCountRecords,
            Map<String, Integer> taskConfigGenerations,
            Set<String> pendingFencing,
            Map<String, Integer> taskCounts) {

        Set<String> connectors = new HashSet<>();
        connectors.addAll(taskCounts.keySet());
        connectors.addAll(taskCountRecords.keySet());
        connectors.addAll(taskConfigGenerations.keySet());
        connectors.addAll(pendingFencing);

        Map<String, Map<String, String>> connectorConfigs = connectors.stream()
                .collect(Collectors.toMap(Function.identity(), c -> CONN1_CONFIG));

        return new ClusterConfigState(1, sessionKey, taskCounts,
                connectorConfigs, Collections.singletonMap(CONN1, TargetState.STARTED),
                taskConfigs, taskCountRecords, taskConfigGenerations, pendingFencing, Collections.emptySet());
    }

    private void expectExecuteTaskReconfiguration(boolean running, ConnectorConfig connectorConfig, IAnswer<List<Map<String, String>>> answer) {
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(running);
        if (running) {
            EasyMock.expect(worker.getPlugins()).andReturn(plugins);
            EasyMock.expect(worker.connectorTaskConfigs(CONN1, connectorConfig)).andAnswer(answer);
        }
    }

    private void expectAnyTicks() {
        member.ensureActive();
        EasyMock.expectLastCall().anyTimes();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall().anyTimes();
    }

    private SessionKey expectNewSessionKey() {
        SecretKey secretKey = EasyMock.niceMock(SecretKey.class);
        EasyMock.expect(secretKey.getAlgorithm()).andReturn(INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT).anyTimes();
        EasyMock.expect(secretKey.getEncoded()).andReturn(new byte[32]).anyTimes();
        SessionKey sessionKey = new SessionKey(secretKey, time.milliseconds() + TimeUnit.DAYS.toMillis(1));
        configBackingStore.putSessionKey(anyObject(SessionKey.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            configUpdateListener.onSessionKeyUpdate(sessionKey);
            return null;
        });
        EasyMock.replay(secretKey);
        return sessionKey;
    }

    private void expectConfigRefreshAndSnapshot(final ClusterConfigState readToEndSnapshot) {
        try {
            configBackingStore.refresh(anyLong(), EasyMock.anyObject(TimeUnit.class));
            EasyMock.expectLastCall();
            EasyMock.expect(configBackingStore.snapshot()).andReturn(readToEndSnapshot);
        } catch (TimeoutException e) {
            fail("Mocked method should not throw checked exception");
        }
    }

    private void startBackgroundHerder() {
        herderExecutor = Executors.newSingleThreadExecutor();
        herderFuture = herderExecutor.submit(herder);
    }

    private void stopBackgroundHerder() throws Exception {
        herder.stop();
        herderExecutor.shutdown();
        assertTrue("herder thread did not finish in time", herderExecutor.awaitTermination(10, TimeUnit.SECONDS));
        herderFuture.get();
    }

    private void expectHerderStartup() {
        worker.start();
        EasyMock.expectLastCall();
        statusBackingStore.start();
        EasyMock.expectLastCall();
        configBackingStore.start();
        EasyMock.expectLastCall();
    }

    private void expectHerderShutdown(boolean wakeup) {
        if (wakeup) {
            member.wakeup();
            EasyMock.expectLastCall();
        }
        EasyMock.expect(worker.connectorNames()).andReturn(Collections.emptySet());
        EasyMock.expect(worker.taskIds()).andReturn(Collections.emptySet());
        member.stop();
        EasyMock.expectLastCall();
        statusBackingStore.stop();
        EasyMock.expectLastCall();
        configBackingStore.stop();
        EasyMock.expectLastCall();
        worker.stop();
        EasyMock.expectLastCall();
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
            assertEquals(Double.NaN, rebalanceTimeMax, 0.0001d);
            assertEquals(Double.NaN, rebalanceTimeAvg, 0.0001d);
        } else {
            assertEquals(rebalanceTime, rebalanceTimeMax, 0.0001d);
            assertEquals(rebalanceTime, rebalanceTimeAvg, 0.0001d);
        }
    }

    @Test
    public void processRestartRequestsFailureSuppression() {
        member.wakeup();
        PowerMock.expectLastCall().anyTimes();

        final String connectorName = "foo";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, false);
        EasyMock.expect(herder.buildRestartPlan(restartRequest)).andThrow(new RuntimeException()).anyTimes();

        PowerMock.replayAll();

        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        herder.processRestartRequests();
        assertTrue(herder.pendingRestartRequests.isEmpty());
    }

    @Test
    public void processRestartRequestsDequeue() {
        member.wakeup();
        PowerMock.expectLastCall().anyTimes();

        EasyMock.expect(herder.buildRestartPlan(EasyMock.anyObject(RestartRequest.class))).andReturn(Optional.empty()).anyTimes();

        PowerMock.replayAll();

        RestartRequest restartRequest = new RestartRequest("foo", false, false);
        configUpdateListener.onRestartRequest(restartRequest);
        restartRequest = new RestartRequest("bar", false, false);
        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(2, herder.pendingRestartRequests.size());
        herder.processRestartRequests();
        assertTrue(herder.pendingRestartRequests.isEmpty());
    }

    @Test
    public void preserveHighestImpactRestartRequest() {
        member.wakeup();
        PowerMock.expectLastCall().anyTimes();
        PowerMock.replayAll();

        final String connectorName = "foo";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, false);
        configUpdateListener.onRestartRequest(restartRequest);

        //will overwrite as this is higher impact
        restartRequest = new RestartRequest(connectorName, false, true);
        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        assertFalse(herder.pendingRestartRequests.get(connectorName).onlyFailed());
        assertTrue(herder.pendingRestartRequests.get(connectorName).includeTasks());

        //will be ignored as the existing request has higher impact
        restartRequest = new RestartRequest(connectorName, true, false);
        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        //compare against existing request
        assertFalse(herder.pendingRestartRequests.get(connectorName).onlyFailed());
        assertTrue(herder.pendingRestartRequests.get(connectorName).includeTasks());
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private DistributedHerder exactlyOnceHerder() {
        Map<String, String> config = new HashMap<>(HERDER_CONFIG);
        config.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        return PowerMock.createPartialMock(DistributedHerder.class,
                new String[]{"connectorType", "updateDeletedConnectorStatus", "updateDeletedTaskStatus", "validateConnectorConfig"},
                new DistributedConfig(config), worker, WORKER_ID, KAFKA_CLUSTER_ID,
                statusBackingStore, configBackingStore, member, MEMBER_URL, restClient, metrics, time, noneConnectorClientConfigOverridePolicy,
                new AutoCloseable[0]);
    }

}
