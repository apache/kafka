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
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractStatus;
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
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.TooManyTasksException;
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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.AppliedConnectorConfig;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.Stage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.crypto.SecretKey;

import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.UncheckedCloseable;
import static org.apache.kafka.connect.runtime.AbstractStatus.State.FAILED;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.ExactlyOnceSupportLevel.REQUIRED;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.CONNECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalMatchers.leq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@SuppressWarnings("unchecked")
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
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
            Collections.emptySet(),
            Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_PAUSED_CONN1 = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.PAUSED),
            TASK_CONFIGS_MAP,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
            Collections.emptySet(),
            Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_STOPPED_CONN1 = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 0),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.STOPPED),
            Collections.emptyMap(), // Stopped connectors should have an empty set of task configs
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, 10),
            Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
            Collections.singleton(CONN1),
            Collections.emptySet());

    private static final ClusterConfigState SNAPSHOT_STOPPED_CONN1_FENCED = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 0),
            Collections.singletonMap(CONN1, CONN1_CONFIG),
            Collections.singletonMap(CONN1, TargetState.STOPPED),
            Collections.emptyMap(),
            Collections.singletonMap(CONN1, 0),
            Collections.singletonMap(CONN1, 11),
            Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
            Collections.emptySet(),
            Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_UPDATED_CONN1_CONFIG = new ClusterConfigState(
            1,
            null,
            Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG_UPDATED),
            Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG_UPDATED)),
            Collections.emptySet(),
            Collections.emptySet());

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
    private final CountDownLatch shutdownCalled = new CountDownLatch(1);
    private ConfigBackingStore.UpdateListener configUpdateListener;
    private WorkerRebalanceListener rebalanceListener;
    private ExecutorService herderExecutor;
    private Future<?> herderFuture;

    private SinkConnectorConfig conn1SinkConfig;
    private SinkConnectorConfig conn1SinkConfigUpdated;
    private short connectProtocolVersion;
    private final SampleConnectorClientConfigOverridePolicy
        noneConnectorClientConfigOverridePolicy = new SampleConnectorClientConfigOverridePolicy();

    @BeforeEach
    public void setUp() throws Exception {
        time = new MockTime();
        metrics = new MockConnectMetrics(time);
        AutoCloseable uponShutdown = shutdownCalled::countDown;

        // Default to the old protocol unless specified otherwise
        connectProtocolVersion = CONNECT_PROTOCOL_V0;

        herder = mock(DistributedHerder.class, withSettings().defaultAnswer(CALLS_REAL_METHODS).useConstructor(new DistributedConfig(HERDER_CONFIG),
                worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, configBackingStore, member, MEMBER_URL, restClient, metrics, time,
                noneConnectorClientConfigOverridePolicy, Collections.emptyList(), null, new AutoCloseable[]{uponShutdown}));

        configUpdateListener = herder.new ConfigUpdateListener();
        rebalanceListener = herder.new RebalanceListener(time);
        conn1SinkConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);
        conn1SinkConfigUpdated = new SinkConnectorConfig(plugins, CONN1_CONFIG_UPDATED);
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) metrics.stop();
        if (herderExecutor != null) {
            herderExecutor.shutdownNow();
            herderExecutor = null;
        }
    }

    @Test
    public void testJoinAssignment() {
        // Join group and get assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);
        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        expectMemberPoll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);
        verifyNoMoreInteractions(member, configBackingStore, statusBackingStore, worker);
    }

    @Test
    public void testRebalance() {
        // Join group and get assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);
        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        expectMemberPoll();

        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();
        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);

        verify(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));

        // Rebalance and get a new assignment
        expectRebalance(singletonList(CONN1), singletonList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, singletonList(CONN1), Collections.emptyList());
        herder.tick();
        time.sleep(3000L);
        assertStatistics(3, 2, 100, 3000);

        // Verify that the connector is started twice but the task is only started once (the first mocked rebalance assigns CONN1 and TASK1,
        // the second mocked rebalance revokes CONN1 and TASK1 and (re)assigns CONN1)
        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker, times(2)).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));
        verifyNoMoreInteractions(member, configBackingStore, statusBackingStore, worker);
    }

    @Test
    public void testIncrementalCooperativeRebalanceForNewMember() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains revocations from other members. For the new
        // member the assignment should be empty
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectMemberPoll();

        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();

        // The new member got its assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, singletonList(CONN1), singletonList(TASK1), 0);

        // and the new assignment started
        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);

        herder.tick();
        time.sleep(3000L);
        assertStatistics(3, 2, 100, 3000);

        verify(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));
        verifyNoMoreInteractions(member, statusBackingStore, configBackingStore, worker);
    }

    @Test
    public void testIncrementalCooperativeRebalanceForExistingMember() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains revocations because a new member joined.
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);
        expectRebalance(singletonList(CONN1), singletonList(TASK1),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                Collections.emptyList(), Collections.emptyList(), 0);
        doNothing().when(member).requestRejoin();
        expectMemberPoll();

        herder.configState = SNAPSHOT;
        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();

        // In the second rebalance the new member gets its assignment and this member has no
        // assignments or revocations
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);

        herder.tick();
        time.sleep(3000L);
        assertStatistics(3, 2, 100, 3000);

        verifyNoMoreInteractions(member, statusBackingStore, configBackingStore, worker);
    }

    @Test
    public void testIncrementalCooperativeRebalanceWithDelay() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // Join group. First rebalance contains some assignments but also a delay, because a
        // member was detected missing
        int rebalanceDelay = 10_000;

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                Collections.emptyList(), singletonList(TASK2),
                rebalanceDelay);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(worker.startSourceTask(eq(TASK2), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);
        doAnswer(invocation -> {
            time.sleep(9900L);
            return null;
        }).when(member).poll(anyLong(), any());

        // Request to re-join expected because the scheduled rebalance delay has been reached
        doNothing().when(member).requestRejoin();

        time.sleep(1000L);
        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();

        // The member got its assignment and revocation
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, singletonList(CONN1), singletonList(TASK1), 0);

        // and the new assignment started
        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);
        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        expectMemberPoll();

        herder.tick();
        time.sleep(2000L);
        assertStatistics(3, 2, 100, 2000);

        verifyNoMoreInteractions(member, statusBackingStore, configBackingStore, worker);
    }

    @Test
    public void testRebalanceFailedConnector() {
        // Join group and get assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1));
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);
        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        expectMemberPoll();

        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        verify(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));

        // Rebalance and get a new assignment
        expectRebalance(singletonList(CONN1), singletonList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, singletonList(CONN1), Collections.emptyList());

        // worker is not running, so we should see no call to connectorTaskConfigs()
        expectExecuteTaskReconfiguration(false, null, null);

        herder.tick();
        time.sleep(2000L);
        assertStatistics(3, 2, 100, 2000L);

        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));

        verifyNoMoreInteractions(member, statusBackingStore, configBackingStore, worker);
    }

    @Test
    public void testRevoke() {
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        revokeAndReassign(false);
    }

    @Test
    public void testIncompleteRebalanceBeforeRevoke() {
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        revokeAndReassign(true);
    }

    public void revokeAndReassign(boolean incompleteRebalance) {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        int configOffset = 1;

        // Join group and get initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(connectProtocolVersion);
        // The lists need to be mutable because assignments might be removed
        expectRebalance(configOffset, new ArrayList<>(singletonList(CONN1)), new ArrayList<>(singletonList(TASK1)));
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        expectMemberPoll();

        herder.tick();

        // worker is stable with an existing set of tasks

        if (incompleteRebalance) {
            // Perform a partial re-balance just prior to the revocation
            // bump the configOffset to trigger reading the config topic to the end
            configOffset++;
            expectRebalance(configOffset, Collections.emptyList(), Collections.emptyList());
            // give it the wrong snapshot, as if we're out of sync/can't reach the broker
            expectConfigRefreshAndSnapshot(SNAPSHOT);
            doNothing().when(member).requestRejoin();
            // tick exits early because we failed, and doesn't do the poll at the end of the method
            // the worker did not startWork or reset the rebalanceResolved flag
            herder.tick();
        }

        // Revoke the connector in the next rebalance
        expectRebalance(singletonList(CONN1), Collections.emptyList(),
            ConnectProtocol.Assignment.NO_ERROR, configOffset, Collections.emptyList(),
                Collections.emptyList());

        if (incompleteRebalance) {
            // Same as SNAPSHOT, except with an updated offset
            // Allow the task to read to the end of the topic and complete the rebalance
            ClusterConfigState secondSnapshot = new ClusterConfigState(
                    configOffset,
                    null,
                    Collections.singletonMap(CONN1, 3),
                    Collections.singletonMap(CONN1, CONN1_CONFIG),
                    Collections.singletonMap(CONN1, TargetState.STARTED),
                    TASK_CONFIGS_MAP,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                    Collections.emptySet(),
                    Collections.emptySet()
            );
            expectConfigRefreshAndSnapshot(secondSnapshot);
        }

        doNothing().when(member).requestRejoin();

        herder.tick();

        // re-assign the connector back to the same worker to ensure state was cleaned up
        expectRebalance(configOffset, singletonList(CONN1), Collections.emptyList());

        herder.tick();

        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker, times(2)).connectorTaskConfigs(eq(CONN1), eq(conn1SinkConfig));
        verify(worker).startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED));
        verify(worker).stopAndAwaitConnector(CONN1);

        // The tick loop where the revoke happens returns early (because there's a subsequent rebalance) and doesn't result in a poll at
        // the end of the method
        verify(member, times(2)).poll(anyLong(), any());

        verifyNoMoreInteractions(member, statusBackingStore, configBackingStore, worker);
    }

    @Test
    public void testHaltCleansUpWorker() {
        herder.halt();

        verify(worker).stopAndAwaitConnectors();
        verify(worker).stopAndAwaitTasks();
        verify(member).stop();
        verify(configBackingStore).stop();
        verify(statusBackingStore).stop();
        verify(worker).stop();

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testCreateConnector() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // Initial rebalance where this member becomes the leader
        herder.tick();

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN2_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(CONN2_CONFIG), validateCallback.capture());

        // CONN2 is new, should succeed
        doNothing().when(configBackingStore).putConnectorConfig(eq(CONN2), eq(CONN2_CONFIG), isNull());

        // This will occur just before/during the second tick
        expectMemberEnsureActive();

        // No immediate action besides this -- change will be picked up via the config log

        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        // This tick runs the initial herder request, which issues an asynchronous request for
        // connector validation
        herder.tick();

        // Once that validation is complete, another request is added to the herder request queue
        // for actually performing the config write; this tick is for that request
        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        ConnectorInfo info = new ConnectorInfo(CONN2, CONN2_CONFIG, Collections.emptyList(), ConnectorType.SOURCE);
        verify(putConnectorCallback).onCompletion(isNull(), eq(new Herder.Created<>(true, info)));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "ensuring membership in the cluster",
                        "writing a config for connector " + CONN2 + " to the config topic"
                ),
                stages
        );
    }

    @Test
    public void testCreateConnectorWithInitialState() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // Initial rebalance where this member becomes the leader
        herder.tick();

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN2_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(CONN2_CONFIG), validateCallback.capture());

        // CONN2 is new, should succeed
        doNothing().when(configBackingStore).putConnectorConfig(eq(CONN2), eq(CONN2_CONFIG), eq(TargetState.STOPPED));

        // This will occur just before/during the second tick
        expectMemberEnsureActive();

        // No immediate action besides this -- change will be picked up via the config log
        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, TargetState.STOPPED, false, putConnectorCallback);
        // This tick runs the initial herder request, which issues an asynchronous request for
        // connector validation
        herder.tick();

        // Once that validation is complete, another request is added to the herder request queue
        // for actually performing the config write; this tick is for that request
        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        ConnectorInfo info = new ConnectorInfo(CONN2, CONN2_CONFIG, Collections.emptyList(), ConnectorType.SOURCE);
        verify(putConnectorCallback).onCompletion(isNull(), eq(new Herder.Created<>(true, info)));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "ensuring membership in the cluster",
                        "writing a config for connector " + CONN2 + " to the config topic"
                ),
                stages
        );
    }

    @Test
    public void testCreateConnectorConfigBackingStoreError() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // Initial rebalance where this member becomes the leader
        herder.tick();

        // Mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN2_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(CONN2_CONFIG), validateCallback.capture());

        doThrow(new ConnectException("Error writing connector configuration to Kafka"))
                .when(configBackingStore).putConnectorConfig(eq(CONN2), eq(CONN2_CONFIG), isNull());

        // This will occur just before/during the second tick
        expectMemberEnsureActive();

        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        // This tick runs the initial herder request, which issues an asynchronous request for
        // connector validation
        herder.tick();

        // Once that validation is complete, another request is added to the herder request queue
        // for actually performing the config write; this tick is for that request
        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        // Verify that the exception thrown during the config backing store write is propagated via the callback
        verify(putConnectorCallback).onCompletion(any(ConnectException.class), isNull());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "ensuring membership in the cluster",
                        "writing a config for connector " + CONN2 + " to the config topic"
                ),
                stages
        );
    }

    @Test
    public void testCreateConnectorFailedValidation() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        HashMap<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.remove(ConnectorConfig.NAME_CONFIG);

        // Mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN2_INVALID_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(config), validateCallback.capture());

        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.putConnectorConfig(CONN2, config, false, putConnectorCallback);
        herder.tick();

        // We don't need another rebalance to occur
        expectMemberEnsureActive();
        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(putConnectorCallback).onCompletion(error.capture(), isNull());
        assertInstanceOf(BadRequestException.class, error.getValue());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "awaiting startup",
                        "ensuring membership in the cluster",
                        "reading to the end of the config topic"
                ),
                stages
        );
    }

    @Test
    public void testConnectorNameConflictsWithWorkerGroupId() {
        Map<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.put(ConnectorConfig.NAME_CONFIG, "test-group");

        SinkConnector connectorMock = mock(SinkConnector.class);

        // CONN2 creation should fail because the worker group id (connect-test-group) conflicts with
        // the consumer group id we would use for this sink
        Map<String, ConfigValue> validatedConfigs = herder.validateSinkConnectorConfig(
                connectorMock, SinkConnectorConfig.configDef(), config);

        ConfigValue nameConfig = validatedConfigs.get(ConnectorConfig.NAME_CONFIG);
        assertEquals(
                Collections.singletonList("Consumer group for sink connector named test-group conflicts with Connect worker group connect-test-group"),
                nameConfig.errorMessages());
    }

    @Test
    public void testConnectorGroupIdConflictsWithWorkerGroupId() {
        String overriddenGroupId = CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + GROUP_ID_CONFIG;
        Map<String, String> config = new HashMap<>(CONN2_CONFIG);
        config.put(overriddenGroupId, "connect-test-group");

        SinkConnector connectorMock = mock(SinkConnector.class);

        // CONN2 creation should fail because the worker group id (connect-test-group) conflicts with
        // the consumer group id we would use for this sink
        Map<String, ConfigValue> validatedConfigs = herder.validateSinkConnectorConfig(
                connectorMock, SinkConnectorConfig.configDef(), config);

        ConfigValue overriddenGroupIdConfig = validatedConfigs.get(overriddenGroupId);
        assertEquals(
                Collections.singletonList("Consumer group connect-test-group conflicts with Connect worker group connect-test-group"),
                overriddenGroupIdConfig.errorMessages());

        ConfigValue nameConfig = validatedConfigs.get(ConnectorConfig.NAME_CONFIG);
        assertEquals(
                Collections.emptyList(),
                nameConfig.errorMessages()
        );
    }

    @Test
    public void testCreateConnectorAlreadyExists() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // mock the actual validation since its asynchronous nature is difficult to test and should
        // be covered sufficiently by the unit tests for the AbstractHerder class
        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN1_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(CONN1_CONFIG), validateCallback.capture());

        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.putConnectorConfig(CONN1, CONN1_CONFIG, false, putConnectorCallback);
        herder.tick();

        // We don't need another rebalance to occur
        expectMemberEnsureActive();
        herder.tick();
        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        // CONN1 already exists
        verify(putConnectorCallback).onCompletion(any(AlreadyExistsException.class), isNull());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "awaiting startup",
                        "ensuring membership in the cluster",
                        "reading to the end of the config topic"
                ),
                stages
        );
    }

    @Test
    public void testDestroyConnector() {
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        // Start with one connector
        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // And delete the connector
        doNothing().when(configBackingStore).removeConnectorConfig(CONN1);
        doNothing().when(putConnectorCallback).onCompletion(null, new Herder.Created<>(false, null));

        List<String> stages = expectRecordStages(putConnectorCallback);

        herder.deleteConnectorConfig(CONN1, putConnectorCallback);

        herder.tick();
        time.sleep(1000L);
        assertStatistics("leaderUrl", false, 3, 1, 100, 1000L);

        // The change eventually is reflected to the config topic and the deleted connector and
        // tasks are revoked
        TopicStatus fooStatus = new TopicStatus(FOO_TOPIC, CONN1, 0, time.milliseconds());
        TopicStatus barStatus = new TopicStatus(BAR_TOPIC, CONN1, 0, time.milliseconds());
        when(statusBackingStore.getAllTopics(eq(CONN1))).thenReturn(new HashSet<>(Arrays.asList(fooStatus, barStatus)));
        doNothing().when(statusBackingStore).deleteTopic(eq(CONN1), eq(FOO_TOPIC));
        doNothing().when(statusBackingStore).deleteTopic(eq(CONN1), eq(BAR_TOPIC));

        expectRebalance(singletonList(CONN1), singletonList(TASK1),
                ConnectProtocol.Assignment.NO_ERROR, 2, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);
        expectConfigRefreshAndSnapshot(ClusterConfigState.EMPTY);
        doNothing().when(member).requestRejoin();

        configUpdateListener.onConnectorConfigRemove(CONN1); // read updated config that removes the connector
        herder.configState = ClusterConfigState.EMPTY;
        herder.tick();
        time.sleep(1000L);
        assertStatistics("leaderUrl", true, 3, 1, 100, 2100L);

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, putConnectorCallback);

        assertEquals(
                Arrays.asList(
                        "awaiting startup",
                        "ensuring membership in the cluster",
                        "reading to the end of the config topic",
                        "starting 1 connector(s) and task(s) after a rebalance",
                        "removing the config for connector sourceA from the config topic"
                ),
                stages
        );
    }

    @Test
    public void testRestartConnector() throws Exception {

        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        // Initial rebalance where this member becomes the leader
        herder.tick();

        expectMemberEnsureActive();

        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
        herder.tick();
        callback.get(1000L, TimeUnit.MILLISECONDS);

        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verify(worker, times(2)).connectorTaskConfigs(eq(CONN1), any());
        verify(worker).stopAndAwaitConnector(CONN1);
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartUnknownConnector() {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN2, callback);
        herder.tick();

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals(NotFoundException.class, e.getCause().getClass());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorRedirectToLeader() {
        // get the initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
        herder.tick();

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals(NotLeaderException.class, e.getCause().getClass());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorRedirectToOwner() {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();
        String ownerUrl = "ownerUrl";
        when(member.ownerUrl(CONN1)).thenReturn(ownerUrl);

        time.sleep(1000L);
        assertStatistics(3, 1, 100, 1000L);

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);

        herder.tick();
        time.sleep(2000L);
        assertStatistics(3, 1, 100, 3000L);

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals(NotAssignedException.class, e.getCause().getClass());
        assertEquals(ownerUrl, ((NotAssignedException) e.getCause()).forwardUrl());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorAndTasksUnknownConnector() {
        String connectorName = "UnknownConnector";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);

        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();
        herder.tick();
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, ee.getCause());
        assertTrue(ee.getMessage().contains("Unknown connector:"));

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorAndTasksNotLeader() {
        // get the initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();

        herder.tick();
        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotLeaderException.class, ee.getCause());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorAndTasksUnknownStatus() {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();
        when(statusBackingStore.get(CONN1)).thenReturn(null);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        doNothing().when(configBackingStore).putRestartRequest(restartRequest);

        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        ExecutionException ee = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, ee.getCause());
        assertTrue(ee.getMessage().contains("Status for connector"));

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartConnectorAndTasksSuccess() throws Exception {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the connector restart
        expectMemberEnsureActive();

        RestartPlan restartPlan = mock(RestartPlan.class);
        ConnectorStateInfo connectorStateInfo = mock(ConnectorStateInfo.class);
        when(restartPlan.restartConnectorStateInfo()).thenReturn(connectorStateInfo);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);
        doNothing().when(configBackingStore).putRestartRequest(restartRequest);

        FutureCallback<ConnectorStateInfo> callback = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, callback);
        herder.tick();
        assertEquals(connectorStateInfo,  callback.get(1000L, TimeUnit.MILLISECONDS));

        verifyNoMoreInteractions(restartPlan, worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testDoRestartConnectorAndTasksEmptyPlan() {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        doReturn(Optional.empty()).when(herder).buildRestartPlan(restartRequest);
        herder.doRestartConnectorAndTasks(restartRequest);

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testDoRestartConnectorAndTasksNoAssignments() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.taskIdsToRestart()).thenReturn(Collections.singletonList(taskId));

        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.assignment = ExtendedAssignment.empty();
        herder.doRestartConnectorAndTasks(restartRequest);

        verifyNoMoreInteractions(restartPlan, worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testDoRestartConnectorAndTasksOnlyConnector() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.taskIdsToRestart()).thenReturn(Collections.singletonList(taskId));

        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.assignment = mock(ExtendedAssignment.class);
        when(herder.assignment.connectors()).thenReturn(Collections.singletonList(CONN1));
        when(herder.assignment.tasks()).thenReturn(Collections.emptyList());

        herder.configState = SNAPSHOT;

        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        ConnectorStatus status = new ConnectorStatus(CONN1, AbstractStatus.State.RESTARTING, WORKER_ID, 0);
        doNothing().when(statusBackingStore).put(eq(status));

        ArgumentCaptor<Callback<TargetState>>  stateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            stateCallback.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), any(), stateCallback.capture());
        doNothing().when(member).wakeup();

        herder.doRestartConnectorAndTasks(restartRequest);

        verifyNoMoreInteractions(restartPlan, worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testDoRestartConnectorAndTasksOnlyTasks() {
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        // The connector has three tasks
        when(restartPlan.taskIdsToRestart()).thenReturn(Arrays.asList(TASK0, TASK1, TASK2));
        when(restartPlan.totalTaskCount()).thenReturn(3);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.assignment = mock(ExtendedAssignment.class);
        when(herder.assignment.connectors()).thenReturn(Collections.emptyList());
        // But only one task is assigned to this worker
        when(herder.assignment.tasks()).thenReturn(Collections.singletonList(TASK0));
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        herder.configState = SNAPSHOT;

        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(TASK0));

        TaskStatus status = new TaskStatus(TASK0, AbstractStatus.State.RESTARTING, WORKER_ID, 0);
        doNothing().when(statusBackingStore).put(eq(status));

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), any())).thenReturn(true);

        herder.doRestartConnectorAndTasks(restartRequest);

        verifyNoMoreInteractions(restartPlan, worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testDoRestartConnectorAndTasksBoth() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONN1, 0);
        RestartRequest restartRequest = new RestartRequest(CONN1, false, true);
        RestartPlan restartPlan = mock(RestartPlan.class);
        when(restartPlan.shouldRestartConnector()).thenReturn(true);
        when(restartPlan.taskIdsToRestart()).thenReturn(Collections.singletonList(taskId));
        when(restartPlan.totalTaskCount()).thenReturn(1);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        doReturn(Optional.of(restartPlan)).when(herder).buildRestartPlan(restartRequest);

        herder.assignment = mock(ExtendedAssignment.class);
        when(herder.assignment.connectors()).thenReturn(Collections.singletonList(CONN1));
        when(herder.assignment.tasks()).thenReturn(Collections.singletonList(taskId));

        herder.configState = SNAPSHOT;

        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        ConnectorStatus status = new ConnectorStatus(CONN1, AbstractStatus.State.RESTARTING, WORKER_ID, 0);
        doNothing().when(statusBackingStore).put(eq(status));

        ArgumentCaptor<Callback<TargetState>>  stateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            stateCallback.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), any(), stateCallback.capture());
        doNothing().when(member).wakeup();

        doNothing().when(worker).stopAndAwaitTasks(Collections.singletonList(taskId));

        TaskStatus taskStatus = new TaskStatus(TASK0, AbstractStatus.State.RESTARTING, WORKER_ID, 0);
        doNothing().when(statusBackingStore).put(eq(taskStatus));

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), any())).thenReturn(true);

        herder.doRestartConnectorAndTasks(restartRequest);

        verifyNoMoreInteractions(restartPlan, worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartTask() throws Exception {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), any())).thenReturn(true);

        herder.tick();

        // now handle the task restart
        expectMemberEnsureActive();
        doNothing().when(worker).stopAndAwaitTask(TASK0);
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();
        callback.get(1000L, TimeUnit.MILLISECONDS);

        verify(worker, times(2)).startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), any());
        verify(worker).stopAndAwaitTask(TASK0);
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartUnknownTask() {
        // get the initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        herder.tick();

        expectMemberEnsureActive();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(new ConnectorTaskId("blah", 0), callback);
        herder.tick();

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(NotFoundException.class, e.getCause().getClass());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartTaskRedirectToLeader() {
        // get the initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        herder.tick();

        // now handle the task restart
        expectMemberEnsureActive();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(NotLeaderException.class, e.getCause().getClass());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRestartTaskRedirectToOwner() {
        // get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        // now handle the task restart
        String ownerUrl = "ownerUrl";
        when(member.ownerUrl(TASK0)).thenReturn(ownerUrl);
        expectMemberEnsureActive();

        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartTask(TASK0, callback);
        herder.tick();

        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(NotAssignedException.class, e.getCause().getClass());
        assertEquals(ownerUrl, ((NotAssignedException) e.getCause()).forwardUrl());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testRequestProcessingOrder() {
        Callable<Void> action = mock(Callable.class);
        Callback<Void> callback = mock(Callback.class);

        final DistributedHerder.DistributedHerderRequest req1 = herder.addRequest(100, action, callback);
        final DistributedHerder.DistributedHerderRequest req2 = herder.addRequest(10, action, callback);
        final DistributedHerder.DistributedHerderRequest req3 = herder.addRequest(200, action, callback);
        final DistributedHerder.DistributedHerderRequest req4 = herder.addRequest(200, action, callback);

        assertEquals(req2, herder.requests.pollFirst()); // lowest delay
        assertEquals(req1, herder.requests.pollFirst()); // next lowest delay
        assertEquals(req3, herder.requests.pollFirst()); // same delay as req4, but added first
        assertEquals(req4, herder.requests.pollFirst());
    }

    @Test
    public void testConnectorConfigAdded() {
        // If a connector was added, we need to rebalance
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        // join, no configs so no need to catch up on config topic
        expectRebalance(-1, Collections.emptyList(), Collections.emptyList());
        expectMemberPoll();

        herder.tick(); // join

        // Checks for config updates and starts rebalance
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);
        // Rebalance will be triggered when the new config is detected
        doNothing().when(member).requestRejoin();

        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config

        // Performs rebalance and gets new assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList());

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick(); // do rebalance

        verify(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @ParameterizedTest
    @ValueSource(shorts = {CONNECT_PROTOCOL_V0, CONNECT_PROTOCOL_V1, CONNECT_PROTOCOL_V2})
    public void testConnectorConfigDetectedAfterLeaderAlreadyAssigned(short protocolVersion) {
        connectProtocolVersion = protocolVersion;

        // If a connector was added, we need to rebalance
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(protocolVersion);

        // join, no configs so no need to catch up on config topic
        expectRebalance(-1, Collections.emptyList(), Collections.emptyList());
        expectMemberPoll();

        herder.tick(); // join

        // Checks for config updates and starts rebalance
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        // Rebalance will be triggered when the new config is detected
        // This rebalance is unnecessary and only the result of a mostly-benign bug;
        // see https://issues.apache.org/jira/browse/KAFKA-17155
        doNothing().when(member).requestRejoin();

        // Rebalance will be triggered when the new config is detected
        // Performs rebalance and gets new assignment
        // Important--we're simulating a scenario where the leader has already detected the new
        // connector, and assigns it to our herder at the top of its tick thread
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList());

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick(); // assigned connector

        // We should only start the connector once; if we start it several times, that's probably a bug
        verify(worker, times(1)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorConfigUpdate() {
        // Connector config can be applied without any rebalance

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        // join
        expectRebalance(1, singletonList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick();

        // apply config
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT); // for this test, it doesn't matter if we use the same config snapshot
        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick();

        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorConfigUpdateFailedTransformation() {
        // Connector config can be applied without any rebalance

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        WorkerConfigTransformer configTransformer = mock(WorkerConfigTransformer.class);
        // join
        expectRebalance(1, singletonList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick();

        // apply config
        expectMemberEnsureActive();
        // During the next tick, throw an error from the transformer
        ClusterConfigState snapshotWithTransform = new ClusterConfigState(
                1,
                null,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet(),
                configTransformer
        );
        when(configBackingStore.snapshot()).thenReturn(snapshotWithTransform);
        when(configTransformer.transform(eq(CONN1), any()))
            .thenThrow(new ConfigException("Simulated exception thrown during config transformation"));
        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        ArgumentCaptor<ConnectorStatus> failedStatus = ArgumentCaptor.forClass(ConnectorStatus.class);
        doNothing().when(statusBackingStore).putSafe(failedStatus.capture());

        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick();

        assertEquals(CONN1, failedStatus.getValue().id());
        assertEquals(FAILED, failedStatus.getValue().state());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore, configTransformer);
    }

    @Test
    public void testConnectorPaused() {
        // ensure that target state changes are propagated to the worker

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        // join
        expectRebalance(1, singletonList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick(); // join

        // handle the state change
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT_PAUSED_CONN1);

        ArgumentCaptor<Callback<TargetState>> onPause = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onPause.getValue().onCompletion(null, TargetState.PAUSED);
            return null;
        }).when(worker).setTargetState(eq(CONN1), eq(TargetState.PAUSED), onPause.capture());

        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // worker should apply the state change
        herder.tick();

        verify(worker).setTargetState(eq(CONN1), eq(TargetState.PAUSED), any(Callback.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorResumed() {
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        // start with the connector paused
        expectRebalance(1, singletonList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT_PAUSED_CONN1);
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.PAUSED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.PAUSED), onStart.capture());

        herder.tick(); // join

        // handle the state change
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onResume = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onResume.getValue().onCompletion(null, TargetState.STARTED);
            return null;
        }).when(worker).setTargetState(eq(CONN1), eq(TargetState.STARTED), onResume.capture());
        // we expect reconfiguration after resuming
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to started
        herder.tick(); // apply state change
        herder.tick();

        verify(worker).setTargetState(eq(CONN1), eq(TargetState.STARTED), any(Callback.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorStopped() {
        // ensure that target state changes are propagated to the worker

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        // join
        expectRebalance(1, singletonList(CONN1), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick(); // join

        // handle the state change
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT_STOPPED_CONN1);

        ArgumentCaptor<Callback<TargetState>> onStop = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStop.getValue().onCompletion(null, TargetState.STOPPED);
            return null;
        }).when(worker).setTargetState(eq(CONN1), eq(TargetState.STOPPED), onStop.capture());

        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to stopped
        herder.tick(); // worker should apply the state change
        herder.tick();

        verify(worker).setTargetState(eq(CONN1), eq(TargetState.STOPPED), any(Callback.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testUnknownConnectorPaused() {
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick(); // join

        // state change is ignored since we have no target state
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);

        configUpdateListener.onConnectorTargetStateChange("unknown-connector");
        herder.tick(); // continue

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testStopConnector() throws Exception {
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        // join as leader
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick(); // join

        // handle stop request
        expectMemberEnsureActive();
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        doNothing().when(configBackingStore).putTaskConfigs(CONN1, Collections.emptyList());
        doNothing().when(configBackingStore).putTargetState(CONN1, TargetState.STOPPED);

        FutureCallback<Void> cb = new FutureCallback<>();

        herder.stopConnector(CONN1, cb); // external request
        herder.tick(); // continue

        assertTrue(cb.isDone(), "Callback should already have been invoked by herder");
        cb.get(0, TimeUnit.MILLISECONDS);

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testStopConnectorNotLeader() {
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        // join as member (non-leader)
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick();

        // handle stop request
        expectMemberEnsureActive();
        FutureCallback<Void> cb = new FutureCallback<>();

        herder.stopConnector(CONN1, cb); // external request
        herder.tick(); // continue

        assertTrue(cb.isDone(), "Callback should already have been invoked by herder");
        ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> cb.get(0, TimeUnit.SECONDS),
            "Should not be able to handle request to stop connector when not leader"
        );
        assertInstanceOf(NotLeaderException.class, e.getCause());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testStopConnectorFailToWriteTaskConfigs() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        // join as leader
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick(); // join

        ConnectException taskConfigsWriteException = new ConnectException("Could not write task configs to config topic");
        // handle stop request
        expectMemberEnsureActive();
        doThrow(taskConfigsWriteException).when(configBackingStore).putTaskConfigs(CONN1, Collections.emptyList());
        // We do not expect configBackingStore::putTargetState to be invoked, which
        // is intentional since that call should only take place if we are first able to
        // successfully write the empty list of task configs

        FutureCallback<Void> cb = new FutureCallback<>();

        herder.stopConnector(CONN1, cb); // external request
        herder.tick(); // continue

        assertTrue(cb.isDone(), "Callback should already have been invoked by herder");
        ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> cb.get(0, TimeUnit.SECONDS),
            "Should not be able to handle request to stop connector when not leader"
        );
        assertEquals(e.getCause(), taskConfigsWriteException);

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorPausedRunningTaskOnly() {
        // even if we don't own the connector, we should still propagate target state
        // changes to the worker so that tasks will transition correctly

        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick(); // join

        // handle the state change
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT_PAUSED_CONN1);

        ArgumentCaptor<Callback<TargetState>> onPause = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onPause.getValue().onCompletion(null, TargetState.PAUSED);
            return null;
        }).when(worker).setTargetState(eq(CONN1), eq(TargetState.PAUSED), onPause.capture());

        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // apply state change

        verify(worker).setTargetState(eq(CONN1), eq(TargetState.PAUSED), any(Callback.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testConnectorResumedRunningTaskOnly() {
        // even if we don't own the connector, we should still propagate target state
        // changes to the worker so that tasks will transition correctly

        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        // join
        expectRebalance(1, Collections.emptyList(), singletonList(TASK0));
        expectConfigRefreshAndSnapshot(SNAPSHOT_PAUSED_CONN1);
        expectMemberPoll();

        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.PAUSED))).thenReturn(true);

        herder.tick(); // join

        // handle the state change
        expectMemberEnsureActive();
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.PAUSED);
            return null;
        }).when(worker).setTargetState(eq(CONN1), eq(TargetState.STARTED), onStart.capture());

        configUpdateListener.onConnectorTargetStateChange(CONN1); // state changes to paused
        herder.tick(); // apply state change
        herder.tick();

        verify(worker).setTargetState(eq(CONN1), eq(TargetState.STARTED), any(Callback.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testTaskConfigAdded() {
        // Task config always requires rebalance
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        // join
        expectRebalance(-1, Collections.emptyList(), Collections.emptyList());
        expectMemberPoll();

        herder.tick(); // join

        // Checks for config updates and starts rebalance
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);
        // Rebalance will be triggered when the new config is detected
        doNothing().when(member).requestRejoin();

        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK0, TASK1, TASK2)); // read updated config
        herder.tick(); // apply config

        // Performs rebalance and gets new assignment
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Collections.emptyList(),
                singletonList(TASK0));
        when(worker.startSourceTask(eq(TASK0), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        herder.tick(); // do rebalance

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testJoinLeaderCatchUpFails() throws Exception {
        // Join group and as leader fail to do assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl", Collections.emptyList(),
                Collections.emptyList(), 0, true);

        // Reading to end of log times out
        doThrow(new TimeoutException()).when(configBackingStore).refresh(anyLong(), any(TimeUnit.class));
        doNothing().when(member).maybeLeaveGroup(eq("taking too long to read the log"));
        doNothing().when(member).requestRejoin();

        long before = time.milliseconds();
        int workerUnsyncBackoffMs = DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT;
        int coordinatorDiscoveryTimeoutMs = 100;
        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());

        time.sleep(1000L);
        assertStatistics("leaderUrl", true, 3, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

        before = time.milliseconds();

        // After backoff, restart the process and this time succeed
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);
        expectMemberPoll();

        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs, time.milliseconds());
        time.sleep(2000L);
        assertStatistics("leaderUrl", false, 3, 1, 100, 2000L);

        // one more tick, to make sure we don't keep trying to read to the config topic unnecessarily
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);

        // tick once more to ensure that the successful read to the end of the config topic was
        // tracked and no further unnecessary attempts were made
        herder.tick();

        verify(configBackingStore, times(2)).refresh(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testJoinLeaderCatchUpRetriesForIncrementalCooperative() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;

        // Join group as leader
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();

        // The leader gets the same assignment after a rebalance is triggered
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", singletonList(CONN1), singletonList(TASK1), 0, true);

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);

        herder.tick();

        // Another rebalance is triggered but this time it fails to read to the max offset and
        // triggers a re-sync
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);

        // The leader will retry a few times to read to the end of the config log
        doNothing().when(member).requestRejoin();
        doThrow(TimeoutException.class).when(configBackingStore).refresh(anyLong(), any(TimeUnit.class));
        doNothing().when(member).maybeLeaveGroup(eq("taking too long to read the log"));

        long before;
        int coordinatorDiscoveryTimeoutMs = 100;
        int maxRetries = 5;
        int retries = 3;
        for (int i = maxRetries; i >= maxRetries - retries; --i) {
            before = time.milliseconds();
            int workerUnsyncBackoffMs =
                    DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT / 10 / i;
            herder.tick();
            assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());
            coordinatorDiscoveryTimeoutMs = 0;
        }

        // After a few retries succeed to read the log to the end
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", singletonList(CONN1), singletonList(TASK1), 0, true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        before = time.milliseconds();
        coordinatorDiscoveryTimeoutMs = 100;
        herder.tick();
        assertEquals(before + coordinatorDiscoveryTimeoutMs, time.milliseconds());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testJoinLeaderCatchUpFailsForIncrementalCooperative() throws Exception {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;

        // Join group as leader
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, singletonList(CONN1), singletonList(TASK1), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectMemberPoll();

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        when(worker.startSourceTask(eq(TASK1), any(), any(), any(), eq(herder), eq(TargetState.STARTED))).thenReturn(true);

        assertStatistics(0, 0, 0, Double.POSITIVE_INFINITY);

        herder.tick();

        // The leader gets the same assignment after a rebalance is triggered
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1,
                "leader", "leaderUrl", singletonList(CONN1), singletonList(TASK1), 0, true);

        time.sleep(2000L);
        assertStatistics(3, 1, 100, 2000);

        herder.tick();

        // Another rebalance is triggered but this time it fails to read to the max offset and
        // triggers a re-sync
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.CONFIG_MISMATCH, 1, "leader", "leaderUrl",
                Collections.emptyList(), Collections.emptyList(), 0, true);

        // The leader will exhaust the retries while trying to read to the end of the config log
        doNothing().when(member).requestRejoin();
        doThrow(TimeoutException.class).when(configBackingStore).refresh(anyLong(), any(TimeUnit.class));
        doNothing().when(member).maybeLeaveGroup(eq("taking too long to read the log"));

        long before;
        int coordinatorDiscoveryTimeoutMs = 100;
        int maxRetries = 5;
        for (int i = maxRetries; i > 0; --i) {
            before = time.milliseconds();
            int workerUnsyncBackoffMs =
                    DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT / 10 / i;
            herder.tick();
            assertEquals(before + coordinatorDiscoveryTimeoutMs + workerUnsyncBackoffMs, time.milliseconds());
            coordinatorDiscoveryTimeoutMs = 0;
        }

        ArgumentCaptor<ExtendedAssignment> assignmentCapture = ArgumentCaptor.forClass(ExtendedAssignment.class);
        doNothing().when(member).revokeAssignment(assignmentCapture.capture());

        before = time.milliseconds();
        herder.tick();
        assertEquals(before, time.milliseconds());

        assertEquals(Collections.singleton(CONN1), assignmentCapture.getValue().connectors());
        assertEquals(Collections.singleton(TASK1), assignmentCapture.getValue().tasks());

        // After a complete backoff and a revocation of running tasks rejoin and this time succeed
        // The worker gets back the assignment that had given up
        expectRebalance(Collections.emptyList(), Collections.emptyList(),
                ConnectProtocol.Assignment.NO_ERROR,
                1, "leader", "leaderUrl", singletonList(CONN1), singletonList(TASK1),
                0, true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        herder.tick();

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testAccessors() throws Exception {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);

        expectMemberPoll();

        WorkerConfigTransformer configTransformer = mock(WorkerConfigTransformer.class);

        ClusterConfigState snapshotWithTransform = new ClusterConfigState(
                1,
                null,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet(),
                configTransformer);

        expectConfigRefreshAndSnapshot(snapshotWithTransform);

        // list connectors, get connector info, get connector config, get task configs
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

        // Config transformation should not occur when requesting connector or task info
        verify(configTransformer, never()).transform(eq(CONN1), any());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        when(member.memberId()).thenReturn("leader");
        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        ArgumentCaptor<Callback<TargetState>> onStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), eq(CONN1_CONFIG), any(), eq(herder), eq(TargetState.STARTED), onStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        expectMemberPoll();

        // Should pick up original config
        FutureCallback<Map<String, String>> connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        herder.tick();
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG, connectorConfigCb.get());

        // Poll loop for second round of calls
        expectMemberEnsureActive();

        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN1_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(CONN1_CONFIG_UPDATED), validateCallback.capture());

        doAnswer(invocation -> {
            // Simulate response to writing config + waiting until end of log to be read
            configUpdateListener.onConnectorConfigUpdate(CONN1);
            return null;
        }).when(configBackingStore).putConnectorConfig(eq(CONN1), eq(CONN1_CONFIG_UPDATED), isNull());

        // As a result of reconfig, should need to update snapshot. With only connector updates, we'll just restart
        // connector without rebalance
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT_UPDATED_CONN1_CONFIG);
        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        ArgumentCaptor<Callback<TargetState>> onStart2 = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            onStart2.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), eq(CONN1_CONFIG_UPDATED), any(), eq(herder), eq(TargetState.STARTED), onStart2.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfigUpdated, invocation -> TASK_CONFIGS);

        // Apply new config.
        FutureCallback<Herder.Created<ConnectorInfo>> putConfigCb = new FutureCallback<>();
        herder.putConnectorConfig(CONN1, CONN1_CONFIG_UPDATED, true, putConfigCb);
        herder.tick();
        assertTrue(putConfigCb.isDone());
        ConnectorInfo updatedInfo = new ConnectorInfo(CONN1, CONN1_CONFIG_UPDATED, Arrays.asList(TASK0, TASK1, TASK2),
                ConnectorType.SOURCE);
        assertEquals(new Herder.Created<>(false, updatedInfo), putConfigCb.get());

        // Third tick just to read the config - check config again to validate change
        connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        herder.tick();
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG_UPDATED, connectorConfigCb.get());

        // Once after initial rebalance and assignment; another after config update
        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testPatchConnectorConfigNotFound() {
        when(member.memberId()).thenReturn("leader");
        expectRebalance(0, Collections.emptyList(), Collections.emptyList(), true);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());

        ClusterConfigState clusterConfigState = new ClusterConfigState(
                0,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet());
        expectConfigRefreshAndSnapshot(clusterConfigState);

        Map<String, String> connConfigPatch = new HashMap<>();
        connConfigPatch.put("foo1", "baz1");

        FutureCallback<Herder.Created<ConnectorInfo>> patchCallback = new FutureCallback<>();
        herder.patchConnectorConfig(CONN2, connConfigPatch, patchCallback);
        herder.tick();
        assertTrue(patchCallback.isDone());
        ExecutionException exception = assertThrows(ExecutionException.class, patchCallback::get);
        assertInstanceOf(NotFoundException.class, exception.getCause());
    }

    @Test
    public void testPatchConnectorConfigNotALeader() {
        when(member.memberId()).thenReturn("not-leader");

        // The connector is pre-existing due to the mocks.
        ClusterConfigState originalSnapshot = new ClusterConfigState(
                1,
                null,
                Collections.singletonMap(CONN1, 0),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet());
        expectConfigRefreshAndSnapshot(originalSnapshot);
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        // Patch the connector config.

        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), false);

        FutureCallback<Herder.Created<ConnectorInfo>> patchCallback = new FutureCallback<>();
        herder.patchConnectorConfig(CONN1, new HashMap<>(), patchCallback);
        herder.tick();
        assertTrue(patchCallback.isDone());
        ExecutionException fencingException = assertThrows(ExecutionException.class, patchCallback::get);
        assertInstanceOf(ConnectException.class, fencingException.getCause());
    }

    @Test
    public void testPatchConnectorConfig() throws Exception {
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        when(member.memberId()).thenReturn("leader");
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());

        Map<String, String> originalConnConfig = new HashMap<>(CONN1_CONFIG);
        originalConnConfig.put("foo0", "unaffected");
        originalConnConfig.put("foo1", "will-be-changed");
        originalConnConfig.put("foo2", "will-be-removed");

        // The connector is pre-existing due to the mocks.

        ClusterConfigState originalSnapshot = new ClusterConfigState(
                1,
                null,
                Collections.singletonMap(CONN1, 0),
                Collections.singletonMap(CONN1, originalConnConfig),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet());
        expectConfigRefreshAndSnapshot(originalSnapshot);
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        expectMemberPoll();

        // Patch the connector config.
        Map<String, String> connConfigPatch = new HashMap<>();
        connConfigPatch.put("foo1", "changed");
        connConfigPatch.put("foo2", null);
        connConfigPatch.put("foo3", "added");

        Map<String, String> patchedConnConfig = new HashMap<>(originalConnConfig);
        patchedConnConfig.put("foo0", "unaffected");
        patchedConnConfig.put("foo1", "changed");
        patchedConnConfig.remove("foo2");
        patchedConnConfig.put("foo3", "added");

        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);

        ArgumentCaptor<Callback<ConfigInfos>> validateCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            validateCallback.getValue().onCompletion(null, CONN1_CONFIG_INFOS);
            return null;
        }).when(herder).validateConnectorConfig(eq(patchedConnConfig), validateCallback.capture());

        FutureCallback<Herder.Created<ConnectorInfo>> patchCallback = new FutureCallback<>();
        herder.patchConnectorConfig(CONN1, connConfigPatch, patchCallback);
        herder.tick();
        assertTrue(patchCallback.isDone());
        assertEquals(patchedConnConfig, patchCallback.get().result().config());

        // This is effectively the main check of this test:
        // we validate that what's written in the config storage is the patched config.
        verify(configBackingStore).putConnectorConfig(eq(CONN1), eq(patchedConnConfig), isNull());
        verifyNoMoreInteractions(configBackingStore);

        // No need to check herder.connectorConfig explicitly:
        // all the related parts are mocked and that the config is correct is checked by eq()'s in the mocked called above.
    }

    @Test
    public void testKeyRotationWhenWorkerBecomesLeader() {
        long rotationTtlDelay = DistributedConfig.INTER_WORKER_KEY_TTL_MS_MS_DEFAULT;
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectMemberPoll();

        herder.tick();

        // First rebalance: poll indefinitely as no key has been read yet, so expiration doesn't come into play
        verify(member).poll(eq(Long.MAX_VALUE), any());

        expectRebalance(2, Collections.emptyList(), Collections.emptyList());
        SessionKey initialKey = new SessionKey(mock(SecretKey.class), 0);
        ClusterConfigState snapshotWithKey =  new ClusterConfigState(
                2,
                initialKey,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet());
        expectConfigRefreshAndSnapshot(snapshotWithKey);

        configUpdateListener.onSessionKeyUpdate(initialKey);
        herder.tick();

        // Second rebalance: poll indefinitely as worker is follower, so expiration still doesn't come into play
        verify(member, times(2)).poll(eq(Long.MAX_VALUE), any());

        expectRebalance(2, Collections.emptyList(), Collections.emptyList(), "member", MEMBER_URL, true);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        ArgumentCaptor<SessionKey> updatedKey = ArgumentCaptor.forClass(SessionKey.class);
        doAnswer(invocation -> {
            configUpdateListener.onSessionKeyUpdate(updatedKey.getValue());
            return null;
        }).when(configBackingStore).putSessionKey(updatedKey.capture());

        herder.tick();

        // Third rebalance: poll for a limited time as worker has become leader and must wake up for key expiration
        verify(member).poll(eq(rotationTtlDelay), any());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testKeyRotationDisabledWhenWorkerBecomesFollower() {
        long rotationTtlDelay = DistributedConfig.INTER_WORKER_KEY_TTL_MS_MS_DEFAULT;
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), "member", MEMBER_URL, true);
        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        SecretKey initialSecretKey = mock(SecretKey.class);
        when(initialSecretKey.getAlgorithm()).thenReturn(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
        when(initialSecretKey.getEncoded()).thenReturn(new byte[32]);
        SessionKey initialKey = new SessionKey(initialSecretKey, time.milliseconds());
        ClusterConfigState snapshotWithKey =  new ClusterConfigState(
                1,
                initialKey,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet());
        expectConfigRefreshAndSnapshot(snapshotWithKey);
        expectMemberPoll();

        configUpdateListener.onSessionKeyUpdate(initialKey);
        herder.tick();

        // First rebalance: poll for a limited time as worker is leader and must wake up for key expiration
        verify(member).poll(leq(rotationTtlDelay), any());

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        herder.tick();

        // Second rebalance: poll indefinitely as worker is no longer leader, so key expiration doesn't come into play
        verify(member).poll(eq(Long.MAX_VALUE), any());
        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testPutTaskConfigsSignatureNotRequiredV0() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);

        Callback<Void> taskConfigCb = mock(Callback.class);
        List<String> stages = expectRecordStages(taskConfigCb);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        // Expect a wakeup call after the request to write task configs is added to the herder's request queue
        verify(member).wakeup();
        verifyNoMoreInteractions(member, taskConfigCb);
        assertEquals(
                singletonList("awaiting startup"),
                stages
        );
    }

    @Test
    public void testPutTaskConfigsSignatureNotRequiredV1() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V1);

        Callback<Void> taskConfigCb = mock(Callback.class);
        List<String> stages = expectRecordStages(taskConfigCb);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        // Expect a wakeup call after the request to write task configs is added to the herder's request queue
        verify(member).wakeup();
        verifyNoMoreInteractions(member, taskConfigCb);
        assertEquals(
                singletonList("awaiting startup"),
                stages
        );
    }

    @Test
    public void testPutTaskConfigsMissingRequiredSignature() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        Callback<Void> taskConfigCb = mock(Callback.class);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, null);

        ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(taskConfigCb).onCompletion(errorCapture.capture(), isNull());
        assertInstanceOf(BadRequestException.class, errorCapture.getValue());

        verifyNoMoreInteractions(member, taskConfigCb);
    }

    @Test
    public void testPutTaskConfigsDisallowedSignatureAlgorithm() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        InternalRequestSignature signature = mock(InternalRequestSignature.class);
        when(signature.keyAlgorithm()).thenReturn("HmacSHA489");

        Callback<Void> taskConfigCb = mock(Callback.class);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(taskConfigCb).onCompletion(errorCapture.capture(), isNull());
        assertInstanceOf(BadRequestException.class, errorCapture.getValue());

        verifyNoMoreInteractions(member, taskConfigCb);
    }

    @Test
    public void testPutTaskConfigsInvalidSignature() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        InternalRequestSignature signature = mock(InternalRequestSignature.class);
        when(signature.keyAlgorithm()).thenReturn("HmacSHA256");
        when(signature.isValid(any())).thenReturn(false);

        SessionKey sessionKey = mock(SessionKey.class);
        SecretKey secretKey = mock(SecretKey.class);
        when(sessionKey.key()).thenReturn(secretKey);
        when(sessionKey.creationTimestamp()).thenReturn(time.milliseconds());

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        Callback<Void> taskConfigCb = mock(Callback.class);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(taskConfigCb).onCompletion(errorCapture.capture(), isNull());
        assertInstanceOf(ConnectRestException.class, errorCapture.getValue());
        assertEquals(FORBIDDEN.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());

        verifyNoMoreInteractions(member, taskConfigCb);
    }

    @Test
    public void putTaskConfigsWorkerStillStarting() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        InternalRequestSignature signature = mock(InternalRequestSignature.class);
        when(signature.keyAlgorithm()).thenReturn("HmacSHA256");

        Callback<Void> taskConfigCb = mock(Callback.class);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(taskConfigCb).onCompletion(errorCapture.capture(), isNull());
        assertInstanceOf(ConnectRestException.class, errorCapture.getValue());
        assertEquals(SERVICE_UNAVAILABLE.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());

        verifyNoMoreInteractions(member, taskConfigCb);
    }

    @Test
    public void testPutTaskConfigsValidRequiredSignature() {
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        InternalRequestSignature signature = mock(InternalRequestSignature.class);
        when(signature.keyAlgorithm()).thenReturn("HmacSHA256");
        when(signature.isValid(any())).thenReturn(true);

        SessionKey sessionKey = mock(SessionKey.class);
        SecretKey secretKey = mock(SecretKey.class);
        when(sessionKey.key()).thenReturn(secretKey);
        when(sessionKey.creationTimestamp()).thenReturn(time.milliseconds());

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        Callback<Void> taskConfigCb = mock(Callback.class);
        List<String> stages = expectRecordStages(taskConfigCb);
        herder.putTaskConfigs(CONN1, TASK_CONFIGS, taskConfigCb, signature);

        // Expect a wakeup call after the request to write task configs is added to the herder's request queue
        verify(member).wakeup();
        verifyNoMoreInteractions(member, taskConfigCb);

        assertEquals(
                singletonList("awaiting startup"),
                stages
        );
    }

    @Test
    public void testFailedToWriteSessionKey() {
        // First tick -- after joining the group, we try to write a new
        // session key to the config topic, and fail
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        doThrow(new ConnectException("Oh no!")).when(configBackingStore).putSessionKey(any(SessionKey.class));
        herder.tick();

        // Second tick -- we read to the end of the config topic first,
        // then ensure we're still active in the group
        // then try a second time to write a new session key,
        // then finally begin polling for group activity
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        doNothing().when(configBackingStore).putSessionKey(any(SessionKey.class));

        herder.tick();

        verify(member, times(2)).ensureActive(any());
        verify(member, times(1)).poll(anyLong(), any());
        verify(configBackingStore, times(2)).putSessionKey(any(SessionKey.class));
    }

    @Test
    public void testFailedToReadBackNewlyWrittenSessionKey() throws Exception {
        SecretKey secretKey = mock(SecretKey.class);
        when(secretKey.getAlgorithm()).thenReturn(INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
        when(secretKey.getEncoded()).thenReturn(new byte[32]);
        SessionKey sessionKey = new SessionKey(secretKey, time.milliseconds());
        ClusterConfigState snapshotWithSessionKey = new ClusterConfigState(
                1,
                sessionKey,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet());

        // First tick -- after joining the group, we try to write a new session key to
        // the config topic, and fail (in this case, we're trying to simulate that we've
        // actually written the key successfully, but haven't been able to read it back
        // from the config topic, so to the herder it looks the same as if it'd just failed
        // to write the key)
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        doThrow(new ConnectException("Oh no!")).when(configBackingStore).putSessionKey(any(SessionKey.class));

        herder.tick();

        // Second tick -- we read to the end of the config topic first, and pick up
        // the session key that we were able to write the last time,
        // then ensure we're still active in the group
        // then finally begin polling for group activity
        // Importantly, we do not try to write a new session key this time around
        doAnswer(invocation -> {
            configUpdateListener.onSessionKeyUpdate(sessionKey);
            return null;
        }).when(configBackingStore).refresh(anyLong(), any(TimeUnit.class));
        when(configBackingStore.snapshot()).thenReturn(snapshotWithSessionKey);

        herder.tick();

        verify(member, times(2)).ensureActive(any());
        verify(member, times(1)).poll(anyLong(), any());
        verify(configBackingStore, times(1)).putSessionKey(any(SessionKey.class));
    }

    @Test
    public void testFenceZombiesInvalidSignature() {
        // Don't have to run the whole gamut of scenarios (invalid signature, missing signature, earlier protocol that doesn't require signatures)
        // since the task config tests cover that pretty well. One sanity check to ensure that this method is guarded should be sufficient.

        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);

        InternalRequestSignature signature = mock(InternalRequestSignature.class);
        when(signature.keyAlgorithm()).thenReturn("HmacSHA256");
        when(signature.isValid(any())).thenReturn(false);

        SessionKey sessionKey = mock(SessionKey.class);
        SecretKey secretKey = mock(SecretKey.class);
        when(sessionKey.key()).thenReturn(secretKey);
        when(sessionKey.creationTimestamp()).thenReturn(time.milliseconds());

        // Read a new session key from the config topic
        configUpdateListener.onSessionKeyUpdate(sessionKey);

        Callback<Void> taskConfigCb = mock(Callback.class);
        herder.fenceZombieSourceTasks(CONN1, taskConfigCb, signature);

        ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(taskConfigCb).onCompletion(errorCapture.capture(), isNull());
        assertInstanceOf(ConnectRestException.class, errorCapture.getValue());
        assertEquals(FORBIDDEN.getStatusCode(), ((ConnectRestException) errorCapture.getValue()).statusCode());

        verifyNoMoreInteractions(member);
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
        ExecutorService forwardRequestExecutor = mock(ExecutorService.class);
        herder.forwardRequestExecutor = forwardRequestExecutor;

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList());
        expectMemberPoll();

        doAnswer(invocation -> {
            if (!succeed) {
                throw new ConnectRestException(409, "Rebalance :(");
            }
            return null;
        }).when(restClient).httpRequest(
                any(), eq("PUT"), isNull(), isNull(), any(), any()
        );

        ArgumentCaptor<Runnable> forwardRequest = ArgumentCaptor.forClass(Runnable.class);

        doAnswer(invocation -> {
            forwardRequest.getValue().run();
            return null;
        }).when(forwardRequestExecutor).execute(forwardRequest.capture());

        expectHerderShutdown();
        doNothing().when(forwardRequestExecutor).shutdown();
        when(forwardRequestExecutor.awaitTermination(anyLong(), any())).thenReturn(true);

        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(TASK1, fencing);

        if (!succeed) {
            ExecutionException fencingException =
                    assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
            assertInstanceOf(ConnectException.class, fencingException.getCause());
        } else {
            fencing.get(10, TimeUnit.SECONDS);
        }

        stopBackgroundHerder();

        verifyNoMoreInteractions(member, worker, forwardRequestExecutor);
    }

    @Test
    public void testExternalZombieFencingRequestForAlreadyFencedConnector() throws Exception {
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
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
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
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
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
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

        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        expectConfigRefreshAndSnapshot(configState);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        if (expectTaskCountRecord) {
            doNothing().when(configBackingStore).putTaskCountRecord(CONN1, 1);
        }

        expectHerderShutdown();

        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        fencing.get(10, TimeUnit.SECONDS);

        stopBackgroundHerder();

        verifyNoMoreInteractions(member, worker, configBackingStore, statusBackingStore);
    }

    /**
     * Tests zombie fencing that completes extremely quickly, and causes all callback-related logic to be invoked
     * effectively as soon as it's put into place. This is not likely to occur in practice, but the test is valuable all the
     * same especially since it may shed light on potential deadlocks when the unlikely-but-not-impossible happens.
     */
    @Test
    public void testExternalZombieFencingRequestImmediateCompletion() throws Exception {
        expectHerderStartup();
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // The future returned by Worker::fenceZombies
        KafkaFuture<Void> workerFencingFuture = mock(KafkaFuture.class);
        // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic)
        KafkaFuture<Void> herderFencingFuture = mock(KafkaFuture.class);

        // Immediately invoke callbacks that the herder sets up for when the worker fencing and writes to the config topic have completed
        ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = ArgumentCaptor.forClass(KafkaFuture.BiConsumer.class);
        when(herderFencingFuture.whenComplete(herderFencingCallback.capture())).thenAnswer(invocation -> {
            herderFencingCallback.getValue().accept(null, null);
            return null;
        });

        ArgumentCaptor<KafkaFuture.BaseFunction<Void, Void>> fencingFollowup = ArgumentCaptor.forClass(KafkaFuture.BaseFunction.class);
        when(workerFencingFuture.thenApply(fencingFollowup.capture())).thenAnswer(invocation -> {
            fencingFollowup.getValue().apply(null);
            return herderFencingFuture;
        });
        when(worker.fenceZombies(eq(CONN1), eq(2), eq(CONN1_CONFIG))).thenReturn(workerFencingFuture);

        doNothing().when(configBackingStore).putTaskCountRecord(CONN1, 1);

        expectHerderShutdown();

        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        fencing.get(10, TimeUnit.SECONDS);

        stopBackgroundHerder();

        verifyNoMoreInteractions(herderFencingFuture, workerFencingFuture, member, worker, configBackingStore, statusBackingStore);
    }

    /**
     * The herder tries to perform a round of fencing, but fails synchronously while invoking Worker::fenceZombies
     */
    @Test
    public void testExternalZombieFencingRequestSynchronousFailure() throws Exception {
        expectHerderStartup();
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        Exception fencingException = new KafkaException("whoops!");
        when(worker.fenceZombies(eq(CONN1), eq(2), eq(CONN1_CONFIG))).thenThrow(fencingException);

        expectHerderShutdown();

        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
        assertEquals(fencingException, exception.getCause());

        stopBackgroundHerder();

        verifyNoMoreInteractions(member, worker, configBackingStore, statusBackingStore);
    }

    /**
     * The herder tries to perform a round of fencing and is able to retrieve a future from worker::fenceZombies, but the attempt
     * fails at a later point.
     */
    @Test
    public void testExternalZombieFencingRequestAsynchronousFailure() throws Exception {
        expectHerderStartup();
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

        ClusterConfigState configState = exactlyOnceSnapshot(
                sessionKey,
                TASK_CONFIGS_MAP,
                Collections.singletonMap(CONN1, 2),
                Collections.singletonMap(CONN1, 5),
                Collections.singleton(CONN1)
        );
        expectConfigRefreshAndSnapshot(configState);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // The future returned by Worker::fenceZombies
        KafkaFuture<Void> workerFencingFuture = mock(KafkaFuture.class);
        // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic)
        KafkaFuture<Void> herderFencingFuture = mock(KafkaFuture.class);
        // The callbacks that the herder has accrued for outstanding fencing futures
        ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallbacks = ArgumentCaptor.forClass(KafkaFuture.BiConsumer.class);

        when(worker.fenceZombies(eq(CONN1), eq(2), eq(CONN1_CONFIG))).thenReturn(workerFencingFuture);

        when(workerFencingFuture.thenApply(any(KafkaFuture.BaseFunction.class))).thenReturn(herderFencingFuture);

        CountDownLatch callbacksInstalled = new CountDownLatch(2);
        when(herderFencingFuture.whenComplete(herderFencingCallbacks.capture())).thenAnswer(invocation -> {
            callbacksInstalled.countDown();
            return null;
        });

        expectHerderShutdown();

        startBackgroundHerder();

        FutureCallback<Void> fencing = new FutureCallback<>();
        herder.fenceZombieSourceTasks(CONN1, fencing);

        assertTrue(callbacksInstalled.await(10, TimeUnit.SECONDS));

        Exception fencingException = new AuthorizationException("you didn't say the magic word");
        herderFencingCallbacks.getAllValues().forEach(cb -> cb.accept(null, fencingException));

        ExecutionException exception = assertThrows(ExecutionException.class, () -> fencing.get(10, TimeUnit.SECONDS));
        assertInstanceOf(ConnectException.class, exception.getCause());

        stopBackgroundHerder();

        verifyNoMoreInteractions(herderFencingFuture, workerFencingFuture, member, worker, configBackingStore, statusBackingStore);
    }

    /**
     * Issues multiple rapid fencing requests for a handful of connectors, each of which takes a little while to complete.
     * This mimics what might happen when a few connectors are reconfigured in quick succession and each task for the
     * connector needs to hit the leader with a fencing request during its preflight check.
     */
    @Test
    public void testExternalZombieFencingRequestDelayedCompletion() throws Exception {
        final String conn3 = "sourceC";
        final Map<String, Integer> tasksPerConnector = new HashMap<>();
        tasksPerConnector.put(CONN1, 5);
        tasksPerConnector.put(CONN2, 3);
        tasksPerConnector.put(conn3, 12);

        expectHerderStartup();
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V2);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);

        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        SessionKey sessionKey = expectNewSessionKey();

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
        expectConfigRefreshAndSnapshot(configState);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        // The callbacks that the herder has accrued for outstanding fencing futures, which will be completed after
        // a successful round of fencing and a task record write to the config topic
        Map<String, ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>>> herderFencingCallbacks = new HashMap<>();
        // The callbacks that the herder has installed for after a successful round of zombie fencing, but before writing
        // a task record to the config topic
        Map<String, ArgumentCaptor<KafkaFuture.BaseFunction<Void, Void>>> workerFencingFollowups = new HashMap<>();

        Map<String, CountDownLatch> callbacksInstalled = new HashMap<>();
        tasksPerConnector.keySet().forEach(connector -> {
            // The future returned by Worker::fenceZombies
            KafkaFuture<Void> workerFencingFuture = mock(KafkaFuture.class);
            // The future tracked by the herder (which tracks the fencing performed by the worker and the possible followup write to the config topic)
            KafkaFuture<Void> herderFencingFuture = mock(KafkaFuture.class);

            ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = ArgumentCaptor.forClass(KafkaFuture.BiConsumer.class);
            herderFencingCallbacks.put(connector, herderFencingCallback);

            // Don't immediately invoke callbacks that the herder sets up for when the worker fencing and writes to the config topic have completed
            // Instead, wait for them to be installed, then invoke them explicitly after the fact on a thread separate from the herder's tick thread
            when(herderFencingFuture.whenComplete(herderFencingCallback.capture())).thenReturn(null);

            ArgumentCaptor<KafkaFuture.BaseFunction<Void, Void>> fencingFollowup = ArgumentCaptor.forClass(KafkaFuture.BaseFunction.class);
            CountDownLatch callbackInstalled = new CountDownLatch(1);
            workerFencingFollowups.put(connector, fencingFollowup);
            callbacksInstalled.put(connector, callbackInstalled);
            when(workerFencingFuture.thenApply(fencingFollowup.capture())).thenAnswer(invocation -> {
                callbackInstalled.countDown();
                return herderFencingFuture;
            });

            when(worker.fenceZombies(eq(connector), eq(taskCountRecords.get(connector)), any())).thenReturn(workerFencingFuture);
        });

        tasksPerConnector.forEach((connector, taskCount) -> doNothing().when(configBackingStore).putTaskCountRecord(eq(connector), eq(taskCount)));

        expectHerderShutdown();

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
                herderFencingCallbacks.get(connector).getAllValues().forEach(cb -> cb.accept(null, null));
            } catch (InterruptedException e) {
                fail("Unexpectedly interrupted");
            }
        });

        for (FutureCallback<Void> fencing : stackedFencingRequests) {
            fencing.get(10, TimeUnit.SECONDS);
        }

        stopBackgroundHerder();

        // We should only perform a single physical zombie fencing for each connector; all the subsequent requests should be stacked onto the first one
        tasksPerConnector.keySet().forEach(connector -> verify(worker).fenceZombies(eq(connector), eq(taskCountRecords.get(connector)), any()));
        verifyNoMoreInteractions(member, worker, configBackingStore, statusBackingStore);
    }

    @Test
    public void testVerifyTaskGeneration() {
        Map<String, Integer> taskConfigGenerations = new HashMap<>();
        herder.configState = new ClusterConfigState(
                1,
                null,
                Collections.singletonMap(CONN1, 3),
                Collections.singletonMap(CONN1, CONN1_CONFIG),
                Collections.singletonMap(CONN1, TargetState.STARTED),
                TASK_CONFIGS_MAP,
                Collections.emptyMap(),
                taskConfigGenerations,
                Collections.singletonMap(CONN1, new AppliedConnectorConfig(CONN1_CONFIG)),
                Collections.emptySet(),
                Collections.emptySet());

        Callback<Void> verifyCallback = mock(Callback.class);

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

        verify(verifyCallback, times(3)).onCompletion(isNull(), isNull());
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
        assertTrue(((ThreadPoolExecutor) herder.herderExecutor).getThreadFactory().newThread(EMPTY_RUNNABLE).getName()
                .startsWith(DistributedHerder.class.getSimpleName()));

        assertTrue(((ThreadPoolExecutor) herder.forwardRequestExecutor).getThreadFactory().newThread(EMPTY_RUNNABLE).getName()
                .startsWith("ForwardRequestExecutor"));

        assertTrue(((ThreadPoolExecutor) herder.startAndStopExecutor).getThreadFactory().newThread(EMPTY_RUNNABLE).getName()
                .startsWith("StartAndStopExecutor"));
    }

    @Test
    public void testHerderStopServicesClosesUponShutdown() {
        assertEquals(1, shutdownCalled.getCount());
        herder.stopServices();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
        assertEquals(0, shutdownCalled.getCount());
    }

    @Test
    public void testPollDurationOnSlowConnectorOperations() {
        connectProtocolVersion = CONNECT_PROTOCOL_V1;
        // If an operation during tick() takes some amount of time, that time should count against the rebalance delay
        final int rebalanceDelayMs = 20000;
        final long operationDelayMs = 10000;
        final long maxPollWaitMs = rebalanceDelayMs - operationDelayMs;
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(connectProtocolVersion);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);

        // Assign the connector to this worker, and have it start
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        expectConfigRefreshAndSnapshot(SNAPSHOT);
        ArgumentCaptor<Callback<TargetState>> onFirstStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            time.sleep(operationDelayMs);
            onFirstStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), eq(CONN1_CONFIG), any(), eq(herder), eq(TargetState.STARTED), onFirstStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfig, invocation -> TASK_CONFIGS);

        herder.tick();

        // Rebalance again due to config update
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT_UPDATED_CONN1_CONFIG);
        doNothing().when(worker).stopAndAwaitConnector(CONN1);

        ArgumentCaptor<Callback<TargetState>> onSecondStart = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            time.sleep(operationDelayMs);
            onSecondStart.getValue().onCompletion(null, TargetState.STARTED);
            return true;
        }).when(worker).startConnector(eq(CONN1), eq(CONN1_CONFIG_UPDATED), any(), eq(herder), eq(TargetState.STARTED), onSecondStart.capture());
        expectExecuteTaskReconfiguration(true, conn1SinkConfigUpdated, invocation -> TASK_CONFIGS);

        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick();

        // Third tick should resolve all outstanding requests
        expectRebalance(Collections.emptyList(), Collections.emptyList(), ConnectProtocol.Assignment.NO_ERROR, 1, singletonList(CONN1), Collections.emptyList(), rebalanceDelayMs);
        // which includes querying the connector task configs after the update
        expectExecuteTaskReconfiguration(true, conn1SinkConfigUpdated, invocation -> {
            time.sleep(operationDelayMs);
            return TASK_CONFIGS;
        });
        herder.tick();

        // We should poll for less than the delay - time to start the connector, meaning that a long connector start
        // does not delay the poll timeout
        verify(member, times(3)).poll(leq(maxPollWaitMs), any());
        verify(worker, times(2)).startConnector(eq(CONN1), any(), any(), eq(herder), eq(TargetState.STARTED), any());
        verifyNoMoreInteractions(member, worker, configBackingStore);
    }

    @Test
    public void shouldThrowWhenStartAndStopExecutorThrowsRejectedExecutionExceptionAndHerderNotStopping() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, singletonList(CONN1), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        herder.startAndStopExecutor.shutdown();
        assertThrows(RejectedExecutionException.class, herder::tick);
    }

    @Test
    public void testTaskReconfigurationRetriesWithConnectorTaskConfigsException() {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(worker.isRunning(CONN1)).thenReturn(true);
        when(worker.getPlugins()).thenReturn(plugins);

        SinkConnectorConfig sinkConnectorConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);

        when(worker.connectorTaskConfigs(CONN1, sinkConnectorConfig))
                .thenThrow(new ConnectException("Failed to generate task configs"))
                .thenThrow(new ConnectException("Failed to generate task configs"))
                .thenReturn(TASK_CONFIGS);

        expectAndVerifyTaskReconfigurationRetries();
    }

    @Test
    public void testTaskReconfigurationNoRetryWithTooManyTasks() {
        // initial tick
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(worker.isRunning(CONN1)).thenReturn(true);
        when(worker.getPlugins()).thenReturn(plugins);

        herder.tick();
        // No requests are queued, so we shouldn't plan on waking up without external action
        // (i.e., rebalance, user request, or shutdown)
        // This helps indicate that no retriable operations (such as generating task configs after
        // a backoff period) are queued up by the worker
        verify(member, times(1)).poll(eq(Long.MAX_VALUE), any());

        // Process the task reconfiguration request in this tick
        int numTasks = MAX_TASKS + 5;
        SinkConnectorConfig sinkConnectorConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);
        // Fail to generate tasks because the connector provided too many task configs
        when(worker.connectorTaskConfigs(CONN1, sinkConnectorConfig))
                .thenThrow(new TooManyTasksException(CONN1, numTasks, MAX_TASKS));

        herder.requestTaskReconfiguration(CONN1);
        herder.tick();
        // We tried to generate task configs for the connector one time during this tick
        verify(worker, times(1)).connectorTaskConfigs(CONN1, sinkConnectorConfig);
        // Verifying again that no requests are queued
        verify(member, times(2)).poll(eq(Long.MAX_VALUE), any());
        verifyNoMoreInteractions(worker);

        time.sleep(DistributedHerder.RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MAX_MS);
        herder.tick();
        // We ticked one more time, and no further attempt was made to generate task configs
        verifyNoMoreInteractions(worker);
        // And we don't have any requests queued
        verify(member, times(3)).poll(eq(Long.MAX_VALUE), any());
    }

    @Test
    public void testTaskReconfigurationRetriesWithLeaderRequestForwardingException() {
        herder = mock(DistributedHerder.class, withSettings().defaultAnswer(CALLS_REAL_METHODS).useConstructor(new DistributedConfig(HERDER_CONFIG),
                worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, configBackingStore, member, MEMBER_URL, restClient, metrics, time,
                noneConnectorClientConfigOverridePolicy, Collections.emptyList(), new MockSynchronousExecutor(), new AutoCloseable[]{}));

        rebalanceListener = herder.new RebalanceListener(time);

        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(worker.isSinkConnector(CONN1)).thenReturn(Boolean.TRUE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), false);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(worker.isRunning(CONN1)).thenReturn(true);
        when(worker.getPlugins()).thenReturn(plugins);

        SinkConnectorConfig sinkConnectorConfig = new SinkConnectorConfig(plugins, CONN1_CONFIG);

        List<Map<String, String>> changedTaskConfigs = new ArrayList<>(TASK_CONFIGS);
        changedTaskConfigs.add(TASK_CONFIG);
        when(worker.connectorTaskConfigs(CONN1, sinkConnectorConfig)).thenReturn(changedTaskConfigs);

        doThrow(new ConnectException("Request to leader to reconfigure connector tasks failed"))
                .doThrow(new ConnectException("Request to leader to reconfigure connector tasks failed"))
                .doNothing()
                .when(restClient).httpRequest(any(), eq("POST"), any(), any(), any(), any());

        expectAndVerifyTaskReconfigurationRetries();
    }

    private void expectAndVerifyTaskReconfigurationRetries() {
        // initial tick
        herder.tick();
        herder.requestTaskReconfiguration(CONN1);
        // process the task reconfiguration request in this tick
        herder.tick();
        // advance the time by 250ms so that the task reconfiguration request with initial retry backoff is processed
        time.sleep(250);
        herder.tick();
        // advance the time by 500ms so that the task reconfiguration request with double the initial retry backoff is processed
        time.sleep(500);
        herder.tick();

        // 1. end of initial tick when no request has been added to the herder queue yet
        // 2. the third task reconfiguration request is expected to pass; so expect no more retries (a Long.MAX_VALUE poll
        //    timeout indicates that there is no herder request currently in the queue)
        verify(member, times(2)).poll(eq(Long.MAX_VALUE), any());

        // task reconfiguration herder request with initial retry backoff
        verify(member).poll(eq(250L), any());

        // task reconfiguration herder request with double the initial retry backoff
        verify(member).poll(eq(500L), any());

        verifyNoMoreInteractions(member, worker, restClient);
    }

    @Test
    public void processRestartRequestsFailureSuppression() {
        doNothing().when(member).wakeup();

        final String connectorName = "foo";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, false);
        doThrow(new RuntimeException()).when(herder).buildRestartPlan(restartRequest);

        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        herder.processRestartRequests();
        assertTrue(herder.pendingRestartRequests.isEmpty());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void processRestartRequestsDequeue() {
        doNothing().when(member).wakeup();
        doReturn(Optional.empty()).when(herder).buildRestartPlan(any(RestartRequest.class));

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
        doNothing().when(member).wakeup();

        final String connectorName = "foo";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, false);
        configUpdateListener.onRestartRequest(restartRequest);

        // Will overwrite as this is higher impact
        restartRequest = new RestartRequest(connectorName, false, true);
        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        assertFalse(herder.pendingRestartRequests.get(connectorName).onlyFailed());
        assertTrue(herder.pendingRestartRequests.get(connectorName).includeTasks());

        // Will be ignored as the existing request has higher impact
        restartRequest = new RestartRequest(connectorName, true, false);
        configUpdateListener.onRestartRequest(restartRequest);
        assertEquals(1, herder.pendingRestartRequests.size());
        // Compare against existing request
        assertFalse(herder.pendingRestartRequests.get(connectorName).onlyFailed());
        assertTrue(herder.pendingRestartRequests.get(connectorName).includeTasks());

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testExactlyOnceSourceSupportValidation() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.exactlyOnceSupport(eq(config))).thenReturn(ExactlyOnceSupport.SUPPORTED);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(Collections.emptyList(), errors);
    }

    @Test
    public void testExactlyOnceSourceSupportValidationOnUnsupportedConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.exactlyOnceSupport(eq(config))).thenReturn(ExactlyOnceSupport.UNSUPPORTED);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(
                Collections.singletonList("The connector does not support exactly-once semantics with the provided configuration."),
                errors);
    }

    @Test
    public void testExactlyOnceSourceSupportValidationOnUnknownConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.exactlyOnceSupport(eq(config))).thenReturn(null);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains("The connector does not implement the API required for preflight validation of exactly-once source support."),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testExactlyOnceSourceSupportValidationHandlesConnectorErrorsGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        String errorMessage = "time to add a new unit test :)";
        when(connectorMock.exactlyOnceSupport(eq(config))).thenThrow(new NullPointerException(errorMessage));

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains(errorMessage),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testExactlyOnceSourceSupportValidationWhenExactlyOnceNotEnabledOnWorker() {
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, REQUIRED.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.exactlyOnceSupport(eq(config))).thenReturn(ExactlyOnceSupport.SUPPORTED);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertEquals(
                Collections.singletonList("This worker does not have exactly-once source support enabled."),
                errors);
    }

    @Test
    public void testExactlyOnceSourceSupportValidationHandlesInvalidValuesGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG, "invalid");

        SourceConnector connectorMock = mock(SourceConnector.class);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains("String must be one of (case insensitive): "),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testConnectorTransactionBoundaryValidation() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.canDefineTransactionBoundaries(eq(config)))
                .thenReturn(ConnectorTransactionBoundaries.SUPPORTED);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertEquals(Collections.emptyList(), errors);
    }

    @Test
    public void testConnectorTransactionBoundaryValidationOnUnsupportedConnector() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        when(connectorMock.canDefineTransactionBoundaries(eq(config)))
                .thenReturn(ConnectorTransactionBoundaries.UNSUPPORTED);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains("The connector does not support connector-defined transaction boundaries with the given configuration."),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testConnectorTransactionBoundaryValidationHandlesConnectorErrorsGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        SourceConnector connectorMock = mock(SourceConnector.class);
        String errorMessage = "Wait I thought we tested for this?";
        when(connectorMock.canDefineTransactionBoundaries(eq(config))).thenThrow(new ConnectException(errorMessage));

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains(errorMessage),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testConnectorTransactionBoundaryValidationHandlesInvalidValuesGracefully() {
        herder = exactlyOnceHerder();
        Map<String, String> config = new HashMap<>();
        config.put(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG, "CONNECTOR.toString()");

        SourceConnector connectorMock = mock(SourceConnector.class);

        Map<String, ConfigValue> validatedConfigs = herder.validateSourceConnectorConfig(
                connectorMock, SourceConnectorConfig.configDef(), config);

        List<String> errors = validatedConfigs.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG).errorMessages();
        assertFalse(errors.isEmpty());
        assertTrue(
            errors.get(0).contains("String must be one of (case insensitive): "),
            "Error message did not contain expected text: " + errors.get(0));
        assertEquals(1, errors.size());
    }

    @Test
    public void testConnectorOffsets() throws Exception {
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        when(statusBackingStore.connectors()).thenReturn(Collections.emptySet());
        expectMemberPoll();

        herder.tick();

        when(configBackingStore.snapshot()).thenReturn(SNAPSHOT);
        ConnectorOffsets offsets = new ConnectorOffsets(Collections.singletonList(new ConnectorOffset(
                Collections.singletonMap("partitionKey", "partitionValue"),
                Collections.singletonMap("offsetKey", "offsetValue"))));

        ArgumentCaptor<Callback<ConnectorOffsets>> callbackCapture = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            callbackCapture.getValue().onCompletion(null, offsets);
            return null;
        }).when(worker).connectorOffsets(eq(CONN1), eq(CONN1_CONFIG), callbackCapture.capture());

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        herder.connectorOffsets(CONN1, cb);
        herder.tick();
        assertEquals(offsets, cb.get(1000, TimeUnit.MILLISECONDS));

        verifyNoMoreInteractions(worker, member, configBackingStore, statusBackingStore);
    }

    @Test
    public void testModifyConnectorOffsetsUnknownConnector() {
        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        herder.tick();

        // Now handle the connector offsets modification request
        FutureCallback<Message> callback = new FutureCallback<>();
        herder.modifyConnectorOffsets("connector-does-not-exist", new HashMap<>(), callback);
        herder.tick();
        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotFoundException.class, e.getCause());
    }

    @Test
    public void testModifyOffsetsConnectorNotInStoppedState() {
        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT);

        herder.tick();

        // Now handle the connector offsets modification request
        FutureCallback<Message> callback = new FutureCallback<>();
        herder.modifyConnectorOffsets(CONN1, null, callback);
        herder.tick();
        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(BadRequestException.class, e.getCause());
    }

    @Test
    public void testModifyOffsetsNotLeader() {
        // Get the initial assignment
        when(member.memberId()).thenReturn("member");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), false);
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1);

        herder.tick();

        // Now handle the connector offsets modification request
        FutureCallback<Message> callback = new FutureCallback<>();
        herder.modifyConnectorOffsets(CONN1, new HashMap<>(), callback);
        herder.tick();
        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertInstanceOf(NotLeaderException.class, e.getCause());
    }

    @Test
    public void testModifyOffsetsSinkConnector() throws Exception {
        when(herder.connectorType(any())).thenReturn(ConnectorType.SINK);
        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1);

        herder.tick();

        // Now handle the alter connector offsets request
        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap("partitionKey", "partitionValue"),
                Collections.singletonMap("offsetKey", "offsetValue"));

        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been altered successfully");
        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONN1), eq(CONN1_CONFIG), eq(offsets), workerCallbackCapture.capture());

        FutureCallback<Message> callback = new FutureCallback<>();
        herder.alterConnectorOffsets(CONN1, offsets, callback);
        herder.tick();
        assertEquals(msg, callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals("The offsets for this connector have been altered successfully", msg.message());
    }

    @Test
    public void testModifyOffsetsSourceConnectorExactlyOnceDisabled() throws Exception {
        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        when(herder.connectorType(anyMap())).thenReturn(ConnectorType.SOURCE);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1);
        herder.tick();

        // Now handle the reset connector offsets request
        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been reset successfully");
        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONN1), eq(CONN1_CONFIG), isNull(), workerCallbackCapture.capture());

        FutureCallback<Message> callback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONN1, callback);
        herder.tick();
        assertEquals(msg, callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals("The offsets for this connector have been reset successfully", msg.message());
    }

    @Test
    public void testModifyOffsetsSourceConnectorExactlyOnceEnabled() throws Exception {
        // Setup herder with exactly-once support for source connectors enabled
        herder = exactlyOnceHerder();
        rebalanceListener = herder.new RebalanceListener(time);
        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1);
        expectMemberPoll();

        herder.tick();

        // Now handle the alter connector offsets request
        expectMemberEnsureActive();
        when(herder.connectorType(any())).thenReturn(ConnectorType.SOURCE);

        // Expect a round of zombie fencing to occur
        KafkaFuture<Void> workerFencingFuture = mock(KafkaFuture.class);
        KafkaFuture<Void> herderFencingFuture = mock(KafkaFuture.class);
        when(worker.fenceZombies(CONN1, SNAPSHOT_STOPPED_CONN1.taskCountRecord(CONN1), CONN1_CONFIG)).thenReturn(workerFencingFuture);
        when(workerFencingFuture.thenApply(any(KafkaFuture.BaseFunction.class))).thenReturn(herderFencingFuture);

        ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = ArgumentCaptor.forClass(KafkaFuture.BiConsumer.class);
        when(herderFencingFuture.whenComplete(herderFencingCallback.capture())).thenAnswer(invocation -> {
            herderFencingCallback.getValue().accept(null, null);
            return null;
        });

        ArgumentCaptor<Callback<Message>> workerCallbackCapture = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been altered successfully");

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap("partitionKey", "partitionValue"),
                Collections.singletonMap("offsetKey", "offsetValue"));
        doAnswer(invocation -> {
            workerCallbackCapture.getValue().onCompletion(null, msg);
            return null;
        }).when(worker).modifyConnectorOffsets(eq(CONN1), eq(CONN1_CONFIG), eq(offsets), workerCallbackCapture.capture());

        FutureCallback<Message> callback = new FutureCallback<>();
        herder.alterConnectorOffsets(CONN1, offsets, callback);
        // Process the zombie fencing request that is queued up first followed by the actual alter offsets request
        herder.tick();
        assertEquals(msg, callback.get(1000L, TimeUnit.MILLISECONDS));

        // Handle the second alter connector offsets request
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1_FENCED);
        FutureCallback<Message> callback2 = new FutureCallback<>();
        herder.alterConnectorOffsets(CONN1, offsets, callback2);
        herder.tick();
        assertEquals(msg, callback2.get(1000L, TimeUnit.MILLISECONDS));

        // Two fencing callbacks are added - one is in ZombieFencing::start itself to remove the connector from the active
        // fencing list. The other is the callback passed from DistributedHerder::modifyConnectorOffsets in order to
        // queue up the actual alter offsets request if the zombie fencing succeeds.
        verify(herderFencingFuture, times(2)).whenComplete(any());

        // No zombie fencing request to the worker is expected in the second alter connector offsets request since we already
        // did a round of zombie fencing the first time and no new tasks came up in the meanwhile.
        verify(worker, times(1)).fenceZombies(eq(CONN1), eq(SNAPSHOT_STOPPED_CONN1.taskCountRecord(CONN1)), eq(CONN1_CONFIG));
        verifyNoMoreInteractions(workerFencingFuture, herderFencingFuture, member, worker);
    }

    @Test
    public void testModifyOffsetsSourceConnectorExactlyOnceEnabledZombieFencingFailure() {
        // Setup herder with exactly-once support for source connectors enabled
        herder = exactlyOnceHerder();
        rebalanceListener = herder.new RebalanceListener(time);

        // Get the initial assignment
        when(member.memberId()).thenReturn("leader");
        when(member.currentProtocolVersion()).thenReturn(CONNECT_PROTOCOL_V0);
        expectRebalance(1, Collections.emptyList(), Collections.emptyList(), true);
        expectConfigRefreshAndSnapshot(SNAPSHOT_STOPPED_CONN1);
        expectMemberPoll();

        herder.tick();

        // Now handle the reset connector offsets request
        expectMemberEnsureActive();
        when(herder.connectorType(any())).thenReturn(ConnectorType.SOURCE);

        // Expect a round of zombie fencing to occur
        KafkaFuture<Void> workerFencingFuture = mock(KafkaFuture.class);
        KafkaFuture<Void> herderFencingFuture = mock(KafkaFuture.class);
        when(worker.fenceZombies(CONN1, SNAPSHOT_STOPPED_CONN1.taskCountRecord(CONN1), CONN1_CONFIG)).thenReturn(workerFencingFuture);
        when(workerFencingFuture.thenApply(any(KafkaFuture.BaseFunction.class))).thenReturn(herderFencingFuture);

        ArgumentCaptor<KafkaFuture.BiConsumer<Void, Throwable>> herderFencingCallback = ArgumentCaptor.forClass(KafkaFuture.BiConsumer.class);
        when(herderFencingFuture.whenComplete(herderFencingCallback.capture())).thenAnswer(invocation -> {
            herderFencingCallback.getValue().accept(null, new ConnectException("Failed to perform zombie fencing"));
            return null;
        });

        FutureCallback<Message> callback = new FutureCallback<>();
        herder.resetConnectorOffsets(CONN1, callback);
        // Process the zombie fencing request that is queued up first
        herder.tick();
        ExecutionException e = assertThrows(ExecutionException.class, () -> callback.get(1000L, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());
        assertEquals("Failed to perform zombie fencing for source connector prior to modifying offsets",
                e.getCause().getMessage());

        // Two fencing callbacks are added - one is in ZombieFencing::start itself to remove the connector from the active
        // fencing list. The other is the callback passed from DistributedHerder::modifyConnectorOffsets in order to
        // queue up the actual reset offsets request if the zombie fencing succeeds.
        verify(herderFencingFuture, times(2)).whenComplete(any());
        verifyNoMoreInteractions(workerFencingFuture, herderFencingFuture, member, worker);
    }

    private void expectMemberPoll() {
        ArgumentCaptor<Supplier<UncheckedCloseable>> onPoll = ArgumentCaptor.forClass(Supplier.class);
        doAnswer(invocation -> {
            onPoll.getValue().get().close();
            return null;
        }).when(member).poll(anyLong(), onPoll.capture());
    }

    private void expectMemberEnsureActive() {
        ArgumentCaptor<Supplier<UncheckedCloseable>> onPoll = ArgumentCaptor.forClass(Supplier.class);
        doAnswer(invocation -> {
            onPoll.getValue().get().close();
            return null;
        }).when(member).ensureActive(onPoll.capture());
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
        ArgumentCaptor<Supplier<UncheckedCloseable>> onPoll = ArgumentCaptor.forClass(Supplier.class);
        doAnswer(invocation -> {
            onPoll.getValue().get().close();
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
        }).when(member).ensureActive(onPoll.capture());

        if (isLeader) {
            doNothing().when(configBackingStore).claimWritePrivileges();
        }

        if (!revokedConnectors.isEmpty()) {
            for (String connector : revokedConnectors) {
                doNothing().when(worker).stopAndAwaitConnector(connector);
            }
        }

        if (!revokedTasks.isEmpty()) {
            doNothing().when(worker).stopAndAwaitTask(any(ConnectorTaskId.class));
        }

        if (!revokedConnectors.isEmpty()) {
            doNothing().when(statusBackingStore).flush();
        }

        doNothing().when(member).wakeup();
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

        Map<String, AppliedConnectorConfig> appliedConnectorConfigs = taskConfigs.keySet().stream()
                .map(ConnectorTaskId::connector)
                .distinct()
                .filter(connectorConfigs::containsKey)
                .collect(Collectors.toMap(
                        Function.identity(),
                        connector -> new AppliedConnectorConfig(connectorConfigs.get(connector))
                ));

        return new ClusterConfigState(
                1,
                sessionKey,
                taskCounts,
                connectorConfigs,
                Collections.singletonMap(CONN1, TargetState.STARTED),
                taskConfigs,
                taskCountRecords,
                taskConfigGenerations,
                appliedConnectorConfigs,
                pendingFencing,
                Collections.emptySet());
    }

    private void expectExecuteTaskReconfiguration(boolean running, ConnectorConfig connectorConfig, Answer<List<Map<String, String>>> answer) {
        when(worker.isRunning(CONN1)).thenReturn(running);
        if (running) {
            when(worker.getPlugins()).thenReturn(plugins);
            when(worker.connectorTaskConfigs(CONN1, connectorConfig)).thenAnswer(answer);
        }
    }

    private SessionKey expectNewSessionKey() {
        SecretKey secretKey = mock(SecretKey.class);
        when(secretKey.getAlgorithm()).thenReturn(INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
        when(secretKey.getEncoded()).thenReturn(new byte[32]);
        return new SessionKey(secretKey, time.milliseconds() + TimeUnit.DAYS.toMillis(1));
    }

    private void expectConfigRefreshAndSnapshot(final ClusterConfigState readToEndSnapshot) {
        try {
            doNothing().when(configBackingStore).refresh(anyLong(), any(TimeUnit.class));
            when(configBackingStore.snapshot()).thenReturn(readToEndSnapshot);
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
        assertTrue(herderExecutor.awaitTermination(10, TimeUnit.SECONDS), "herder thread did not finish in time");
        herderFuture.get();
        assertTrue(noneConnectorClientConfigOverridePolicy.isClosed());
    }

    private void expectHerderStartup() {
        doNothing().when(worker).start();
        doNothing().when(statusBackingStore).start();
        doNothing().when(configBackingStore).start();
    }

    private void expectHerderShutdown() {
        doNothing().when(worker).stopAndAwaitConnectors();
        doNothing().when(worker).stopAndAwaitTasks();

        doNothing().when(member).stop();
        doNothing().when(statusBackingStore).stop();
        doNothing().when(configBackingStore).stop();
        doNothing().when(worker).stop();
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

    private static List<String> expectRecordStages(Callback<?> callback) {
        when(callback.chainStaging(any())).thenCallRealMethod();
        List<String> result = Collections.synchronizedList(new ArrayList<>());

        doAnswer(invocation -> {
            Stage stage = invocation.getArgument(0);
            if (stage != null)
                result.add(stage.description());
            return null;
        }).when(callback).recordStage(any());

        return result;
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract static class BogusSourceConnector extends SourceConnector {
    }

    private abstract static class BogusSourceTask extends SourceTask {
    }

    /**
     * A mock {@link ExecutorService} that runs tasks synchronously on the same thread as the caller. This mock
     * implementation can't be "shut down" and it is the responsibility of the caller to ensure that {@link Runnable}s
     * submitted via {@link #execute(Runnable)} don't hang indefinitely.
     */
    private static class MockSynchronousExecutor extends AbstractExecutorService {
        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {

        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }

    private DistributedHerder exactlyOnceHerder() {
        Map<String, String> config = new HashMap<>(HERDER_CONFIG);
        config.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        return mock(DistributedHerder.class, withSettings().defaultAnswer(CALLS_REAL_METHODS).useConstructor(new DistributedConfig(config),
                worker, WORKER_ID, KAFKA_CLUSTER_ID, statusBackingStore, configBackingStore, member, MEMBER_URL, restClient, metrics, time,
                noneConnectorClientConfigOverridePolicy, Collections.emptyList(), null, new AutoCloseable[0]));
    }

}
