/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DistributedHerder.class)
@PowerMockIgnore("javax.management.*")
public class DistributedHerderTest {
    private static final Map<String, String> HERDER_CONFIG = new HashMap<>();
    static {
        HERDER_CONFIG.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        HERDER_CONFIG.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        HERDER_CONFIG.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        HERDER_CONFIG.put(DistributedConfig.GROUP_ID_CONFIG, "test-connect-group");
        // The WorkerConfig base class has some required settings without defaults
        HERDER_CONFIG.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        HERDER_CONFIG.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        HERDER_CONFIG.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        HERDER_CONFIG.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
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
        CONN1_CONFIG.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final Map<String, String> CONN1_CONFIG_UPDATED = new HashMap<>(CONN1_CONFIG);
    static {
        CONN1_CONFIG_UPDATED.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar,baz");
    }
    private static final Map<String, String> CONN2_CONFIG = new HashMap<>();
    static {
        CONN2_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN2);
        CONN2_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN2_CONFIG.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar");
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
    private static final ClusterConfigState SNAPSHOT_UPDATED_CONN1_CONFIG = new ClusterConfigState(1, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG_UPDATED), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.<String>emptySet());

    private static final String WORKER_ID = "localhost:8083";

    @Mock private KafkaConfigBackingStore configStorage;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private WorkerGroupMember member;
    private MockTime time;
    private DistributedHerder herder;
    @Mock private Worker worker;
    @Mock private Callback<Herder.Created<ConnectorInfo>> putConnectorCallback;

    private ConfigBackingStore.UpdateListener configUpdateListener;
    private WorkerRebalanceListener rebalanceListener;

    @Before
    public void setUp() throws Exception {
        worker = PowerMock.createMock(Worker.class);
        EasyMock.expect(worker.isSinkConnector(CONN1)).andStubReturn(Boolean.FALSE);
        time = new MockTime();

        herder = PowerMock.createPartialMock(DistributedHerder.class, new String[]{"backoff", "updateDeletedConnectorStatus"},
                new DistributedConfig(HERDER_CONFIG), worker, WORKER_ID, statusBackingStore, configStorage, member, MEMBER_URL, time);

        configUpdateListener = herder.new ConfigUpdateListener();
        rebalanceListener = herder.new RebalanceListener();

        PowerMock.expectPrivate(herder, "updateDeletedConnectorStatus").andVoid().anyTimes();
    }

    @Test
    public void testJoinAssignment() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);

        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testRebalance() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.<ConnectorTaskId>asList());

        // and the new assignment started
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testRebalanceFailedConnector() throws Exception {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        expectRebalance(Arrays.asList(CONN1), Arrays.asList(TASK1), ConnectProtocol.Assignment.NO_ERROR,
                1, Arrays.asList(CONN1), Arrays.<ConnectorTaskId>asList());

        // and the new assignment started
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(false);

        // worker is not running, so we should see no call to connectorTaskConfigs()

        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testHaltCleansUpWorker() {
        EasyMock.expect(worker.connectorNames()).andReturn(Collections.singleton(CONN1));
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        EasyMock.expect(worker.taskIds()).andReturn(Collections.singleton(TASK1));
        worker.stopTasks(Collections.singleton(TASK1));
        PowerMock.expectLastCall();
        worker.awaitStopTasks(Collections.singleton(TASK1));
        PowerMock.expectLastCall();
        member.stop();
        PowerMock.expectLastCall();
        configStorage.stop();
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
        // CONN2 is new, should succeed
        configStorage.putConnectorConfig(CONN2, CONN2_CONFIG);
        PowerMock.expectLastCall();
        ConnectorInfo info = new ConnectorInfo(CONN2, CONN2_CONFIG, Collections.<ConnectorTaskId>emptyList());
        putConnectorCallback.onCompletion(null, new Herder.Created<>(true, info));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN2, CONN2_CONFIG, false, putConnectorCallback);
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
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

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        // Start with one connector
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);

        // And delete the connector
        member.wakeup();
        PowerMock.expectLastCall();
        configStorage.removeConnectorConfig(CONN1);
        PowerMock.expectLastCall();
        putConnectorCallback.onCompletion(null, new Herder.Created<ConnectorInfo>(false, null));
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.putConnectorConfig(CONN1, null, true, putConnectorCallback);
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnector() throws Exception {
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andStubReturn(TASK_CONFIGS);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.singletonList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);

        // now handle the connector restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        EasyMock.expect(worker.ownsConnector(CONN1)).andReturn(true);

        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
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

        EasyMock.expect(worker.ownsConnector(CONN1)).andReturn(false);

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
        EasyMock.expect(worker.ownsConnector(CONN1)).andReturn(false);
        EasyMock.expect(member.ownerUrl(CONN1)).andReturn(ownerUrl);

        PowerMock.replayAll();

        herder.tick();
        FutureCallback<Void> callback = new FutureCallback<>();
        herder.restartConnector(CONN1, callback);
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
    public void testRestartTask() throws Exception {
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andStubReturn(TASK_CONFIGS);

        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(1, Collections.<String>emptyList(), Collections.singletonList(TASK0));
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();

        // now handle the task restart
        member.wakeup();
        PowerMock.expectLastCall();
        member.ensureActive();
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        EasyMock.expect(worker.ownsTask(TASK0)).andReturn(true);

        worker.stopAndAwaitTask(TASK0);
        PowerMock.expectLastCall();
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();

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
    public void testRestartTaskRedirectToLeader() throws Exception {
        // get the initial assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // now handle the task restart
        EasyMock.expect(worker.ownsTask(TASK0)).andReturn(false);
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
        EasyMock.expect(worker.ownsTask(TASK0)).andReturn(false);
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
        EasyMock.expect(configStorage.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        PowerMock.expectLastCall();
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
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
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();
        EasyMock.expect(configStorage.snapshot()).andReturn(SNAPSHOT); // for this test, it doesn't matter if we use the same config snapshot
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        configUpdateListener.onConnectorConfigUpdate(CONN1); // read updated config
        herder.tick(); // apply config

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
        EasyMock.expect(configStorage.snapshot()).andReturn(SNAPSHOT);
        member.requestRejoin();
        PowerMock.expectLastCall();
        // Performs rebalance and gets new assignment
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                ConnectProtocol.Assignment.NO_ERROR, 1, Collections.<String>emptyList(),
                Arrays.asList(TASK0));
        worker.startTask(EasyMock.eq(TASK0), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
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
        configStorage.refresh(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall().andThrow(new TimeoutException());
        member.maybeLeaveGroup();
        EasyMock.expectLastCall();
        PowerMock.expectPrivate(herder, "backoff", DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT);
        member.requestRejoin();

        // After backoff, restart the process and this time succeed
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);

        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.startTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject(), EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testAccessors() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
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
        ConnectorInfo info = new ConnectorInfo(CONN1, CONN1_CONFIG, Arrays.asList(TASK0, TASK1, TASK2));
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
        worker.startConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);

        // list connectors, get connector info, get connector config, get task configs
        member.wakeup();
        PowerMock.expectLastCall().anyTimes();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // Poll loop for second round of calls
        member.ensureActive();
        PowerMock.expectLastCall();
        configStorage.putConnectorConfig(CONN1, CONN1_CONFIG_UPDATED);
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
        EasyMock.expect(configStorage.snapshot()).andReturn(SNAPSHOT_UPDATED_CONN1_CONFIG);
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        Capture<ConnectorConfig> capturedUpdatedConfig = EasyMock.newCapture();
        worker.startConnector(EasyMock.capture(capturedUpdatedConfig), EasyMock.<ConnectorContext>anyObject(),
                EasyMock.eq(herder), EasyMock.eq(TargetState.STARTED));
        PowerMock.expectLastCall();
        EasyMock.expect(worker.isRunning(CONN1)).andReturn(true);
        EasyMock.expect(worker.connectorTaskConfigs(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);

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
        ConnectorInfo updatedInfo = new ConnectorInfo(CONN1, CONN1_CONFIG_UPDATED, Arrays.asList(TASK0, TASK1, TASK2));
        assertEquals(new Herder.Created<>(false, updatedInfo), putConfigCb.get());

        // Check config again to validate change
        connectorConfigCb = new FutureCallback<>();
        herder.connectorConfig(CONN1, connectorConfigCb);
        herder.tick();
        assertTrue(connectorConfigCb.isDone());
        assertEquals(CONN1_CONFIG_UPDATED, connectorConfigCb.get());
        // The config passed to Worker should
        assertEquals(Arrays.asList("foo", "bar", "baz"),
                capturedUpdatedConfig.getValue().getList(ConnectorConfig.TOPICS_CONFIG));
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
                rebalanceListener.onAssigned(assignment, 0);
                return null;
            }
        });

        if (revokedConnectors != null) {
            for (String connector : revokedConnectors) {
                worker.stopConnector(connector);
                PowerMock.expectLastCall();
            }
        }

        if (revokedTasks != null && !revokedTasks.isEmpty()) {
            worker.stopTasks(revokedTasks);
            PowerMock.expectLastCall();
            worker.awaitStopTasks(revokedTasks);
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
        configStorage.refresh(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();
        EasyMock.expect(configStorage.snapshot()).andReturn(readToEndSnapshot);
    }


    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

}
