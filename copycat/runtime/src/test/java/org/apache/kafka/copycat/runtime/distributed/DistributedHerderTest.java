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

package org.apache.kafka.copycat.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.copycat.connector.ConnectorContext;
import org.apache.kafka.copycat.errors.AlreadyExistsException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.TaskConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.runtime.WorkerConfig;
import org.apache.kafka.copycat.source.SourceConnector;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.KafkaConfigStorage;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.TestFuture;
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
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DistributedHerder.class)
@PowerMockIgnore("javax.management.*")
public class DistributedHerderTest {
    private static final Properties HERDER_CONFIG = new Properties();
    static {
        HERDER_CONFIG.put(KafkaConfigStorage.CONFIG_TOPIC_CONFIG, "config-topic");
        HERDER_CONFIG.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        HERDER_CONFIG.put(DistributedConfig.GROUP_ID_CONFIG, "test-copycat-group");
        // The WorkerConfig base class has some required settings without defaults
        HERDER_CONFIG.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.copycat.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.copycat.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.copycat.json.JsonConverter");
        HERDER_CONFIG.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.copycat.json.JsonConverter");
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
            Collections.singletonMap(CONN1, CONN1_CONFIG), TASK_CONFIGS_MAP, Collections.<String>emptySet());

    @Mock private KafkaConfigStorage configStorage;
    @Mock private WorkerGroupMember member;
    private DistributedHerder herder;
    @Mock private Worker worker;
    @Mock private Callback<String> createCallback;
    @Mock private Callback<Void> destroyCallback;

    private Callback<String> connectorConfigCallback;
    private Callback<List<ConnectorTaskId>> taskConfigCallback;
    private WorkerRebalanceListener rebalanceListener;

    @Before
    public void setUp() throws Exception {
        worker = PowerMock.createMock(Worker.class);

        herder = PowerMock.createPartialMock(DistributedHerder.class, new String[]{"backoff"},
                new DistributedConfig(HERDER_CONFIG), worker, configStorage, member, MEMBER_URL);
        connectorConfigCallback = Whitebox.invokeMethod(herder, "connectorConfigCallback");
        taskConfigCallback = Whitebox.invokeMethod(herder, "taskConfigCallback");
        rebalanceListener = Whitebox.invokeMethod(herder, "rebalanceListener");
    }

    @Test
    public void testJoinAssignment() {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.addTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testHaltCleansUpWorker() {
        EasyMock.expect(worker.connectorNames()).andReturn(Collections.singleton(CONN1));
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        EasyMock.expect(worker.taskIds()).andReturn(Collections.singleton(TASK1));
        worker.stopTask(TASK1);
        PowerMock.expectLastCall();
        member.stop();
        PowerMock.expectLastCall();
        configStorage.stop();
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
        createCallback.onCompletion(null, CONN2);
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.addConnector(CONN2_CONFIG, createCallback);
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
        createCallback.onCompletion(EasyMock.<AlreadyExistsException>anyObject(), EasyMock.<String>isNull());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.addConnector(CONN1_CONFIG, createCallback);
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        // Start with one connector
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);

        // And delete the connector
        member.wakeup();
        PowerMock.expectLastCall();
        configStorage.putConnectorConfig(CONN1, null);
        PowerMock.expectLastCall();
        destroyCallback.onCompletion(null, null);
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.deleteConnector(CONN1, destroyCallback);
        herder.tick();

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
                CopycatProtocol.Assignment.NO_ERROR, 1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        connectorConfigCallback.onCompletion(null, CONN1); // read updated config
        herder.tick(); // apply config
        herder.tick(); // do rebalance

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorConfigUpdate() {
        // Connector config can be applied without any rebalance

        EasyMock.expect(member.memberId()).andStubReturn("member");
        EasyMock.expect(worker.connectorNames()).andStubReturn(Collections.singleton(CONN1));

        // join
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList());
        expectPostRebalanceCatchup(SNAPSHOT);
        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        // apply config
        member.wakeup();
        member.ensureActive();
        PowerMock.expectLastCall();
        EasyMock.expect(configStorage.snapshot()).andReturn(SNAPSHOT); // for this test, it doesn't matter if we use the same config snapshot
        worker.stopConnector(CONN1);
        PowerMock.expectLastCall();
        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        connectorConfigCallback.onCompletion(null, CONN1); // read updated config
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
                CopycatProtocol.Assignment.NO_ERROR, 1, Collections.<String>emptyList(), Arrays.asList(TASK0));
        worker.addTask(EasyMock.eq(TASK0), EasyMock.<TaskConfig>anyObject());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick(); // join
        taskConfigCallback.onCompletion(null, Arrays.asList(TASK0, TASK1, TASK2)); // read updated config
        herder.tick(); // apply config
        herder.tick(); // do rebalance

        PowerMock.verifyAll();
    }

    @Test
    public void testJoinLeaderCatchUpFails() throws Exception {
        // Join group and as leader fail to do assignment
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        expectRebalance(Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(),
                CopycatProtocol.Assignment.CONFIG_MISMATCH, 1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList());
        // Reading to end of log times out
        TestFuture<Void> readToEndFuture = new TestFuture<>();
        readToEndFuture.resolveOnGet(new TimeoutException());
        EasyMock.expect(configStorage.readToEnd()).andReturn(readToEndFuture);
        PowerMock.expectPrivate(herder, "backoff", DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT);
        member.requestRejoin();

        // After backoff, restart the process and this time succeed
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1));
        expectPostRebalanceCatchup(SNAPSHOT);

        worker.addConnector(EasyMock.<ConnectorConfig>anyObject(), EasyMock.<ConnectorContext>anyObject());
        PowerMock.expectLastCall();
        EasyMock.expect(worker.reconfigureConnectorTasks(CONN1, MAX_TASKS, null)).andReturn(TASK_CONFIGS);
        worker.addTask(EasyMock.eq(TASK1), EasyMock.<TaskConfig>anyObject());
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.tick();
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testInconsistentConfigs() throws Exception {
        // FIXME: if we have inconsistent configs, we need to request forced reconfig + write of the connector's task configs
        // This requires inter-worker communication, so needs the REST API
    }


    private void expectRebalance(final long offset, final List<String> assignedConnectors, final List<ConnectorTaskId> assignedTasks) {
        expectRebalance(null, null, CopycatProtocol.Assignment.NO_ERROR, offset, assignedConnectors, assignedTasks);
    }

    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final Collection<String> revokedConnectors, final List<ConnectorTaskId> revokedTasks,
                                 final short error, final long offset, final List<String> assignedConnectors, final List<ConnectorTaskId> assignedTasks) {
        member.ensureActive();
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                if (revokedConnectors != null)
                    rebalanceListener.onRevoked("leader", revokedConnectors, revokedTasks);
                CopycatProtocol.Assignment assignment = new CopycatProtocol.Assignment(
                        error, "leader", "leaderUrl", offset, assignedConnectors, assignedTasks);
                rebalanceListener.onAssigned(assignment);
                return null;
            }
        });
        member.wakeup();
        PowerMock.expectLastCall();
    }

    private void expectPostRebalanceCatchup(final ClusterConfigState readToEndSnapshot) {
        TestFuture<Void> readToEndFuture = new TestFuture<>();
        readToEndFuture.resolveOnGet((Void) null);
        EasyMock.expect(configStorage.readToEnd()).andReturn(readToEndFuture);
        EasyMock.expect(configStorage.snapshot()).andReturn(readToEndSnapshot);
    }


    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

}
