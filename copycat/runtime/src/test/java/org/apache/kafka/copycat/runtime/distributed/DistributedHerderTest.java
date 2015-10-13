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
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.TaskConfig;
import org.apache.kafka.copycat.runtime.Worker;
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
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class DistributedHerderTest {
    private static final Map<String, String> HERDER_CONFIG = new HashMap<>();
    static {
        HERDER_CONFIG.put(KafkaConfigStorage.CONFIG_TOPIC_CONFIG, "config-topic");
        HERDER_CONFIG.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        HERDER_CONFIG.put(DistributedHerderConfig.GROUP_ID_CONFIG, "test--copycat-group");
    }

    private static final String CONN1 = "sourceA";
    private static final String CONN2 = "sourceA";
    private static final ConnectorTaskId TASK0 = new ConnectorTaskId(CONN1, 0);
    private static final ConnectorTaskId TASK1 = new ConnectorTaskId(CONN1, 1);
    private static final ConnectorTaskId TASK2 = new ConnectorTaskId(CONN1, 2);
    private static final Integer MAX_TASKS = 3;
    private static final Map<String, String> CONNECTOR_CONFIG = new HashMap<>();
    static {
        CONNECTOR_CONFIG.put(ConnectorConfig.NAME_CONFIG, "sourceA");
        CONNECTOR_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONNECTOR_CONFIG.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONNECTOR_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
    }
    private static final HashMap<ConnectorTaskId, Map<String, String>> TASK_CONFIGS = new HashMap<>();
    static {
        TASK_CONFIGS.put(TASK0, TASK_CONFIG);
        TASK_CONFIGS.put(TASK1, TASK_CONFIG);
        TASK_CONFIGS.put(TASK2, TASK_CONFIG);
    }
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONNECTOR_CONFIG), TASK_CONFIGS, Collections.<String>emptySet());

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

        herder = new DistributedHerder(worker, HERDER_CONFIG, configStorage, member);
        connectorConfigCallback = Whitebox.invokeMethod(herder, "connectorConfigCallback");
        taskConfigCallback = Whitebox.invokeMethod(herder, "taskConfigCallback");
        rebalanceListener = Whitebox.invokeMethod(herder, "rebalanceListener");
    }

    @Test
    public void testJoinAssignment() {
        // Join group and get assignment
        EasyMock.expect(member.memberId()).andStubReturn("member");
        expectRebalance(1, Arrays.asList(CONN1), Arrays.asList(TASK1), SNAPSHOT, true);
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
        expectRebalance(1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(), SNAPSHOT, true);

        member.wakeup();
        PowerMock.expectLastCall();
        configStorage.putConnectorConfig(CONN1, CONNECTOR_CONFIG);
        PowerMock.expectLastCall();
        createCallback.onCompletion(null, CONN1);
        PowerMock.expectLastCall();
        member.poll(EasyMock.anyInt());
        PowerMock.expectLastCall();
        // No immediate action besides this -- change will be picked up via the config log

        PowerMock.replayAll();

        herder.addConnector(CONNECTOR_CONFIG, createCallback);
        herder.tick();

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        EasyMock.expect(member.memberId()).andStubReturn("leader");
        // Start with one connector
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList(), SNAPSHOT, true);
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

        // join
        expectRebalance(-1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(), null, false);
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
                1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList(), null, false);
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
        expectRebalance(1, Arrays.asList(CONN1), Collections.<ConnectorTaskId>emptyList(), SNAPSHOT, true);
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
        expectRebalance(-1, Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(), null, false);
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
                1, Collections.<String>emptyList(), Arrays.asList(TASK0), null, false);
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
    public void testInconsistentConfigs() throws Exception {
        // FIXME: if we have inconsistent configs, we need to request forced reconfig + write of the connector's task configs
        // This requires inter-worker communication, so needs the REST API
    }


    // Handles common initial part of rebalance callback. Does not handle instantiation of connectors and tasks.
    private void expectRebalance(final long offset, final Collection<String> assignedConnectors, final List<ConnectorTaskId> assignedTasks, final ClusterConfigState snapshot, final boolean readToEnd) {
        expectRebalance(null, null, offset, assignedConnectors, assignedTasks, snapshot, readToEnd);
    }

    private void expectRebalance(final Collection<String> revokedConnectors, final List<ConnectorTaskId> revokedTasks,
                                 final long offset, final Collection<String> assignedConnectors, final List<ConnectorTaskId> assignedTasks, final ClusterConfigState snapshot,
                                 final boolean readToEnd) {
        member.ensureActive();
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                if (revokedConnectors != null)
                    rebalanceListener.onRevoked("leader", revokedConnectors, revokedTasks);
                rebalanceListener.onAssigned(offset, "leader", assignedConnectors, assignedTasks);
                return null;
            }
        });
        if (readToEnd) {
            TestFuture<Void> readToEndFuture = new TestFuture<>();
            readToEndFuture.resolveOnGet((Void) null);
            EasyMock.expect(configStorage.readToEnd()).andReturn(readToEndFuture);
        }
        if (snapshot != null)
            EasyMock.expect(configStorage.snapshot()).andReturn(snapshot);
    }


    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

}
