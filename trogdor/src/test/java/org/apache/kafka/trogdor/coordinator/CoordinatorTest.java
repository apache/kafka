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

package org.apache.kafka.trogdor.coordinator;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.utils.MockScheduler;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.common.CapturingCommandRunner;
import org.apache.kafka.trogdor.common.ExpectedTasks;
import org.apache.kafka.trogdor.common.ExpectedTasks.ExpectedTaskBuilder;
import org.apache.kafka.trogdor.common.MiniTrogdorCluster;
import org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.DestroyTaskRequest;
import org.apache.kafka.trogdor.rest.RequestConflictException;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskPending;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TaskRunning;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TaskStateType;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.UptimeResponse;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.task.NoOpTaskSpec;
import org.apache.kafka.trogdor.task.SampleTaskSpec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(value = 240000, unit = MILLISECONDS)
public class CoordinatorTest {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorTest.class);

    @Test
    public void testCoordinatorStatus() throws Exception {
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
                addCoordinator("node01").
                build()) {
            CoordinatorStatusResponse status = cluster.coordinatorClient().status();
            assertEquals(cluster.coordinator().status(), status);
        }
    }

    @Test
    public void testCoordinatorUptime() throws Exception {
        MockTime time = new MockTime(0, 200, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            scheduler(scheduler).
            build()) {
            UptimeResponse uptime = cluster.coordinatorClient().uptime();
            assertEquals(cluster.coordinator().uptime(), uptime);

            time.setCurrentTimeMs(250);
            assertNotEquals(cluster.coordinator().uptime(), uptime);
        }
    }

    @Test
    public void testCreateTask() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
                addCoordinator("node01").
                addAgent("node02").
                scheduler(scheduler).
                build()) {
            new ExpectedTasks().waitFor(cluster.coordinatorClient());

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1, 2);
            cluster.coordinatorClient().createTask(
                new CreateTaskRequest("foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskPending(fooSpec)).
                    build()).
                waitFor(cluster.coordinatorClient());

            // Re-creating a task with the same arguments is not an error.
            cluster.coordinatorClient().createTask(
                new CreateTaskRequest("foo", fooSpec));

            // Re-creating a task with different arguments gives a RequestConflictException.
            NoOpTaskSpec barSpec = new NoOpTaskSpec(1000, 2000);
            assertThrows(RequestConflictException.class, () -> cluster.coordinatorClient().createTask(
                new CreateTaskRequest("foo", barSpec)),
                "Recreating task with different task spec is not allowed");

            time.sleep(2);
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, new TextNode("active"))).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                waitFor(cluster.coordinatorClient()).
                waitFor(cluster.agentClient("node02"));

            time.sleep(3);
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 2, 5, "", false, new TextNode("done"))).
                    build()).
                waitFor(cluster.coordinatorClient());
        }
    }

    @Test
    public void testTaskDistribution() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
                addCoordinator("node01").
                addAgent("node01").
                addAgent("node02").
                scheduler(scheduler).
                build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            AgentClient agentClient1 = cluster.agentClient("node01");
            AgentClient agentClient2 = cluster.agentClient("node02");

            new ExpectedTasks().
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(5, 7);
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").taskState(
                    new TaskPending(fooSpec)).build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            time.sleep(11);
            ObjectNode status1 = new ObjectNode(JsonNodeFactory.instance);
            status1.set("node01", new TextNode("active"));
            status1.set("node02", new TextNode("active"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 11, status1)).
                    workerState(new WorkerRunning("foo", fooSpec, 11,  new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            time.sleep(7);
            ObjectNode status2 = new ObjectNode(JsonNodeFactory.instance);
            status2.set("node01", new TextNode("done"));
            status2.set("node02", new TextNode("done"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 11, 18,
                        "", false, status2)).
                    workerState(new WorkerDone("foo", fooSpec, 11, 18, new TextNode("done"), "")).
                    build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);
        }
    }

    @Test
    public void testTaskCancellation() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            AgentClient agentClient1 = cluster.agentClient("node01");
            AgentClient agentClient2 = cluster.agentClient("node02");

            new ExpectedTasks().
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(5, 7);
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").taskState(new TaskPending(fooSpec)).build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            time.sleep(11);

            ObjectNode status1 = new ObjectNode(JsonNodeFactory.instance);
            status1.set("node01", new TextNode("active"));
            status1.set("node02", new TextNode("active"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 11, status1)).
                    workerState(new WorkerRunning("foo", fooSpec, 11, new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            ObjectNode status2 = new ObjectNode(JsonNodeFactory.instance);
            status2.set("node01", new TextNode("done"));
            status2.set("node02", new TextNode("done"));
            time.sleep(7);
            coordinatorClient.stopTask(new StopTaskRequest("foo"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 11, 18, "",
                        true, status2)).
                    workerState(new WorkerDone("foo", fooSpec, 11, 18, new TextNode("done"), "")).
                    build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            coordinatorClient.destroyTask(new DestroyTaskRequest("foo"));
            new ExpectedTasks().
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);
        }
    }

    @Test
    public void testTaskDestruction() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            AgentClient agentClient1 = cluster.agentClient("node01");
            AgentClient agentClient2 = cluster.agentClient("node02");

            new ExpectedTasks().
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(2, 12);
            coordinatorClient.destroyTask(new DestroyTaskRequest("foo"));
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            NoOpTaskSpec barSpec = new NoOpTaskSpec(20, 20);
            coordinatorClient.createTask(new CreateTaskRequest("bar", barSpec));
            coordinatorClient.destroyTask(new DestroyTaskRequest("bar"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").taskState(new TaskPending(fooSpec)).build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);
            time.sleep(10);

            ObjectNode status1 = new ObjectNode(JsonNodeFactory.instance);
            status1.set("node01", new TextNode("active"));
            status1.set("node02", new TextNode("active"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 10, status1)).
                    build()).
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);

            coordinatorClient.destroyTask(new DestroyTaskRequest("foo"));
            new ExpectedTasks().
                waitFor(coordinatorClient).
                waitFor(agentClient1).
                waitFor(agentClient2);
        }
    }

    public static class ExpectedLines {
        List<String> expectedLines = new ArrayList<>();

        public ExpectedLines addLine(String line) {
            expectedLines.add(line);
            return this;
        }

        public ExpectedLines waitFor(final String nodeName,
                final CapturingCommandRunner runner) throws InterruptedException {
            TestUtils.waitForCondition(() -> linesMatch(nodeName, runner.lines(nodeName)),
                "failed to find the expected lines " + this);
            return this;
        }

        private boolean linesMatch(final String nodeName, List<String> actualLines) {
            int matchIdx = 0, i = 0;
            while (true) {
                if (matchIdx == expectedLines.size()) {
                    log.debug("Got expected lines for {}", nodeName);
                    return true;
                }
                if (i == actualLines.size()) {
                    log.info("Failed to find the expected lines for {}.  First " +
                        "missing line on index {}: {}",
                        nodeName, matchIdx, expectedLines.get(matchIdx));
                    return false;
                }
                String actualLine = actualLines.get(i++);
                String expectedLine = expectedLines.get(matchIdx);
                if (expectedLine.equals(actualLine)) {
                    matchIdx++;
                } else {
                    log.trace("Expected:\n'{}', Got:\n'{}'", expectedLine, actualLine);
                    matchIdx = 0;
                }
            }
        }

        @Override
        public String toString() {
            return Utils.join(expectedLines, ", ");
        }
    }

    private static List<List<String>> createPartitionLists(String[][] array) {
        List<List<String>> list = new ArrayList<>();
        for (String[] a : array) {
            list.add(Arrays.asList(a));
        }
        return list;
    }

    @Test
    public void testNetworkPartitionFault() throws Exception {
        CapturingCommandRunner runner = new CapturingCommandRunner();
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
                addCoordinator("node01").
                addAgent("node01").
                addAgent("node02").
                addAgent("node03").
                commandRunner(runner).
                scheduler(scheduler).
                build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            NetworkPartitionFaultSpec spec = new NetworkPartitionFaultSpec(0, Long.MAX_VALUE,
                createPartitionLists(new String[][] {
                    new String[] {"node01", "node02"},
                    new String[] {"node03"},
                }));
            coordinatorClient.createTask(new CreateTaskRequest("netpart", spec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("netpart").taskSpec(spec).build()).
                waitFor(coordinatorClient);
            checkLines("-A", runner);
        }
        checkLines("-D", runner);
    }

    private void checkLines(String prefix, CapturingCommandRunner runner) throws InterruptedException {
        new ExpectedLines().
            addLine("sudo iptables " + prefix + " INPUT -p tcp -s 127.0.0.1 -j DROP " +
                "-m comment --comment node03").
            waitFor("node01", runner);
        new ExpectedLines().
            addLine("sudo iptables " + prefix + " INPUT -p tcp -s 127.0.0.1 -j DROP " +
                "-m comment --comment node03").
            waitFor("node02", runner);
        new ExpectedLines().
            addLine("sudo iptables " + prefix + " INPUT -p tcp -s 127.0.0.1 -j DROP " +
                "-m comment --comment node01").
            addLine("sudo iptables " + prefix + " INPUT -p tcp -s 127.0.0.1 -j DROP " +
                "-m comment --comment node02").
            waitFor("node03", runner);
    }

    @Test
    public void testTasksRequestMatches() throws Exception {
        TasksRequest req1 = new TasksRequest(null, 0, 0, 0, 0, Optional.empty());
        assertTrue(req1.matches("foo1", -1, -1, TaskStateType.PENDING));
        assertTrue(req1.matches("bar1", 100, 200, TaskStateType.DONE));
        assertTrue(req1.matches("baz1", 100, -1, TaskStateType.RUNNING));

        TasksRequest req2 = new TasksRequest(null, 100, 0, 0, 0, Optional.empty());
        assertFalse(req2.matches("foo1", -1, -1, TaskStateType.PENDING));
        assertTrue(req2.matches("bar1", 100, 200, TaskStateType.DONE));
        assertFalse(req2.matches("bar1", 99, 200, TaskStateType.DONE));
        assertFalse(req2.matches("baz1", 99, -1, TaskStateType.RUNNING));

        TasksRequest req3 = new TasksRequest(null, 200, 900, 200, 900, Optional.empty());
        assertFalse(req3.matches("foo1", -1, -1, TaskStateType.PENDING));
        assertFalse(req3.matches("bar1", 100, 200, TaskStateType.DONE));
        assertFalse(req3.matches("bar1", 200, 1000, TaskStateType.DONE));
        assertTrue(req3.matches("bar1", 200, 700, TaskStateType.DONE));
        assertFalse(req3.matches("baz1", 101, -1, TaskStateType.RUNNING));

        List<String> taskIds = new ArrayList<>();
        taskIds.add("foo1");
        taskIds.add("bar1");
        taskIds.add("baz1");
        TasksRequest req4 = new TasksRequest(taskIds, 1000, -1, -1, -1, Optional.empty());
        assertFalse(req4.matches("foo1", -1, -1, TaskStateType.PENDING));
        assertTrue(req4.matches("foo1", 1000, -1, TaskStateType.RUNNING));
        assertFalse(req4.matches("foo1", 900, -1, TaskStateType.RUNNING));
        assertFalse(req4.matches("baz2", 2000, -1, TaskStateType.RUNNING));
        assertFalse(req4.matches("baz2", -1, -1, TaskStateType.PENDING));

        TasksRequest req5 = new TasksRequest(null, 0, 0, 0, 0, Optional.of(TaskStateType.RUNNING));
        assertTrue(req5.matches("foo1", -1, -1, TaskStateType.RUNNING));
        assertFalse(req5.matches("bar1", -1, -1, TaskStateType.DONE));
        assertFalse(req5.matches("baz1", -1, -1, TaskStateType.STOPPING));
        assertFalse(req5.matches("baz1", -1, -1, TaskStateType.PENDING));
    }

    @Test
    public void testTasksRequest() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            new ExpectedTasks().waitFor(coordinatorClient);

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1, 10);
            NoOpTaskSpec barSpec = new NoOpTaskSpec(3, 1);
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            coordinatorClient.createTask(new CreateTaskRequest("bar", barSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskPending(fooSpec)).
                    build()).
                addTask(new ExpectedTaskBuilder("bar").
                    taskState(new TaskPending(barSpec)).
                    build()).
                waitFor(coordinatorClient);

            assertEquals(0, coordinatorClient.tasks(
                new TasksRequest(null, 10, 0, 10, 0, Optional.empty())).tasks().size());
            TasksResponse resp1 = coordinatorClient.tasks(
                new TasksRequest(Arrays.asList("foo", "baz"), 0, 0, 0, 0, Optional.empty()));
            assertTrue(resp1.tasks().containsKey("foo"));
            assertFalse(resp1.tasks().containsKey("bar"));
            assertEquals(1, resp1.tasks().size());

            time.sleep(2);
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, new TextNode("active"))).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                addTask(new ExpectedTaskBuilder("bar").
                    taskState(new TaskPending(barSpec)).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02"));

            TasksResponse resp2 = coordinatorClient.tasks(
                new TasksRequest(null, 1, 0, 0, 0, Optional.empty()));
            assertTrue(resp2.tasks().containsKey("foo"));
            assertFalse(resp2.tasks().containsKey("bar"));
            assertEquals(1, resp2.tasks().size());

            assertEquals(0, coordinatorClient.tasks(
                new TasksRequest(null, 3, 0, 0, 0, Optional.empty())).tasks().size());
        }
    }

    /**
     * If an agent fails in the middle of a task and comes back up when the task is considered expired,
     * we want the task to be marked as DONE and not re-sent should a second failure happen.
     */
    @Test
    public void testAgentFailureAndTaskExpiry() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1, 500);
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            TaskState expectedState = new ExpectedTaskBuilder("foo").taskState(new TaskPending(fooSpec)).build().taskState();

            TaskState resp = coordinatorClient.task(new TaskRequest("foo"));
            assertEquals(expectedState, resp);


            time.sleep(2);
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, new TextNode("active"))).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02"));

            cluster.restartAgent("node02");
            time.sleep(550);
            // coordinator heartbeat sees that the agent is back up, re-schedules the task but the agent expires it
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 2, 552, "worker expired", false, null)).
                    workerState(new WorkerDone("foo", fooSpec, 552, 552, null, "worker expired")).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02"));

            cluster.restartAgent("node02");
            // coordinator heartbeat sees that the agent is back up but does not re-schedule the task as it is DONE
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 2, 552, "worker expired", false, null)).
                    // no worker states
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02"));
        }
    }

    @Test
    public void testTaskRequestWithOldStartMsGetsUpdated() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1, 500);
            time.sleep(552);

            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            NoOpTaskSpec updatedSpec = new NoOpTaskSpec(552, 500);
            coordinatorClient.createTask(new CreateTaskRequest("fooSpec", fooSpec));
            TaskState expectedState = new ExpectedTaskBuilder("fooSpec").taskState(
                new TaskRunning(updatedSpec, 552, new TextNode("receiving"))
            ).build().taskState();

            TaskState resp = coordinatorClient.task(new TaskRequest("fooSpec"));
            assertEquals(expectedState, resp);
        }
    }

    @Test
    public void testTaskRequestWithFutureStartMsDoesNotGetRun() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1000, 500);
            time.sleep(999);

            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            coordinatorClient.createTask(new CreateTaskRequest("fooSpec", fooSpec));
            TaskState expectedState = new ExpectedTaskBuilder("fooSpec").taskState(
                new TaskPending(fooSpec)
            ).build().taskState();

            TaskState resp = coordinatorClient.task(new TaskRequest("fooSpec"));
            assertEquals(expectedState, resp);
        }
    }

    @Test
    public void testTaskRequest() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();

            NoOpTaskSpec fooSpec = new NoOpTaskSpec(1, 10);
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            TaskState expectedState = new ExpectedTaskBuilder("foo").taskState(new TaskPending(fooSpec)).build().taskState();

            TaskState resp = coordinatorClient.task(new TaskRequest("foo"));
            assertEquals(expectedState, resp);

            time.sleep(2);
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, new TextNode("active"))).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02"));

            assertThrows(NotFoundException.class, () -> coordinatorClient.task(new TaskRequest("non-existent-foo")));
        }
    }

    @Test
    public void testWorkersExitingAtDifferentTimes() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        Scheduler scheduler = new MockScheduler(time);
        try (MiniTrogdorCluster cluster = new MiniTrogdorCluster.Builder().
            addCoordinator("node01").
            addAgent("node02").
            addAgent("node03").
            scheduler(scheduler).
            build()) {
            CoordinatorClient coordinatorClient = cluster.coordinatorClient();
            new ExpectedTasks().waitFor(coordinatorClient);

            HashMap<String, Long> nodeToExitMs = new HashMap<>();
            nodeToExitMs.put("node02", 10L);
            nodeToExitMs.put("node03", 20L);
            SampleTaskSpec fooSpec =
                new SampleTaskSpec(2, 100, nodeToExitMs, "");
            coordinatorClient.createTask(new CreateTaskRequest("foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskPending(fooSpec)).
                    build()).
                waitFor(coordinatorClient);

            time.sleep(2);
            ObjectNode status1 = new ObjectNode(JsonNodeFactory.instance);
            status1.set("node02", new TextNode("active"));
            status1.set("node03", new TextNode("active"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, status1)).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node02")).
                waitFor(cluster.agentClient("node03"));

            time.sleep(10);
            ObjectNode status2 = new ObjectNode(JsonNodeFactory.instance);
            status2.set("node02", new TextNode("halted"));
            status2.set("node03", new TextNode("active"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, status2)).
                    workerState(new WorkerRunning("foo", fooSpec, 2, new TextNode("active"))).
                    build()).
                waitFor(coordinatorClient).
                waitFor(cluster.agentClient("node03"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskRunning(fooSpec, 2, status2)).
                    workerState(new WorkerDone("foo", fooSpec, 2, 12, new TextNode("halted"), "")).
                    build()).
                waitFor(cluster.agentClient("node02"));

            time.sleep(10);
            ObjectNode status3 = new ObjectNode(JsonNodeFactory.instance);
            status3.set("node02", new TextNode("halted"));
            status3.set("node03", new TextNode("halted"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    taskState(new TaskDone(fooSpec, 2, 22, "",
                        false, status3)).
                    build()).
                waitFor(coordinatorClient);
        }
    }
}
