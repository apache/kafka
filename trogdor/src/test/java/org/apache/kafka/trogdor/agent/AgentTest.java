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

package org.apache.kafka.trogdor.agent;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.utils.MockScheduler;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.basic.BasicNode;
import org.apache.kafka.trogdor.basic.BasicPlatform;
import org.apache.kafka.trogdor.basic.BasicTopology;
import org.apache.kafka.trogdor.common.ExpectedTasks;
import org.apache.kafka.trogdor.common.ExpectedTasks.ExpectedTaskBuilder;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.fault.Kibosh;
import org.apache.kafka.trogdor.fault.Kibosh.KiboshControlFile;
import org.apache.kafka.trogdor.fault.Kibosh.KiboshFilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.RequestConflictException;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.UptimeResponse;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.task.NoOpTaskSpec;
import org.apache.kafka.trogdor.task.SampleTaskSpec;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 120000, unit = MILLISECONDS)
public class AgentTest {

    private static BasicPlatform createBasicPlatform(Scheduler scheduler) {
        TreeMap<String, Node> nodes = new TreeMap<>();
        HashMap<String, String> config = new HashMap<>();
        config.put(Platform.Config.TROGDOR_AGENT_PORT, Integer.toString(Agent.DEFAULT_PORT));
        nodes.put("node01", new BasicNode("node01", "localhost",
            config, Collections.emptySet()));
        BasicTopology topology = new BasicTopology(nodes);
        return new BasicPlatform("node01", topology,
            scheduler, new BasicPlatform.ShellCommandRunner());
    }

    private Agent createAgent(Scheduler scheduler) {
        JsonRestServer restServer = new JsonRestServer(0);
        AgentRestResource resource = new AgentRestResource();
        restServer.start(resource);
        return new Agent(createBasicPlatform(scheduler), scheduler,
                restServer, resource);
    }

    @Test
    public void testAgentStartShutdown() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentProgrammaticShutdown() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        client.invokeShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentGetStatus() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        AgentStatusResponse status = client.status();
        assertEquals(agent.status(), status);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testCreateExpiredWorkerIsNotScheduled() throws Exception {
        long initialTimeMs = 100;
        long tickMs = 15;
        final boolean[] toSleep = {true};
        MockTime time = new MockTime(tickMs, initialTimeMs, 0) {
            /**
             * Modify sleep() to call super.sleep() every second call
             * in order to avoid the endless loop in the tick() calls to the MockScheduler listener
             */
            @Override
            public void sleep(long ms) {
                toSleep[0] = !toSleep[0];
                if (toSleep[0])
                    super.sleep(ms);
            }
        };
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        AgentStatusResponse status = client.status();

        assertEquals(Collections.emptyMap(), status.workers());
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(10, 10);
        client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
        long actualStartTimeMs = initialTimeMs + tickMs;
        long doneMs = actualStartTimeMs + 2 * tickMs;
        new ExpectedTasks().addTask(new ExpectedTaskBuilder("foo").
            workerState(new WorkerDone("foo", fooSpec, actualStartTimeMs,
                doneMs, null, "worker expired")).
            taskState(new TaskDone(fooSpec, actualStartTimeMs, doneMs, "worker expired", false, null)).
            build()).
            waitFor(client);
    }

    @Test
    public void testAgentGetUptime() throws Exception {
        MockTime time = new MockTime(0, 111, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();

        UptimeResponse uptime = client.uptime();
        assertEquals(agent.uptime(), uptime);

        time.setCurrentTimeMs(150);
        assertNotEquals(agent.uptime(), uptime);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentCreateWorkers() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        AgentStatusResponse status = client.status();
        assertEquals(Collections.emptyMap(), status.workers());
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(1000, 600000);
        client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
        new ExpectedTasks().addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("active"))).
                build()).
            waitFor(client);

        final NoOpTaskSpec barSpec = new NoOpTaskSpec(2000, 900000);
        client.createWorker(new CreateWorkerRequest(1, "bar", barSpec));
        client.createWorker(new CreateWorkerRequest(1, "bar", barSpec));

        assertThrows(RequestConflictException.class,
            () -> client.createWorker(new CreateWorkerRequest(1, "foo", barSpec)),
            "Recreating a request with a different taskId is not allowed");
        assertThrows(RequestConflictException.class,
            () -> client.createWorker(new CreateWorkerRequest(1, "bar", fooSpec)),
            "Recreating a request with a different spec is not allowed");

        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("active"))).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning("bar", barSpec, 0, new TextNode("active"))).
                build()).
            waitFor(client);

        final NoOpTaskSpec bazSpec = new NoOpTaskSpec(1, 450000);
        client.createWorker(new CreateWorkerRequest(2, "baz", bazSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("active"))).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning("bar", barSpec, 0, new TextNode("active"))).
                build()).
            addTask(new ExpectedTaskBuilder("baz").
                workerState(new WorkerRunning("baz", bazSpec, 0, new TextNode("active"))).
                build()).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentFinishesTasks() throws Exception {
        long startTimeMs = 2000;
        MockTime time = new MockTime(0, startTimeMs, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(startTimeMs, 2);
        long fooSpecStartTimeMs = startTimeMs;
        client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, startTimeMs, new TextNode("active"))).
                build()).
            waitFor(client);

        time.sleep(1);

        long barSpecWorkerId = 1;
        long barSpecStartTimeMs = startTimeMs + 1;
        final NoOpTaskSpec barSpec = new NoOpTaskSpec(startTimeMs, 900000);
        client.createWorker(new CreateWorkerRequest(barSpecWorkerId, "bar", barSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, fooSpecStartTimeMs, new TextNode("active"))).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning("bar", barSpec, barSpecStartTimeMs, new TextNode("active"))).
                build()).
            waitFor(client);

        time.sleep(1);

        // foo task expired
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone("foo", fooSpec, fooSpecStartTimeMs, fooSpecStartTimeMs + 2, new TextNode("done"), "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning("bar", barSpec, barSpecStartTimeMs, new TextNode("active"))).
                build()).
            waitFor(client);

        time.sleep(5);
        client.stopWorker(new StopWorkerRequest(barSpecWorkerId));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone("foo", fooSpec, fooSpecStartTimeMs, fooSpecStartTimeMs + 2, new TextNode("done"), "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerDone("bar", barSpec, barSpecStartTimeMs, startTimeMs + 7, new TextNode("done"), "")).
                build()).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testWorkerCompletions() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        SampleTaskSpec fooSpec = new SampleTaskSpec(0, 900000,
            Collections.singletonMap("node01", 1L), "");
        client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("active"))).
                build()).
            waitFor(client);

        SampleTaskSpec barSpec = new SampleTaskSpec(0, 900000,
            Collections.singletonMap("node01", 2L), "baz");
        client.createWorker(new CreateWorkerRequest(1, "bar", barSpec));

        time.sleep(1);
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone("foo", fooSpec, 0, 1,
                    new TextNode("halted"), "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning("bar", barSpec, 0,
                    new TextNode("active"))).
                build()).
            waitFor(client);

        time.sleep(1);
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone("foo", fooSpec, 0, 1,
                    new TextNode("halted"), "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerDone("bar", barSpec, 0, 2,
                    new TextNode("halted"), "baz")).
                build()).
            waitFor(client);
    }

    private static class MockKibosh implements AutoCloseable {
        private final File tempDir;
        private final Path controlFile;

        MockKibosh() throws IOException {
            tempDir = TestUtils.tempDirectory();
            controlFile = Paths.get(tempDir.toPath().toString(), Kibosh.KIBOSH_CONTROL);
            KiboshControlFile.EMPTY.write(controlFile);
        }

        KiboshControlFile read() throws IOException {
            return KiboshControlFile.read(controlFile);
        }

        @Override
        public void close() throws Exception {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testKiboshFaults() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        try (MockKibosh mockKibosh = new MockKibosh()) {
            assertEquals(KiboshControlFile.EMPTY, mockKibosh.read());
            FilesUnreadableFaultSpec fooSpec = new FilesUnreadableFaultSpec(0, 900000,
                Collections.singleton("myAgent"), mockKibosh.tempDir.getPath(), "/foo", 123);
            client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("Added fault foo"))).
                    build()).
                waitFor(client);
            assertEquals(new KiboshControlFile(Collections.singletonList(
                new KiboshFilesUnreadableFaultSpec("/foo", 123))), mockKibosh.read());
            FilesUnreadableFaultSpec barSpec = new FilesUnreadableFaultSpec(0, 900000,
                Collections.singleton("myAgent"), mockKibosh.tempDir.getPath(), "/bar", 456);
            client.createWorker(new CreateWorkerRequest(1, "bar", barSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("Added fault foo"))).build()).
                addTask(new ExpectedTaskBuilder("bar").
                    workerState(new WorkerRunning("bar", barSpec, 0, new TextNode("Added fault bar"))).build()).
                waitFor(client);
            assertEquals(new KiboshControlFile(asList(
                new KiboshFilesUnreadableFaultSpec("/foo", 123),
                new KiboshFilesUnreadableFaultSpec("/bar", 456))
            ), mockKibosh.read());
            time.sleep(1);
            client.stopWorker(new StopWorkerRequest(0));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerDone("foo", fooSpec, 0, 1, new TextNode("Removed fault foo"), "")).build()).
                addTask(new ExpectedTaskBuilder("bar").
                    workerState(new WorkerRunning("bar", barSpec, 0, new TextNode("Added fault bar"))).build()).
                waitFor(client);
            assertEquals(new KiboshControlFile(Collections.singletonList(
                new KiboshFilesUnreadableFaultSpec("/bar", 456))), mockKibosh.read());
        }
    }

    @Test
    public void testDestroyWorkers() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(0, 5);
        client.createWorker(new CreateWorkerRequest(0, "foo", fooSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec, 0, new TextNode("active"))).
                build()).
            waitFor(client);
        time.sleep(1);

        client.destroyWorker(new DestroyWorkerRequest(0));
        client.destroyWorker(new DestroyWorkerRequest(0));
        client.destroyWorker(new DestroyWorkerRequest(1));
        new ExpectedTasks().waitFor(client);
        time.sleep(1);

        final NoOpTaskSpec fooSpec2 = new NoOpTaskSpec(2, 1);
        client.createWorker(new CreateWorkerRequest(1, "foo", fooSpec2));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning("foo", fooSpec2, 2, new TextNode("active"))).
                build()).
            waitFor(client);

        time.sleep(2);
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone("foo", fooSpec2, 2, 4, new TextNode("done"), "")).
                build()).
            waitFor(client);

        time.sleep(1);
        client.destroyWorker(new DestroyWorkerRequest(1));
        new ExpectedTasks().waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    static void testExec(Agent agent, String expected, boolean expectedReturn, TaskSpec spec) throws Exception {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        PrintStream p = new PrintStream(b, true, StandardCharsets.UTF_8.toString());
        boolean actualReturn = agent.exec(spec, p);
        assertEquals(expected, b.toString());
        assertEquals(expectedReturn, actualReturn);
    }

    @Test
    public void testAgentExecWithTimeout() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        NoOpTaskSpec spec = new NoOpTaskSpec(0, 1);
        TaskSpec rebasedSpec = agent.rebaseTaskSpecTime(spec);
        testExec(agent,
            String.format("Waiting for completion of task:%s%n",
                JsonUtil.toPrettyJsonString(rebasedSpec)) +
            String.format("Task failed with status null and error worker expired%n"),
            false, rebasedSpec);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentExecWithNormalExit() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        SampleTaskSpec spec = new SampleTaskSpec(0, 120000,
            Collections.singletonMap("node01", 1L), "");
        TaskSpec rebasedSpec = agent.rebaseTaskSpecTime(spec);
        testExec(agent,
            String.format("Waiting for completion of task:%s%n",
                JsonUtil.toPrettyJsonString(rebasedSpec)) +
                String.format("Task succeeded with status \"halted\"%n"),
            true, rebasedSpec);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

}
