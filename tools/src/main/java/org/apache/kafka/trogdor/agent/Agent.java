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

import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.rest.UptimeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Set;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Trogdor agent.
 *
 * The agent process runs tasks.
 */
public final class Agent {
    private static final Logger log = LoggerFactory.getLogger(Agent.class);

    /**
     * The default Agent port.
     */
    public static final int DEFAULT_PORT = 8888;

    /**
     * The workerId to use in exec mode.
     */
    private static final long EXEC_WORKER_ID = 1;

    /**
     * The taskId to use in exec mode.
     */
    private static final String EXEC_TASK_ID = "task0";

    /**
     * The platform object to use for this agent.
     */
    private final Platform platform;

    /**
     * The time at which this server was started.
     */
    private final long serverStartMs;

    /**
     * The WorkerManager.
     */
    private final WorkerManager workerManager;

    /**
     * The REST server.
     */
    private final JsonRestServer restServer;

    private final Time time;

    /**
     * Create a new Agent.
     *
     * @param platform      The platform object to use.
     * @param scheduler     The scheduler to use for this Agent.
     * @param restServer    The REST server to use.
     * @param resource      The AgentRestResource to use.
     */
    public Agent(Platform platform, Scheduler scheduler,
                 JsonRestServer restServer, AgentRestResource resource) {
        this.platform = platform;
        this.time = scheduler.time();
        this.serverStartMs = time.milliseconds();
        this.workerManager = new WorkerManager(platform, scheduler);
        this.restServer = restServer;
        resource.setAgent(this);
    }

    public int port() {
        return this.restServer.port();
    }

    public void beginShutdown() throws Exception {
        restServer.beginShutdown();
        workerManager.beginShutdown();
    }

    public void waitForShutdown() throws Exception {
        restServer.waitForShutdown();
        workerManager.waitForShutdown();
    }

    public AgentStatusResponse status() throws Exception {
        return new AgentStatusResponse(serverStartMs, workerManager.workerStates());
    }

    public UptimeResponse uptime() {
        return new UptimeResponse(serverStartMs, time.milliseconds());
    }

    public void createWorker(CreateWorkerRequest req) throws Throwable {
        workerManager.createWorker(req.workerId(), req.taskId(), req.spec());
    }

    public void stopWorker(StopWorkerRequest req) throws Throwable {
        workerManager.stopWorker(req.workerId(), false);
    }

    public void destroyWorker(DestroyWorkerRequest req) throws Throwable {
        workerManager.stopWorker(req.workerId(), true);
    }

    /**
     * Rebase the task spec time so that it is not earlier than the current time.
     * This is only needed for tasks passed in with --exec.  Normally, the
     * controller rebases the task spec time.
     */
    TaskSpec rebaseTaskSpecTime(TaskSpec spec) throws Exception {
        ObjectNode node = JsonUtil.JSON_SERDE.valueToTree(spec);
        node.set("startMs", new LongNode(Math.max(time.milliseconds(), spec.startMs())));
        return JsonUtil.JSON_SERDE.treeToValue(node, TaskSpec.class);
    }

    /**
     * Start a task on the agent, and block until it completes.
     *
     * @param spec          The task specifiction.
     * @param out           The output stream to print to.
     *
     * @return              True if the task run successfully; false otherwise.
     */
    boolean exec(TaskSpec spec, PrintStream out) throws Exception {
        TaskController controller = null;
        try {
            controller = spec.newController(EXEC_TASK_ID);
        } catch (Exception e) {
            out.println("Unable to create the task controller.");
            e.printStackTrace(out);
            return false;
        }
        Set<String> nodes = controller.targetNodes(platform.topology());
        if (!nodes.contains(platform.curNode().name())) {
            out.println("This task is not configured to run on this node.  It runs on node(s): " +
                Utils.join(nodes, ", ") + ", whereas this node is " +
                platform.curNode().name());
            return false;
        }
        KafkaFuture<String> future = null;
        try {
            future = workerManager.createWorker(EXEC_WORKER_ID, EXEC_TASK_ID, spec);
        } catch (Throwable e) {
            out.println("createWorker failed");
            e.printStackTrace(out);
            return false;
        }
        out.println("Waiting for completion of task:" + JsonUtil.toPrettyJsonString(spec));
        String error = future.get();
        if (error == null || error.isEmpty()) {
            out.println("Task succeeded with status " +
                JsonUtil.toPrettyJsonString(workerManager.workerStates().get(EXEC_WORKER_ID).status()));
            return true;
        } else {
            out.println("Task failed with status " +
                JsonUtil.toPrettyJsonString(workerManager.workerStates().get(EXEC_WORKER_ID).status()) +
                " and error " + error);
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-agent")
            .defaultHelp(true)
            .description("The Trogdor fault injection agent");
        parser.addArgument("--agent.config", "-c")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .metavar("CONFIG")
            .help("The configuration file to use.");
        parser.addArgument("--node-name", "-n")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("node_name")
            .metavar("NODE_NAME")
            .help("The name of this node.");
        parser.addArgument("--exec", "-e")
            .action(store())
            .type(String.class)
            .dest("task_spec")
            .metavar("TASK_SPEC")
            .help("Execute a single task spec and then exit.  The argument is the task spec to load when starting up, or a path to it.");
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        String configPath = res.getString("config");
        String nodeName = res.getString("node_name");
        String taskSpec = res.getString("task_spec");

        Platform platform = Platform.Config.parse(nodeName, configPath);
        JsonRestServer restServer =
            new JsonRestServer(Node.Util.getTrogdorAgentPort(platform.curNode()));
        AgentRestResource resource = new AgentRestResource();
        log.info("Starting agent process.");
        final Agent agent = new Agent(platform, Scheduler.SYSTEM, restServer, resource);
        restServer.start(resource);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("Running agent shutdown hook.");
            try {
                agent.beginShutdown();
                agent.waitForShutdown();
            } catch (Exception e) {
                log.error("Got exception while running agent shutdown hook.", e);
            }
        }));
        if (taskSpec != null) {
            TaskSpec spec = null;
            try {
                spec = JsonUtil.objectFromCommandLineArgument(taskSpec, TaskSpec.class);
            } catch (Exception e) {
                System.out.println("Unable to parse the supplied task spec.");
                e.printStackTrace();
                Exit.exit(1);
            }
            TaskSpec effectiveSpec = agent.rebaseTaskSpecTime(spec);
            Exit.exit(agent.exec(effectiveSpec, System.out) ? 0 : 1);
        }
        agent.waitForShutdown();
    }
};
