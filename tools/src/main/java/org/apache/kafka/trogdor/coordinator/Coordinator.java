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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.CreateTaskResponse;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.StopTaskResponse;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Trogdor coordinator.
 *
 * The coordinator manages the agent processes in the cluster. 
 */
public final class Coordinator {
    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    public static final int DEFAULT_PORT = 8889;

    /**
     * The start time of the Coordinator in milliseconds.
     */
    private final long startTimeMs;

    /**
     * The task manager.
     */
    private final TaskManager taskManager;

    /**
     * The REST server.
     */
    private final JsonRestServer restServer;

    /**
     * Create a new Coordinator.
     *
     * @param platform      The platform object to use.
     * @param scheduler     The scheduler to use for this Coordinator.
     * @param restServer    The REST server to use.
     * @param resource      The AgentRestResoure to use.
     */
    public Coordinator(Platform platform, Scheduler scheduler, JsonRestServer restServer,
                       CoordinatorRestResource resource) {
        this.startTimeMs = scheduler.time().milliseconds();
        this.taskManager = new TaskManager(platform, scheduler);
        this.restServer = restServer;
        resource.setCoordinator(this);
    }

    public int port() {
        return this.restServer.port();
    }

    public CoordinatorStatusResponse status() throws Exception {
        return new CoordinatorStatusResponse(startTimeMs);
    }

    public CreateTaskResponse createTask(CreateTaskRequest request) throws Exception {
        return new CreateTaskResponse(taskManager.createTask(request.id(), request.spec()));
    }

    public StopTaskResponse stopTask(StopTaskRequest request) throws Exception {
        return new StopTaskResponse(taskManager.stopTask(request.id()));
    }

    public TasksResponse tasks(TasksRequest request) throws Exception {
        return taskManager.tasks(request);
    }

    public void beginShutdown(boolean stopAgents) throws Exception {
        restServer.beginShutdown();
        taskManager.beginShutdown(stopAgents);
    }

    public void waitForShutdown() throws Exception {
        restServer.waitForShutdown();
        taskManager.waitForShutdown();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator")
            .defaultHelp(true)
            .description("The Trogdor fault injection coordinator");
        parser.addArgument("--coordinator.config", "-c")
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

        Platform platform = Platform.Config.parse(nodeName, configPath);
        JsonRestServer restServer = new JsonRestServer(
            Node.Util.getTrogdorCoordinatorPort(platform.curNode()));
        CoordinatorRestResource resource = new CoordinatorRestResource();
        log.info("Starting coordinator process.");
        final Coordinator coordinator = new Coordinator(platform, Scheduler.SYSTEM,
            restServer, resource);
        restServer.start(resource);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.warn("Running coordinator shutdown hook.");
                try {
                    coordinator.beginShutdown(false);
                    coordinator.waitForShutdown();
                } catch (Exception e) {
                    log.error("Got exception while running coordinator shutdown hook.", e);
                }
            }
        });
        coordinator.waitForShutdown();
    }
};
