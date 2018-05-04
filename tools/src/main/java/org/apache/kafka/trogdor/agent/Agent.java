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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Trogdor agent.
 *
 * The agent process runs tasks.
 */
public final class Agent {
    private static final Logger log = LoggerFactory.getLogger(Agent.class);

    public static final int DEFAULT_PORT = 8888;

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

    /**
     * Create a new Agent.
     *
     * @param platform      The platform object to use.
     * @param scheduler     The scheduler to use for this Agent.
     * @param restServer    The REST server to use.
     * @param resource      The AgentRestResoure to use.
     */
    public Agent(Platform platform, Scheduler scheduler,
                 JsonRestServer restServer, AgentRestResource resource) {
        this.serverStartMs = scheduler.time().milliseconds();
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

    public void createWorker(CreateWorkerRequest req) throws Throwable {
        workerManager.createWorker(req.workerId(), req.taskId(), req.spec());
    }

    public void stopWorker(StopWorkerRequest req) throws Throwable {
        workerManager.stopWorker(req.workerId(), false);
    }

    public void destroyWorker(DestroyWorkerRequest req) throws Throwable {
        workerManager.stopWorker(req.workerId(), true);
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
        JsonRestServer restServer =
            new JsonRestServer(Node.Util.getTrogdorAgentPort(platform.curNode()));
        AgentRestResource resource = new AgentRestResource();
        log.info("Starting agent process.");
        final Agent agent = new Agent(platform, Scheduler.SYSTEM, restServer, resource);
        restServer.start(resource);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.warn("Running agent shutdown hook.");
                try {
                    agent.beginShutdown();
                    agent.waitForShutdown();
                } catch (Exception e) {
                    log.error("Got exception while running agent shutdown hook.", e);
                }
            }
        });
        agent.waitForShutdown();
    }
};
