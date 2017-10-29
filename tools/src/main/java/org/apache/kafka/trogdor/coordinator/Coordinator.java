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
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.fault.DoneState;
import org.apache.kafka.trogdor.fault.Fault;
import org.apache.kafka.trogdor.fault.FaultSet;
import org.apache.kafka.trogdor.fault.FaultSpec;
import org.apache.kafka.trogdor.fault.SendingState;
import org.apache.kafka.trogdor.rest.CoordinatorFaultsResponse;
import org.apache.kafka.trogdor.rest.CreateCoordinatorFaultRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Trogdor coordinator.
 *
 * The coordinator manages the agent processes in the cluster. 
 */
public final class Coordinator {
    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    /**
     * The clock to use for this coordinator.
     */
    private final Time time;

    /**
     * The start time in milliseconds.
     */
    private final long startTimeMs;

    /**
     * The platform.
     */
    private final Platform platform;

    /**
     * NodeManager objects for each node in the cluster.
     */
    private final Map<String, NodeManager> nodeManagers;

    /**
     * The lock protecting shutdown and faultQueue.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The condition variable which the coordinator thread waits on.
     */
    private final Condition cond = lock.newCondition();

    /**
     * The coordinator runnable.
     */
    private final CoordinatorRunnable runnable;

    /**
     * The REST server.
     */
    private final JsonRestServer restServer;

    /**
     * The coordinator thread.
     */
    private final KafkaThread thread;

    /**
     * True if the server is shutting down.
     */
    private boolean shutdown = false;

    /**
     * The set of faults which have been scheduled.
     */
    private final FaultSet pendingFaults = new FaultSet();

    /**
     * The set of faults which have been sent to the NodeManagers.
     */
    private final FaultSet processedFaults = new FaultSet();

    class CoordinatorRunnable implements Runnable {
        @Override
        public void run() {
            log.info("Starting main service thread.");
            try {
                long nextWakeMs = 0;
                while (true) {
                    long now = time.milliseconds();
                    List<Fault> toStart = new ArrayList<>();
                    lock.lock();
                    try {
                        if (shutdown) {
                            break;
                        }
                        if (nextWakeMs > now) {
                            if (cond.await(nextWakeMs - now, TimeUnit.MILLISECONDS)) {
                                log.trace("CoordinatorRunnable woke up early.");
                            }
                            now = time.milliseconds();
                            if (shutdown) {
                                break;
                            }
                        }
                        nextWakeMs = now + (60L * 60L * 1000L);
                        Iterator<Fault> iter = pendingFaults.iterateByStart();
                        while (iter.hasNext()) {
                            Fault fault = iter.next();
                            if (now < fault.spec().startMs()) {
                                nextWakeMs = Math.min(nextWakeMs, fault.spec().startMs());
                                break;
                            }
                            toStart.add(fault);
                            iter.remove();
                            processedFaults.add(fault);
                        }
                    } finally {
                        lock.unlock();
                    }
                    for (Fault fault: toStart) {
                        startFault(now, fault);
                    }
                }
            } catch (Throwable t) {
                log.error("CoordinatorRunnable shutting down with exception", t);
            } finally {
                log.info("CoordinatorRunnable shutting down.");
                restServer.stop();
                for (NodeManager nodeManager : nodeManagers.values()) {
                    nodeManager.beginShutdown();
                }
                for (NodeManager nodeManager : nodeManagers.values()) {
                    nodeManager.waitForShutdown();
                }
            }
        }
    }

    /**
     * Create a new Coordinator.
     *
     * @param platform      The platform object to use.
     * @param time          The timekeeper to use for this Coordinator.
     * @param restServer    The REST server to use.
     * @param resource      The AgentRestResoure to use.
     */
    public Coordinator(Platform platform, Time time, JsonRestServer restServer,
                       CoordinatorRestResource resource) {
        this.platform = platform;
        this.time = time;
        this.startTimeMs = time.milliseconds();
        this.runnable = new CoordinatorRunnable();
        this.restServer = restServer;
        this.nodeManagers = new HashMap<>();
        for (Node node : platform.topology().nodes().values()) {
            if (Node.Util.getTrogdorAgentPort(node) > 0) {
                this.nodeManagers.put(node.name(), new NodeManager(time, node));
            }
        }
        if (this.nodeManagers.isEmpty()) {
            log.warn("No agent nodes configured.");
        }
        this.thread = new KafkaThread("TrogdorCoordinatorThread", runnable, false);
        this.thread.start();
        resource.setCoordinator(this);
    }

    public int port() {
        return this.restServer.port();
    }

    private void startFault(long now, Fault fault) {
        Set<String> affectedNodes = fault.targetNodes(platform.topology());
        Set<NodeManager> affectedManagers = new HashSet<>();
        Set<String> nonexistentNodes = new HashSet<>();
        Set<String> nodeNames = new HashSet<>();
        for (String affectedNode : affectedNodes) {
            NodeManager nodeManager = nodeManagers.get(affectedNode);
            if (nodeManager == null) {
                nonexistentNodes.add(affectedNode);
            } else {
                affectedManagers.add(nodeManager);
                nodeNames.add(affectedNode);
            }
        }
        if (!nonexistentNodes.isEmpty()) {
            log.warn("Fault {} refers to {} non-existent node(s): {}", fault.id(),
                    nonexistentNodes.size(), Utils.join(nonexistentNodes, ", "));
        }
        log.info("Applying fault {} on {} node(s): {}", fault.id(),
                nodeNames.size(), Utils.join(nodeNames, ", "));
        if (nodeNames.isEmpty()) {
            fault.setState(new DoneState(now, ""));
        } else {
            fault.setState(new SendingState(nodeNames));
        }
        for (NodeManager nodeManager : affectedManagers) {
            nodeManager.enqueueFault(fault);
        }
    }

    public void beginShutdown() {
        lock.lock();
        try {
            this.shutdown = true;
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForShutdown() {
        try {
            this.thread.join();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for thread shutdown", e);
            Thread.currentThread().interrupt();
        }
    }

    public long startTimeMs() {
        return startTimeMs;
    }

    public CoordinatorFaultsResponse getFaults() {
        Map<String, CoordinatorFaultsResponse.FaultData> faultData = new TreeMap<>();
        lock.lock();
        try {
            getFaultsImpl(faultData, pendingFaults);
            getFaultsImpl(faultData, processedFaults);
        } finally {
            lock.unlock();
        }
        return new CoordinatorFaultsResponse(faultData);
    }

    private void getFaultsImpl(Map<String, CoordinatorFaultsResponse.FaultData> faultData,
                               FaultSet faultSet) {
        for (Iterator<Fault> iter = faultSet.iterateByStart();
             iter.hasNext(); ) {
            Fault fault = iter.next();
            CoordinatorFaultsResponse.FaultData data =
                new CoordinatorFaultsResponse.FaultData(fault.spec(), fault.state());
            faultData.put(fault.id(), data);
        }
    }

    public void createFault(CreateCoordinatorFaultRequest request) throws ClassNotFoundException {
        lock.lock();
        try {
            Fault fault = FaultSpec.Util.createFault(request.id(), request.spec());
            pendingFaults.add(fault);
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }


    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator")
            .defaultHelp(true)
            .description("The Trogdor fault injection coordinator");
        parser.addArgument("--coordinator.config")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .metavar("CONFIG")
            .help("The configuration file to use.");
        parser.addArgument("--node-name")
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
        final Coordinator coordinator = new Coordinator(platform, Time.SYSTEM,
            restServer, resource);
        restServer.start(resource);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.error("Running shutdown hook...");
                coordinator.beginShutdown();
                coordinator.waitForShutdown();
            }
        });
        coordinator.waitForShutdown();
    }
};
