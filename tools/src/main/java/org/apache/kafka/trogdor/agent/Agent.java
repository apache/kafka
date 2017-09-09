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
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.fault.Fault;
import org.apache.kafka.trogdor.fault.FaultSet;
import org.apache.kafka.trogdor.fault.FaultSpec;
import org.apache.kafka.trogdor.fault.RunningState;
import org.apache.kafka.trogdor.rest.AgentFaultsResponse;
import org.apache.kafka.trogdor.rest.CreateAgentFaultRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Trogdor agent.
 *
 * The agent process implements faults directly.
 */
public final class Agent {
    private static final Logger log = LoggerFactory.getLogger(Agent.class);

    /**
     * The clock to use for this agent.
     */
    private final Time time;

    /**
     * The time at which this server was started.
     */
    private final long startTimeMs;

    /**
     * The platform.
     */
    private final Platform platform;

    /**
     * The lock protecting shutdown and faultSet.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The condition variable which the agent thread waits on.
     */
    private final Condition cond = lock.newCondition();

    /**
     * The agent runnable.
     */
    private final AgentRunnable runnable;

    /**
     * The REST server.
     */
    private final JsonRestServer restServer;

    /**
     * The agent thread.
     */
    private final KafkaThread thread;

    /**
     * The set of pending faults.
     */
    private final FaultSet pendingFaults = new FaultSet();

    /**
     * The set of faults which are running.
     */
    private final FaultSet runningFaults = new FaultSet();

    /**
     * The set of faults which are done.
     */
    private final FaultSet doneFaults = new FaultSet();

    /**
     * True if the server is shutting down.
     */
    private boolean shutdown = false;

    class AgentRunnable implements Runnable {
        @Override
        public void run() {
            log.info("Starting main service thread.");
            try {
                while (true) {
                    List<Fault> toStart = new ArrayList<>();
                    List<Fault> started = new ArrayList<>();
                    List<Fault> toEnd = new ArrayList<>();
                    List<Fault> ended = new ArrayList<>();
                    long now = time.milliseconds();
                    long nextWakeMs = now + (60L * 60L * 1000L);
                    lock.lock();
                    try {
                        Iterator<Fault> pending = pendingFaults.iterateByStart();
                        while (pending.hasNext()) {
                            Fault fault = pending.next();
                            toStart.add(fault);
                            long endMs = fault.spec().startMs() + fault.spec().durationMs();
                            nextWakeMs = Math.min(nextWakeMs, endMs);
                            pending.remove();
                        }
                        Iterator<Fault> running = runningFaults.iterateByEnd();
                        while (running.hasNext()) {
                            Fault fault = running.next();
                            RunningState state = (RunningState) fault.state();
                            long endMs = state.startedMs() + fault.spec().durationMs();
                            if (now < endMs) {
                                nextWakeMs = Math.min(nextWakeMs, endMs);
                                break;
                            }
                            toEnd.add(fault);
                            running.remove();
                        }
                    } finally {
                        lock.unlock();
                    }
                    for (Fault fault: toStart) {
                        try {
                            log.debug("Activating fault " + fault);
                            fault.activate(now, platform);
                            started.add(fault);
                        } catch (Throwable e) {
                            log.error("Error activating fault " + fault.id(), e);
                            ended.add(fault);
                        }
                    }
                    for (Fault fault: toEnd) {
                        try {
                            log.debug("Deactivating fault " + fault);
                            fault.deactivate(now, platform);
                        } catch (Throwable e) {
                            log.error("Error deactivating fault " + fault.id(), e);
                        } finally {
                            ended.add(fault);
                        }
                    }
                    lock.lock();
                    try {
                        for (Fault fault : started) {
                            runningFaults.add(fault);
                        }
                        for (Fault fault : ended) {
                            doneFaults.add(fault);
                        }
                        if (shutdown) {
                            return;
                        }
                        if (nextWakeMs > now) {
                            log.trace("Sleeping for {} ms", nextWakeMs - now);
                            if (cond.await(nextWakeMs - now, TimeUnit.MILLISECONDS)) {
                                log.trace("AgentRunnable woke up early");
                            }
                        }
                        if (shutdown) {
                            return;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable t) {
                log.error("Unhandled exception in AgentRunnable", t);
            } finally {
                log.info("AgentRunnable shutting down.");
                restServer.stop();
                int numDeactivated = deactivateRunningFaults();
                log.info("AgentRunnable deactivated {} fault(s).", numDeactivated);
            }
        }
    }

    private int deactivateRunningFaults() {
        long now = time.milliseconds();
        int numDeactivated = 0;
        lock.lock();
        try {
            for (Iterator<Fault> iter = runningFaults.iterateByStart(); iter.hasNext(); ) {
                Fault fault = iter.next();
                try {
                    numDeactivated++;
                    iter.remove();
                    fault.deactivate(now, platform);
                } catch (Exception e) {
                    log.error("Got exception while deactivating {}", fault, e);
                } finally {
                    doneFaults.add(fault);
                }
            }
        } finally {
            lock.unlock();
        }
        return numDeactivated;
    }

    /**
     * Create a new Agent.
     *
     * @param platform      The platform object to use.
     * @param time          The timekeeper to use for this Agent.
     * @param restServer    The REST server to use.
     * @param resource      The AgentRestResoure to use.
     */
    public Agent(Platform platform, Time time, JsonRestServer restServer,
                 AgentRestResource resource) {
        this.platform = platform;
        this.time = time;
        this.restServer = restServer;
        this.startTimeMs = time.milliseconds();
        this.runnable = new AgentRunnable();
        this.thread = new KafkaThread("TrogdorAgentThread", runnable, false);
        this.thread.start();
        resource.setAgent(this);
    }

    public int port() {
        return this.restServer.port();
    }

    public void beginShutdown() {
        lock.lock();
        try {
            if (shutdown)
                return;
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

    public AgentFaultsResponse faults() {
        Map<String, AgentFaultsResponse.FaultData> faultData = new TreeMap<>();
        lock.lock();
        try {
            updateFaultsResponse(faultData, pendingFaults);
            updateFaultsResponse(faultData, runningFaults);
            updateFaultsResponse(faultData, doneFaults);
        } finally {
            lock.unlock();
        }
        return new AgentFaultsResponse(faultData);
    }

    private void updateFaultsResponse(Map<String, AgentFaultsResponse.FaultData> faultData,
                                      FaultSet faultSet) {
        for (Iterator<Fault> iter = faultSet.iterateByStart();
                iter.hasNext(); ) {
            Fault fault = iter.next();
            AgentFaultsResponse.FaultData data =
                new AgentFaultsResponse.FaultData(fault.spec(), fault.state());
            faultData.put(fault.id(), data);
        }
    }

    public void createFault(CreateAgentFaultRequest request) throws ClassNotFoundException {
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
            .newArgumentParser("trogdor-agent")
            .defaultHelp(true)
            .description("The Trogdor fault injection agent");
        parser.addArgument("--agent.config")
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
        JsonRestServer restServer =
            new JsonRestServer(Node.Util.getTrogdorAgentPort(platform.curNode()));
        AgentRestResource resource = new AgentRestResource();
        final Agent agent = new Agent(platform, Time.SYSTEM, restServer, resource);
        restServer.start(resource);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.error("Running shutdown hook...");
                agent.beginShutdown();
                agent.waitForShutdown();
            }
        });
        agent.waitForShutdown();
    }
};
