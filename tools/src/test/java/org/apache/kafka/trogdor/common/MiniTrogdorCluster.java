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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.agent.Agent;
import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.agent.AgentRestResource;
import org.apache.kafka.trogdor.basic.BasicNode;
import org.apache.kafka.trogdor.basic.BasicPlatform;
import org.apache.kafka.trogdor.basic.BasicTopology;
import org.apache.kafka.trogdor.coordinator.Coordinator;

import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.coordinator.CoordinatorRestResource;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MiniTrogdorCluster sets up a local cluster of Trogdor Agents and Coordinators.
 */
public class MiniTrogdorCluster implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MiniTrogdorCluster.class);

    /**
     * The MiniTrogdorCluster#Builder is used to set up a new MiniTrogdorCluster.
     */
    public static class Builder {
        private final TreeSet<String> agentNames = new TreeSet<>();

        private String coordinatorName = null;

        private Scheduler scheduler = Scheduler.SYSTEM;

        private BasicPlatform.CommandRunner commandRunner =
                new BasicPlatform.ShellCommandRunner();

        private static class NodeData {
            String hostname;
            AgentRestResource agentRestResource = null;
            JsonRestServer agentRestServer = null;
            int agentPort = 0;

            JsonRestServer coordinatorRestServer = null;
            int coordinatorPort = 0;
            CoordinatorRestResource coordinatorRestResource = null;

            Platform platform = null;
            Agent agent = null;
            Coordinator coordinator = null;

            BasicNode node = null;
        }

        public Builder() {
        }

        /**
         * Set the timekeeper used by this MiniTrogdorCluster.
         */
        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder commandRunner(BasicPlatform.CommandRunner commandRunner) {
            this.commandRunner = commandRunner;
            return this;
        }

        /**
         * Add a new trogdor coordinator node to the cluster.
         */
        public Builder addCoordinator(String nodeName) {
            if (coordinatorName != null) {
                throw new RuntimeException("At most one coordinator is allowed.");
            }
            coordinatorName = nodeName;
            return this;
        }

        /**
         * Add a new trogdor agent node to the cluster.
         */
        public Builder addAgent(String nodeName) {
            if (agentNames.contains(nodeName)) {
                throw new RuntimeException("There is already an agent on node " + nodeName);
            }
            agentNames.add(nodeName);
            return this;
        }

        private NodeData getOrCreate(String nodeName, TreeMap<String, NodeData> nodes) {
            NodeData data = nodes.get(nodeName);
            if (data != null)
                return data;
            data = new NodeData();
            data.hostname = "127.0.0.1";
            nodes.put(nodeName, data);
            return data;
        }

        /**
         * Create the MiniTrogdorCluster.
         */
        public MiniTrogdorCluster build() throws Exception {
            log.info("Creating MiniTrogdorCluster with agents: {} and coordinator: {}",
                Utils.join(agentNames, ", "), coordinatorName);
            TreeMap<String, NodeData> nodes = new TreeMap<>();
            for (String agentName : agentNames) {
                NodeData node = getOrCreate(agentName, nodes);
                node.agentRestResource = new AgentRestResource();
                node.agentRestServer = new JsonRestServer(0);
                node.agentRestServer.start(node.agentRestResource);
                node.agentPort = node.agentRestServer.port();
            }
            if (coordinatorName != null) {
                NodeData node = getOrCreate(coordinatorName, nodes);
                node.coordinatorRestResource = new CoordinatorRestResource();
                node.coordinatorRestServer = new JsonRestServer(0);
                node.coordinatorRestServer.start(node.coordinatorRestResource);
                node.coordinatorPort = node.coordinatorRestServer.port();
            }
            for (Map.Entry<String, NodeData> entry : nodes.entrySet()) {
                NodeData node = entry.getValue();
                HashMap<String, String> config = new HashMap<>();
                if (node.agentPort != 0) {
                    config.put(Platform.Config.TROGDOR_AGENT_PORT,
                        Integer.toString(node.agentPort));
                }
                if (node.coordinatorPort != 0) {
                    config.put(Platform.Config.TROGDOR_COORDINATOR_PORT,
                        Integer.toString(node.coordinatorPort));
                }
                node.node = new BasicNode(entry.getKey(), node.hostname, config,
                    Collections.<String>emptySet());
            }
            TreeMap<String, Node> topologyNodes = new TreeMap<>();
            for (Map.Entry<String, NodeData> entry : nodes.entrySet()) {
                topologyNodes.put(entry.getKey(), entry.getValue().node);
            }
            final BasicTopology topology = new BasicTopology(topologyNodes);
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
                ThreadUtils.createThreadFactory("MiniTrogdorClusterStartupThread%d", false));
            final AtomicReference<Exception> failure = new AtomicReference<Exception>(null);
            for (final Map.Entry<String, NodeData> entry : nodes.entrySet()) {
                executor.submit((Callable<Void>) () -> {
                    String nodeName = entry.getKey();
                    try {
                        NodeData node = entry.getValue();
                        node.platform = new BasicPlatform(nodeName, topology, scheduler, commandRunner);
                        if (node.agentRestResource != null) {
                            node.agent = new Agent(node.platform, scheduler, node.agentRestServer,
                                node.agentRestResource);
                        }
                        if (node.coordinatorRestResource != null) {
                            node.coordinator = new Coordinator(node.platform, scheduler,
                                node.coordinatorRestServer, node.coordinatorRestResource, 0);
                        }
                    } catch (Exception e) {
                        log.error("Unable to initialize {}", nodeName, e);
                        failure.compareAndSet(null, e);
                    }
                    return null;
                });
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
            Exception failureException = failure.get();
            if (failureException != null) {
                throw failureException;
            }

            TreeMap<String, Agent> agents = new TreeMap<>();
            Coordinator coordinator = null;
            for (Map.Entry<String, NodeData> entry : nodes.entrySet()) {
                NodeData node = entry.getValue();
                if (node.agent != null) {
                    agents.put(entry.getKey(), node.agent);
                }
                if (node.coordinator != null) {
                    coordinator = node.coordinator;
                }
            }
            return new MiniTrogdorCluster(scheduler, agents, nodes, coordinator);
        }
    }

    private final TreeMap<String, Agent> agents;

    private final TreeMap<String, Builder.NodeData> nodesByAgent;

    private final Coordinator coordinator;

    private final Scheduler scheduler;

    private MiniTrogdorCluster(Scheduler scheduler,
                               TreeMap<String, Agent> agents,
                               TreeMap<String, Builder.NodeData> nodesByAgent,
                               Coordinator coordinator) {
        this.scheduler = scheduler;
        this.agents = agents;
        this.nodesByAgent = nodesByAgent;
        this.coordinator = coordinator;
    }

    public TreeMap<String, Agent> agents() {
        return agents;
    }

    public Coordinator coordinator() {
        return coordinator;
    }

    public CoordinatorClient coordinatorClient() {
        if (coordinator == null) {
            throw new RuntimeException("No coordinator configured.");
        }
        return new CoordinatorClient.Builder().
            maxTries(10).
            target("localhost", coordinator.port()).
            build();
    }

    /**
     * Mimic a restart of a Trogdor agent, essentially cleaning out all of its active workers
     */
    public void restartAgent(String nodeName) {
        if (!agents.containsKey(nodeName)) {
            throw new RuntimeException("There is no agent on node " + nodeName);
        }
        Builder.NodeData node = nodesByAgent.get(nodeName);
        agents.put(nodeName, new Agent(node.platform, scheduler, node.agentRestServer, node.agentRestResource));
    }

    public AgentClient agentClient(String nodeName) {
        Agent agent = agents.get(nodeName);
        if (agent == null) {
            throw new RuntimeException("No agent configured on node " + nodeName);
        }
        return new AgentClient.Builder().
            maxTries(10).
            target("localhost", agent.port()).
            build();
    }

    @Override
    public void close() throws Exception {
        log.info("Closing MiniTrogdorCluster.");
        if (coordinator != null) {
            coordinator.beginShutdown(false);
        }
        for (Agent agent : agents.values()) {
            agent.beginShutdown();
        }
        for (Agent agent : agents.values()) {
            agent.waitForShutdown();
        }
        if (coordinator != null) {
            coordinator.waitForShutdown();
        }
    }
};
