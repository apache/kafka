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

package org.apache.kafka.soak.cluster;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.ActionScheduler;
import org.apache.kafka.soak.cloud.Cloud;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.BrokerRole;
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.role.ZooKeeperRole;
import org.apache.kafka.soak.tool.SoakEnvironment;
import org.apache.kafka.soak.tool.SoakShutdownManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The SoakCluster.
 */
public final class SoakCluster implements AutoCloseable {
    private final SoakEnvironment env;
    private final Cloud cloud;
    private final SoakLog clusterLog;
    private final Map<String, SoakNode> nodes;
    private final SoakShutdownManager shutdownManager;

    public SoakCluster(SoakEnvironment env, Cloud cloud, SoakLog clusterLog,
                       SoakClusterSpec spec) throws IOException {
        this.env = env;
        this.cloud = cloud;
        this.clusterLog = clusterLog;
        TreeMap<String, SoakNode> nodes = new TreeMap<>();
        int nodeIndex = 0;
        for (Map.Entry<String, SoakNodeSpec> entry : spec.nodes().entrySet()) {
            String nodeName = entry.getKey();
            SoakNodeSpec nodeSpec = entry.getValue();
            SoakLog soakLog = SoakLog.fromFile(env.outputDirectory(), nodeName, true);
            SoakNode node = new SoakNode(nodeIndex, nodeName, soakLog, nodeSpec);
            nodes.put(nodeName, node);
            nodeIndex++;
        }
        this.nodes = nodes;
        this.shutdownManager = new SoakShutdownManager(clusterLog);
    }

    public SoakLog clusterLog() {
        return clusterLog;
    }

    public Map<String, SoakNode> nodes() {
        return nodes;
    }

    public Collection<SoakNode> nodes(String... names) {
        List<SoakNode> foundNodes = new ArrayList<>();
        for (String name : names) {
            foundNodes.add(nodes.get(name));
        }
        return foundNodes;
    }

    /**
     * Find nodes with a given role.
     *
     * @param roleClass     The role class.
     *
     * @return              A map from monontonically increasing integers to the names of
     *                      nodes.  The map will contain only nodes with the given role.
     */
    public TreeMap<Integer, String> nodesWithRole(Class<? extends Role> roleClass) {
        TreeMap<Integer, String> results = new TreeMap<>();
        int index = 0;
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            String nodeName = entry.getKey();
            SoakNode soakNode = entry.getValue();
            for (Role role : soakNode.spec().roles()) {
                if (roleClass.isInstance(role)) {
                    results.put(index, nodeName);
                    break;
                }
            }
            index++;
        }
        return results;
    }

    public String getBootstrapServers() {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (String nodeName : nodesWithRole(BrokerRole.class).values()) {
            bld.append(prefix);
            prefix = ",";
            bld.append(nodes().get(nodeName).spec().privateDns()).append(":9092");
        }
        return bld.toString();
    }

    public String getZooKeeperConnectString() {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (String nodeName : nodesWithRole(ZooKeeperRole.class).values()) {
            bld.append(prefix);
            prefix = ",";
            bld.append(nodes().get(nodeName).spec().privateDns()).append(":2181");
        }
        return bld.toString();
    }

    public Collection<String> getSoakNodesByNamesOrIndices(List<String> args) {
        if (args.contains("all")) {
            if (args.size() > 1) {
                throw new RuntimeException("Can't specify both 'all' and other node name(s).");
            }
            return Collections.unmodifiableSet(nodes.keySet());
        }
        TreeSet<String> nodesNames = new TreeSet<>();
        for (String arg : args) {
            nodesNames.add(getSoakNodeByNameOrIndex(arg));
        }
        return nodesNames;
    }

    public String getSoakNodeByNameOrIndex(String arg) {
        if (nodes.get(arg) != null) {
            // The argument was the name of a node.
            return arg;
        }
        // Try to parse the argument as a node number.
        int nodeIndex = -1;
        try {
            nodeIndex = Integer.parseInt(arg);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to find a node named " + arg);
        }
        int i = 0;
        for (Iterator<String> iter = nodes.keySet().iterator(); iter.hasNext(); ) {
            String entry = iter.next();
            if (i >= nodeIndex) {
                return entry;
            }
            i++;
        }
        throw new RuntimeException("Unable to find a node with index " +
            nodeIndex + "; we have only " + nodes.size() + " node(s).");
    }

    @Override
    public void close() {
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            Utils.closeQuietly(entry.getValue(), "cluster soakLogs");
        }
    }

    public SoakEnvironment env() {
        return env;
    }

    public Cloud cloud() {
        return cloud;
    }

    public synchronized SoakClusterSpec toSpec() {
        TreeMap<String, SoakNodeSpec> nodeSpecs = new TreeMap<>();
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            nodeSpecs.put(entry.getKey(), entry.getValue().spec());
        }
        return new SoakClusterSpec(nodeSpecs);
    }

    /**
     * Create a new action scheduler.
     *
     * @param targetNames           The targets to execute.
     * @param additionalActions     Some additional actions to add to our scheduler.  We will
     *                              also add the actions corresponding to the cluster roles.
     * @return                      The new scheduler.
     */
    public ActionScheduler createScheduler(List<String> targetNames,
                Collection<Action> additionalActions) throws Exception {
        ActionScheduler.Builder builder = new ActionScheduler.Builder(this);
        builder.addTargetNames(targetNames);
        builder.addActions(additionalActions);
        for (SoakNode node : nodes.values()) {
            for (Role role : node.spec().roles()) {
                builder.addActions(role.createActions(node.nodeName()));
            }
        }
        return builder.build();
    }

    public SoakShutdownManager shutdownManager() {
        return shutdownManager;
    }
}
