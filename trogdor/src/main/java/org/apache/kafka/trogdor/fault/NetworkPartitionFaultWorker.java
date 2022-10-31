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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.Topology;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class NetworkPartitionFaultWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(NetworkPartitionFaultWorker.class);

    private final String id;

    private final List<Set<String>> partitionSets;

    private WorkerStatusTracker status;

    public NetworkPartitionFaultWorker(String id, List<Set<String>> partitionSets) {
        this.id = id;
        this.partitionSets = partitionSets;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> errorFuture) throws Exception {
        log.info("Activating NetworkPartitionFault {}.", id);
        this.status = status;
        this.status.update(new TextNode("creating network partition " + id));
        runIptablesCommands(platform, "-A");
        this.status.update(new TextNode("created network partition " + id));
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("Deactivating NetworkPartitionFault {}.", id);
        this.status.update(new TextNode("removing network partition " + id));
        runIptablesCommands(platform, "-D");
        this.status.update(new TextNode("removed network partition " + id));
    }

    private void runIptablesCommands(Platform platform, String iptablesAction) throws Exception {
        Node curNode = platform.curNode();
        Topology topology = platform.topology();
        TreeSet<String> toBlock = new TreeSet<>();
        for (Set<String> partitionSet : partitionSets) {
            if (!partitionSet.contains(curNode.name())) {
                for (String nodeName : partitionSet) {
                    toBlock.add(nodeName);
                }
            }
        }
        for (String nodeName : toBlock) {
            Node node = topology.node(nodeName);
            InetAddress addr = InetAddress.getByName(node.hostname());
            platform.runCommand(new String[] {
                "sudo", "iptables", iptablesAction, "INPUT", "-p", "tcp", "-s",
                addr.getHostAddress(), "-j", "DROP", "-m", "comment", "--comment", nodeName
            });
        }
    }
}
