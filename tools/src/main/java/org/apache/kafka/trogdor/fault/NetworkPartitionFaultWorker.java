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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.Topology;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

public class NetworkPartitionFaultWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(NetworkPartitionFaultWorker.class);

    private final String id;

    private final List<Set<String>> partitionSets;

    public NetworkPartitionFaultWorker(String id, List<Set<String>> partitionSets) {
        this.id = id;
        this.partitionSets = partitionSets;
    }

    @Override
    public void start(Platform platform, AtomicReference<String> status,
                      KafkaFutureImpl<String> errorFuture) throws Exception {
        log.info("Activating NetworkPartitionFault {}.", id);
        runIptablesCommands(platform, "-A");
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("Deactivating NetworkPartitionFault {}.", id);
        runIptablesCommands(platform, "-D");
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
