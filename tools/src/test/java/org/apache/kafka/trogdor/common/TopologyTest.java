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

import org.apache.kafka.trogdor.agent.Agent;
import org.apache.kafka.trogdor.basic.BasicNode;
import org.apache.kafka.trogdor.basic.BasicTopology;

import org.apache.kafka.trogdor.coordinator.Coordinator;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TopologyTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testAgentNodeNames() throws Exception {
        TreeMap<String, Node> nodes = new TreeMap<>();
        final int numNodes = 5;
        for (int i = 0; i < numNodes; i++) {
            HashMap<String, String> conf = new HashMap<>();
            if (i == 0) {
                conf.put(Platform.Config.TROGDOR_COORDINATOR_PORT, String.valueOf(Coordinator.DEFAULT_PORT));
            } else {
                conf.put(Platform.Config.TROGDOR_AGENT_PORT, String.valueOf(Agent.DEFAULT_PORT));
            }
            BasicNode node = new BasicNode(String.format("node%02d", i),
                String.format("node%d.example.com", i),
                conf,
                new HashSet<String>());
            nodes.put(node.name(), node);
        }
        Topology topology = new BasicTopology(nodes);
        Set<String> names = Topology.Util.agentNodeNames(topology);
        assertEquals(4, names.size());
        for (int i = 1; i < numNodes - 1; i++) {
            assertTrue(names.contains(String.format("node%02d", i)));
        }
    }
};
