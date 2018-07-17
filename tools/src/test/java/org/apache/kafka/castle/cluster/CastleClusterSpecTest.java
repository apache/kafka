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

package org.apache.kafka.castle.cluster;

import org.apache.kafka.castle.common.CastleLog;
import org.apache.kafka.castle.role.BrokerRole;
import org.apache.kafka.castle.role.Role;
import org.apache.kafka.castle.role.TrogdorAgentRole;
import org.apache.kafka.castle.role.TrogdorCoordinatorRole;
import org.apache.kafka.castle.role.ZooKeeperRole;
import org.apache.kafka.castle.tool.CastleEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CastleClusterSpecTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private CastleClusterSpec createCastleClusterSpec() throws Exception {
        Map<String, CastleNodeSpec> map = new HashMap<>();
        CastleNodeSpec specA = new CastleNodeSpec(
            Arrays.asList(new String[] {"broker", "trogdorAgent"}));
        map.put("node[0-2]", specA);
        CastleNodeSpec specB = new CastleNodeSpec(
            Arrays.asList(new String[] {"zooKeeper", "trogdorCoordinator"}));
        map.put("node3", specB);
        Map<String, Role> roles = new HashMap<>();
        roles.put("broker", new BrokerRole(0, Collections.emptyMap(), ""));
        roles.put("trogdorAgent", new TrogdorAgentRole(0));
        roles.put("zooKeeper", new ZooKeeperRole(0));
        roles.put("trogdorCoordinator", new TrogdorCoordinatorRole(0));
        return new CastleClusterSpec(map, roles);
    }

    @Test
    public void testNodeNames() throws Exception {
        CastleClusterSpec clusterSpec = createCastleClusterSpec();
        assertEquals(new HashSet<>(Arrays.asList(
                new String[] {"node0", "node1", "node2", "node3"})),
            clusterSpec.nodes().keySet());
        assertEquals(Arrays.asList(new String[] {"broker", "trogdorAgent"}),
            clusterSpec.nodes().get("node0").roleNames());
        assertEquals(Arrays.asList(new String[] {"broker", "trogdorAgent"}),
            clusterSpec.nodes().get("node1").roleNames());
        assertEquals(Arrays.asList(new String[] {"broker", "trogdorAgent"}),
            clusterSpec.nodes().get("node2").roleNames());
        assertEquals(Arrays.asList(new String[] {"zooKeeper", "trogdorCoordinator"}),
            clusterSpec.nodes().get("node3").roleNames());
    }

    @Test
    public void testToCastleCluster() throws Exception {
        CastleClusterSpec clusterSpec = createCastleClusterSpec();
        CastleCluster cluster = new CastleCluster(
            new CastleEnvironment("", "", 360, "", ""),
            CastleLog.fromStdout("cluster", true),
            clusterSpec);
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{"node0", "node1", "node2", "node3"})),
            cluster.nodes().keySet());
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{"node0", "node1", "node2"})),
            new HashSet<>(cluster.nodesWithRole(TrogdorAgentRole.class).values()));
        CastleClusterSpec clusterSpec2 = cluster.toSpec();
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{"node0", "node1", "node2", "node3"})),
            clusterSpec2.nodes().keySet());
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{"broker", "trogdorAgent", "trogdorCoordinator", "zooKeeper"})),
            clusterSpec2.roles().keySet());
        for (String nodeName : new String[] {"node0", "node1", "node2"}) {
            assertEquals(new HashSet<>(Arrays.asList(
                new String[]{"broker", "trogdorAgent"})),
                new HashSet<>(clusterSpec2.nodes().get(nodeName).roleNames()));
            assertEquals(new HashSet<>(Arrays.asList(
                new String[]{BrokerRole.class.getName(),
                    TrogdorAgentRole.class.getName()})),
                clusterSpec2.nodesToRoles().get(nodeName).values().stream().map(new Function<Role, String>() {
                    @Override
                    public String apply(Role role) {
                        return role.getClass().getName();
                    }
                }).collect(Collectors.toSet()));
        }
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{"trogdorCoordinator", "zooKeeper"})),
            new HashSet<>(clusterSpec2.nodes().get("node3").roleNames()));
        assertEquals(new HashSet<>(Arrays.asList(
            new String[]{TrogdorCoordinatorRole.class.getName(),
                ZooKeeperRole.class.getName()})),
            clusterSpec2.nodesToRoles().get("node3").values().stream().map(new Function<Role, String>() {
                @Override
                public String apply(Role role) {
                    return role.getClass().getName();
                }
            }).collect(Collectors.toSet()));
    }
}
