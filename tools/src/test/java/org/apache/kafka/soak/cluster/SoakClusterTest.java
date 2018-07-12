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

import org.apache.kafka.soak.role.BrokerRole;
import org.apache.kafka.soak.role.TrogdorAgentRole;
import org.apache.kafka.soak.role.TrogdorCoordinatorRole;
import org.apache.kafka.soak.role.ZooKeeperRole;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class SoakClusterTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private MiniSoakCluster createMiniSoakCluster() throws Exception {
        MiniSoakCluster.Builder builder = new MiniSoakCluster.Builder();
        builder.addRole("broker",
            new BrokerRole(0, Collections.emptyMap(), ""),
            "node0", "node1", "node2");
        builder.addRole("zookeeper",
            new ZooKeeperRole(0),
            "node3");
        builder.addRole("trogdorAgentRole",
            new TrogdorAgentRole(0),
            "node0", "node1", "node2");
        builder.addRole("trogdorCoordinatorRole",
            new TrogdorCoordinatorRole(0),
            "node3");
        return builder.build();
    }

    @Test
    public void testNodesWithRole() throws Exception {
        try (MiniSoakCluster miniCluster = createMiniSoakCluster()) {
            TreeMap<Integer, String> nodes =
                miniCluster.cluster().nodesWithRole(ZooKeeperRole.class);
            assertEquals(1, nodes.size());
            Map.Entry<Integer, String> entry = nodes.entrySet().iterator().next();
            assertEquals(Integer.valueOf(3), entry.getKey());
            assertEquals("node3", entry.getValue());
        }
    }

    @Test
    public void testGetSoakNodesByNameOrIndices() throws Exception {
        try (MiniSoakCluster miniCluster = createMiniSoakCluster()) {
            String[] nodeNames = miniCluster.cluster().
                getSoakNodesByNamesOrIndices(Arrays.asList("node0", "node2")).
                toArray(new String[0]);
            assertEquals(2, nodeNames.length);
            assertEquals("node0", nodeNames[0]);
            assertEquals("node2", nodeNames[1]);

            String[] nodeNames2 = miniCluster.cluster().
                getSoakNodesByNamesOrIndices(Arrays.asList("1", "3")).
                toArray(new String[0]);
            assertEquals(2, nodeNames2.length);
            assertEquals("node1", nodeNames2[0]);
            assertEquals("node3", nodeNames2[1]);

            String[] nodeNames3 = miniCluster.cluster().
                getSoakNodesByNamesOrIndices(Arrays.asList("all")).
                toArray(new String[0]);
            assertEquals(4, nodeNames3.length);
            assertEquals("node0", nodeNames3[0]);
            assertEquals("node1", nodeNames3[1]);
            assertEquals("node2", nodeNames3[2]);
            assertEquals("node3", nodeNames3[3]);
        }
    }
}
