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
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.role.TrogdorAgentRole;
import org.apache.kafka.soak.role.TrogdorCoordinatorRole;
import org.apache.kafka.soak.role.ZooKeeperRole;

import java.util.Arrays;
import java.util.List;

/**
 * Test fixtures for unit testing the soak test framework.
 */
public class TestFixture {
    /**
     * Create a cluster with many nodes and roles.
     */
    public static MiniSoakCluster createRoleTestCluster() throws Exception {
        MiniSoakCluster.Builder builder = new MiniSoakCluster.Builder();

        List<Role> roles0 = Arrays.asList(new Role[] {
            new BrokerRole(null, null),
            new TrogdorAgentRole()
        });
        builder.addNodeWithInstanceId("broker0", new SoakNodeSpec(roles0));
        builder.addNodeWithInstanceId("broker1", new SoakNodeSpec(roles0));
        builder.addNodeWithInstanceId("broker2", new SoakNodeSpec(roles0));

        List<Role> roles1 = Arrays.asList(new Role[] {
            new ZooKeeperRole(),
            new TrogdorAgentRole()
        });
        builder.addNodeWithInstanceId("zk0", new SoakNodeSpec(roles1));
        builder.addNodeWithInstanceId("zk1", new SoakNodeSpec(roles1));
        builder.addNodeWithInstanceId("zk2", new SoakNodeSpec(roles1));

        List<Role> roles2 = Arrays.asList(new Role[] {
            new TrogdorCoordinatorRole()
        });
        builder.addNodeWithInstanceId("trogdor0", new SoakNodeSpec(roles2));

        return builder.build();
    }
};
