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

package org.apache.kafka.testkit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MiniKafkaClusterTest {
    private static final Logger log = LoggerFactory.getLogger(MiniKafkaClusterTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateDestroy() throws Exception {
        try (MiniKafkaCluster cluster = new MiniKafkaClusterBuilder()
                .addZookeeperNode(new MiniZookeeperNodeBuilder())
                .addNode(new MiniKafkaNodeBuilder())
                .build()) {
            assertEquals(1, cluster.kafkas().values().size());
            MiniKafkaNode kafka = cluster.kafkas().values().iterator().next();
            int maxReservedBrokerId = Integer.valueOf(kafka.config("reserved.broker.max.id"));
            assertTrue(maxReservedBrokerId <= kafka.id());
        }
    }

    @Test
    public void testThreeNodeCluster() throws Exception {
        try (MiniKafkaCluster cluster = new MiniKafkaClusterBuilder()
                .addZookeeperNode(new MiniZookeeperNodeBuilder())
                .addNode(new MiniKafkaNodeBuilder().id(1))
                .addNode(new MiniKafkaNodeBuilder().id(2))
                .addNode(new MiniKafkaNodeBuilder().id(3))
                .build()) {
            assertTrue(cluster.zkString().startsWith("localhost:"));
            assertEquals(1, cluster.kafkas().get(1).id().intValue());
            assertEquals(2, cluster.kafkas().get(2).id().intValue());
            assertEquals(3, cluster.kafkas().get(3).id().intValue());
        }
    }
}
