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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.server.util.ServerTestUtils;

import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerMetricNamesTest {

    private static final String REPLICA_MANAGER_PREFIX = "kafka.server:type=ReplicaManager,";

    @AfterEach
    public void tearDown() {
        ServerTestUtils.clearYammerMetrics();
    }

    @ClusterTest
    public void testMetrics() {
        Set<String> expected = Set.of(
            "kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount",
            "kafka.server:type=ReplicaManager,name=FailedIsrUpdatesPerSec",
            "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec",
            "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec",
            "kafka.server:type=ReplicaManager,name=LeaderCount",
            "kafka.server:type=ReplicaManager,name=OfflineReplicaCount",
            "kafka.server:type=ReplicaManager,name=PartitionCount",
            "kafka.server:type=ReplicaManager,name=PartitionsWithLateTransactionsCount",
            "kafka.server:type=ReplicaManager,name=ProducerIdCount",
            "kafka.server:type=ReplicaManager,name=ReassigningPartitions",
            "kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount",
            "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"
        );
        Set<String> actual = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .map(MetricName::getMBeanName)
            .filter(name -> name.startsWith(REPLICA_MANAGER_PREFIX))
            .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(expected), actual);
    }
}
