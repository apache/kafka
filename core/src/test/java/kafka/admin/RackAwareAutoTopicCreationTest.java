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
package kafka.admin;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(types = {Type.KRAFT},
    brokers = 4,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.NUM_PARTITIONS_CONFIG, value = "8"),
        @ClusterConfigProperty(key = ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(id = 0, key = ServerConfigs.BROKER_RACK_CONFIG, value = "0"),
        @ClusterConfigProperty(id = 1, key = ServerConfigs.BROKER_RACK_CONFIG, value = "0"),
        @ClusterConfigProperty(id = 2, key = ServerConfigs.BROKER_RACK_CONFIG, value = "1"),
        @ClusterConfigProperty(id = 3, key = ServerConfigs.BROKER_RACK_CONFIG, value = "1"),
    })
public class RackAwareAutoTopicCreationTest {

    private static final String TOPIC = "topic";

    @ClusterTest
    public void testAutoCreateTopic(ClusterInstance cluster) throws Exception {

        try (Admin admin = cluster.admin();
             Producer<byte[], byte[]> producer = cluster.producer()) {

            // send a record to trigger auto create topic
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, null, "key".getBytes(), "value".getBytes());
            assertEquals(0L, producer.send(record).get().offset(), "Should have offset 0");

            // check broker rack content
            Map<Integer, String> expectedBrokerToRackMap = Map.of(
                    0, "0",
                    1, "0",
                    2, "1",
                    3, "1"
            );
            Map<Integer, String> actualBrokerToRackMap = getBrokerToRackMap(cluster);
            assertEquals(expectedBrokerToRackMap, actualBrokerToRackMap);

            // get topic assignments and check it's content
            Map<Integer, List<Integer>> assignments = getTopicAssignment(admin);
            for (List<Integer> brokerList : assignments.values()) {
                assertEquals(new HashSet<>(brokerList).size(), brokerList.size(),
                        "More than one replica is assigned to same broker for the same partition");
            }

            // check rack count for each partition
            ReplicaDistributions distribution = getReplicaDistribution(assignments, expectedBrokerToRackMap);
            Map<String, String> serverProperties = cluster.config().serverProperties();
            int numPartition = Integer.parseInt(serverProperties.get(ServerLogConfigs.NUM_PARTITIONS_CONFIG));
            int replicationFactor = Integer.parseInt(serverProperties.get(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG));
            List<Integer> expectedRackCounts = Collections.nCopies(numPartition, replicationFactor);
            List<Integer> actualRackCounts = distribution.partitionToRackMap.values().stream()
                    .map(racks -> (int) racks.stream().distinct().count())
                    .collect(Collectors.toList());
            assertEquals(expectedRackCounts, actualRackCounts,
                    "More than one replica of the same partition is assigned to the same rack");

            // check replica count for each partition
            int numBrokers = cluster.brokers().size();
            int numReplicasPerBroker = numPartition * replicationFactor / numBrokers;
            List<Integer> expectedReplicasCounts = Collections.nCopies(numBrokers, numReplicasPerBroker);
            List<Integer> actualReplicasCounts = new ArrayList<>(distribution.partitionToCountMap.values());
            assertEquals(expectedReplicasCounts, actualReplicasCounts, "Replica count is not even for broker");
        }
    }

    private static Map<Integer, List<Integer>> getTopicAssignment(Admin admin) throws Exception {
        TopicDescription topicDescription = admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC);
        return topicDescription.partitions().stream()
                .collect(Collectors.toMap(
                        TopicPartitionInfo::partition,
                        p -> p.replicas().stream().map(Node::id).collect(Collectors.toList())));
    }

    private static Map<Integer, String> getBrokerToRackMap(ClusterInstance cluster) {
        Map<Integer, String> actualBrokerToRackMap = new HashMap<>();
        Iterator<BrokerMetadata> iterator = cluster.brokers().get(0).metadataCache().getAliveBrokers().iterator();
        while (iterator.hasNext()) {
            BrokerMetadata metadata = iterator.next();
            actualBrokerToRackMap.put(metadata.id, metadata.rack.get());
        }
        return actualBrokerToRackMap;
    }


    private static ReplicaDistributions getReplicaDistribution(Map<Integer, List<Integer>> assignment,
                                                               Map<Integer, String> brokerRackMapping) {
        Map<Integer, List<String>> partitionToRackMap = new HashMap<>();
        Map<Integer, Integer> partitionToCountMap = new HashMap<>();

        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            int partitionId = entry.getKey();
            List<Integer> replicaList = entry.getValue();

            for (int brokerId : replicaList) {
                partitionToCountMap.put(brokerId, partitionToCountMap.getOrDefault(brokerId, 0) + 1);
                String rack = brokerRackMapping.get(brokerId);
                if (rack == null) {
                    throw new RuntimeException("No mapping found for " + brokerId + " in `brokerRackMapping`");
                }
                partitionToRackMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(rack);
            }
        }
        return new ReplicaDistributions(partitionToRackMap, partitionToCountMap);
    }

    private record ReplicaDistributions(Map<Integer, List<String>> partitionToRackMap,
                                        Map<Integer, Integer> partitionToCountMap) {
    }
}
