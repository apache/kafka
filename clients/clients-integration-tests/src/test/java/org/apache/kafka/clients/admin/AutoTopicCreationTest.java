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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 2)
public class AutoTopicCreationTest {

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = "num.partitions", value = "5"),
            @ClusterConfigProperty(id = 0, key = "default.replication.factor", value = "2"),
            @ClusterConfigProperty(id = 1, key = "num.partitions", value = "5"),
            @ClusterConfigProperty(id = 1, key = "default.replication.factor", value = "2"),
        }
    )
    public void testAutoCreateTopicWithExplicitBrokerConfig(ClusterInstance cluster) throws Exception {
        String topic = "explicit-broker-topic";
        triggerAutoCreateTopic(cluster, topic);
        try (Admin admin = cluster.admin()) {
            TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            assertEquals(5, desc.partitions().size(),
                "num.partitions explicitly set on broker should be used");
            assertEquals(2, desc.partitions().get(0).replicas().size(),
                "default.replication.factor explicitly set on broker should be used");
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(id = 3000, key = "num.partitions", value = "5"),
            @ClusterConfigProperty(id = 3000, key = "default.replication.factor", value = "2"),
        }
    )
    public void testAutoCreateTopicWithImplicitBrokerConfig(ClusterInstance cluster) throws Exception {
        String topic = "implicit-broker-topic";
        triggerAutoCreateTopic(cluster, topic);
        try (Admin admin = cluster.admin()) {
            TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            assertEquals(5, desc.partitions().size(),
                "Controller num.partitions should be used when broker does not explicitly set it");
            assertEquals(2, desc.partitions().get(0).replicas().size(),
                "Controller default.replication.factor should be used when broker does not explicitly set it");
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = "num.partitions", value = "5"),
            @ClusterConfigProperty(id = 1, key = "num.partitions", value = "5"),
            @ClusterConfigProperty(id = 3000, key = "default.replication.factor", value = "2"),
        }
    )
    public void testAutoCreateTopicWithMixedConfig(ClusterInstance cluster) throws Exception {
        String topic = "mixed-config-topic";
        triggerAutoCreateTopic(cluster, topic);
        try (Admin admin = cluster.admin()) {
            TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            assertEquals(5, desc.partitions().size(),
                "num.partitions explicitly set on broker should be used");
            assertEquals(2, desc.partitions().get(0).replicas().size(),
                "Controller default.replication.factor should be used when broker does not set it");
        }
    }

    @ClusterTest
    public void testAutoCreateTopicWithDefaultConfig(ClusterInstance cluster) throws Exception {
        String topic = "default-config-topic";
        triggerAutoCreateTopic(cluster, topic);
        try (Admin admin = cluster.admin()) {
            TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            assertEquals(1, desc.partitions().size(),
                "Default num.partitions of 1 should be used when neither broker nor controller sets it");
            assertEquals(1, desc.partitions().get(0).replicas().size(),
                "Default default.replication.factor of 1 should be used when neither broker nor controller sets it");
        }
    }

    private void triggerAutoCreateTopic(ClusterInstance cluster, String topic) throws Exception {
        // Sends a produce request to a non-existent topic so that auto topic creation is triggered.
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, null, "key".getBytes(), "value".getBytes());
            producer.send(record).get();
        }
    }
}
