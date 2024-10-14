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
package kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;

import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(ClusterTestExtensions.class)
public class AsyncKafkaConsumerIntegrationTest {
    private ClusterInstance clusterInstance;

    @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
            @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic")
    })
    public void testAsyncConsumerWithoutConsumerRebalanceProtocol(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        checkUnsupportedConsumerGroupHeartbeat();
    }

    @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false")
    })
    public void testAsyncConsumerWithOldGroupCoordinator(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        checkUnsupportedConsumerGroupHeartbeat();
    }

    private void createTopic(String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).topicId(topic).get());
        }

        assertDoesNotThrow(() -> clusterInstance.waitForTopic(topic, 1));
    }

    private KafkaConsumer<String, String> createAsyncKafkaConsumer(String groupId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name());
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        return new KafkaConsumer<>(configs);
    }

    private void checkUnsupportedConsumerGroupHeartbeat() throws Exception {
        String topic = "test-topic";
        createTopic(topic);
        try (KafkaConsumer<String, String> consumer = createAsyncKafkaConsumer("test-group")) {
            consumer.subscribe(Collections.singletonList(topic));
            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return false;
                } catch (UnsupportedVersionException e) {
                    return e.getMessage().contains("The cluster doesn't yet support the new consumer group protocol. " +
                            "Set group.protocol=classic to revert to the classic protocol until the cluster is upgraded.");
                }
            }, "Should get UnsupportedVersionException and how to revert to classic protocol");
        }
    }

}
