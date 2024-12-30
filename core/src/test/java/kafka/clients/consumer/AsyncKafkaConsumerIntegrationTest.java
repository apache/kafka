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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;

import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@ExtendWith(ClusterTestExtensions.class)
public class AsyncKafkaConsumerIntegrationTest {

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic")
        })
    })
    public void testAsyncConsumerWithOldGroupCoordinator(ClusterInstance clusterInstance) throws Exception {
        String topic = "test-topic";
        clusterInstance.createTopic(topic, 1, (short) 1);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            consumer.subscribe(Collections.singletonList(topic));
            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return false;
                } catch (UnsupportedVersionException e) {
                    return e.getMessage().equals(AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG);
                }
            }, "Should get UnsupportedVersionException and how to revert to classic protocol");
        }
    }
}
