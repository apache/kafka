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
package org.apache.kafka.clients;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientRebootstrapTest {
    private static final String TOPIC = "topic";
    private static final int REPLICAS = 2;

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")
        }
    )
    public void testAdminRebootstrap(ClusterInstance clusterInstance) {
        var broker0 = 0;
        var broker1 = 1;
        var timeout = 60;

        clusterInstance.shutdownBroker(broker0);

        try (var admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) REPLICAS)));

            // Only the broker 1 is available for the admin client during the bootstrap.
            assertDoesNotThrow(() -> admin.listTopics().names().get(timeout, TimeUnit.SECONDS).contains(TOPIC));

            clusterInstance.shutdownBroker(broker1);
            clusterInstance.startBroker(broker0);

            // The broker 1, originally cached during the bootstrap, is offline.
            // However, the broker 0 from the bootstrap list is online.
            // Should be able to list topics again.
            assertDoesNotThrow(() -> admin.listTopics().names().get(timeout, TimeUnit.SECONDS).contains(TOPIC));
        }
    }

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")
        }
    )
    public void testAdminRebootstrapDisabled(ClusterInstance clusterInstance) {
        var broker0 = 0;
        var broker1 = 1;

        clusterInstance.shutdownBroker(broker0);

        var admin = clusterInstance.admin(Map.of(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none"));
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) REPLICAS)));

        // Only the broker 1 is available for the admin client during the bootstrap.
        assertDoesNotThrow(() -> admin.listTopics().names().get(60, TimeUnit.SECONDS).contains(TOPIC));

        clusterInstance.shutdownBroker(broker1);
        clusterInstance.startBroker(broker0);

        // The broker 1, originally cached during the bootstrap, is offline.
        // As a result, the admin client will throw a TimeoutException when trying to get list of the topics.
        assertThrows(TimeoutException.class, () -> admin.listTopics().names().get(5, TimeUnit.SECONDS));
        // Since the brokers cached during the bootstrap are offline, the admin client needs to wait the default timeout for other threads.
        admin.close(Duration.ZERO);
    }

    public void consumerRebootstrap(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        clusterInstance.createTopic(TOPIC, 1, (short) REPLICAS);

        var part = 0;
        var broker0 = 0;
        var broker1 = 1;
        var partitions = List.of(new TopicPartition(TOPIC, part));

        try (var producer = clusterInstance.producer()) {
            var recordMetadata = producer.send(new ProducerRecord<>(TOPIC, part, "key 0".getBytes(), "value 0".getBytes())).get();
            assertEquals(0, recordMetadata.offset());
            producer.flush();
        }

        clusterInstance.shutdownBroker(broker0);

        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            // Only the server 1 is available for the consumer during the bootstrap.
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            assertEquals(1, consumer.poll(Duration.ofSeconds(1)).count());

            // Bring back the server 0 and shut down 1.
            clusterInstance.shutdownBroker(broker1);
            clusterInstance.startBroker(broker0);

            try (var producer = clusterInstance.producer()) {
                var recordMetadata = producer.send(new ProducerRecord<>(TOPIC, part, "key 1".getBytes(), "value 1".getBytes())).get();
                assertEquals(1, recordMetadata.offset());
                producer.flush();
            }

            // The server 1 originally cached during the bootstrap, is offline.
            // However, the server 0 from the bootstrap list is online.
            assertEquals(1, consumer.poll(Duration.ofSeconds(1)).count());
        }
    }

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        })
    public void testClassicConsumerRebootstrap(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        consumerRebootstrap(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        })
    public void testConsumerRebootstrap(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        consumerRebootstrap(clusterInstance, GroupProtocol.CONSUMER);
    }

    public void consumerRebootstrapDisabled(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws InterruptedException, ExecutionException {
        clusterInstance.createTopic(TOPIC, 1, (short) REPLICAS);

        var part = 0;
        var broker0 = 0;
        var broker1 = 1;
        var tp = new TopicPartition(TOPIC, part);

        try (var producer = clusterInstance.producer()) {
            var recordMetadata = producer.send(new ProducerRecord<>(TOPIC, part, "key 0".getBytes(), "value 0".getBytes())).get();
            assertEquals(0, recordMetadata.offset());
            producer.flush();
        }

        clusterInstance.shutdownBroker(broker0);

        try (var consumer = clusterInstance.consumer(Map.of(
            CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none",
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name)
        )) {
            // Only the server 1 is available for the consumer during the bootstrap.
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            assertEquals(1, consumer.poll(Duration.ofSeconds(1)).count());

            // Bring back the server 0 and shut down 1.
            clusterInstance.shutdownBroker(broker1);
            clusterInstance.startBroker(broker0);

            try (var producer = clusterInstance.producer()) {
                var recordMetadata = producer.send(new ProducerRecord<>(TOPIC, part, "key 1".getBytes(), "value 1".getBytes())).get();
                assertEquals(1, recordMetadata.offset());
                producer.flush();
            }

            // The server 1 originally cached during the bootstrap, is offline.
            // However, the server 0 from the bootstrap list is online.
            assertEquals(0, consumer.poll(Duration.ofSeconds(1)).count());
        }
    }

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")
        }
    )
    public void testClassicConsumerRebootstrapDisabled(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        consumerRebootstrapDisabled(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
        brokers = REPLICAS,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")
        }
    )
    public void testConsumerRebootstrapDisabled(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        consumerRebootstrapDisabled(clusterInstance, GroupProtocol.CONSUMER);
    }
}
