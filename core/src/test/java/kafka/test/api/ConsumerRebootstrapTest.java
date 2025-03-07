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
package kafka.test.api;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConsumerRebootstrapTest {
    private static final int BROKER_COUNT = 2;
    private static final String TOPIC = "topic";
    private static final int PART = 1;

    private static List<ClusterConfig> rebootstrapConfigGenerator() {
        // Enable unclean leader election for the test topic
        Map<String, String> serverProperties = Map.of(
            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true",
            GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(BROKER_COUNT),
            ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false"
        );

        return Stream.of(false, true)
            .map(ConsumerRebootstrapTest::getRebootstrapConfig)
            .flatMap(rebootstrapProperties -> Stream.of(
                ConsumerRebootstrapTest.buildConfig(serverProperties, rebootstrapProperties, GroupProtocol.CLASSIC),
                ConsumerRebootstrapTest.buildConfig(serverProperties, rebootstrapProperties, GroupProtocol.CONSUMER)
            )).toList();
    }

    private static Map<String, String> getRebootstrapConfig(boolean useRebootstrapTriggerMs) {
        Map<String, String> properties = new HashMap<>();
        if (useRebootstrapTriggerMs) {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "5000");
        } else {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "3600000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "1000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        }
        properties.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
        return properties;
    }

    private static ClusterConfig buildConfig(Map<String, String> serverProperties, Map<String, String> rebootstrapProperties, GroupProtocol groupProtocol) {
        Map<String, String> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "-1");

        rebootstrapProperties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());

        return ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setBrokers(BROKER_COUNT)
            .setProducerProperties(producerProperties)
            .setConsumerProperties(rebootstrapProperties)
            .setServerProperties(serverProperties)
            .build();
    }

    @ClusterTemplate(value = "rebootstrapConfigGenerator")
    public void testRebootstrap(ClusterInstance clusterInstance) throws InterruptedException {
        clusterInstance.createTopic(TOPIC, BROKER_COUNT, (short) 2);

        var server0 = clusterInstance.brokers().get(0);
        var server1 = clusterInstance.brokers().get(1);
        var tp = new TopicPartition(TOPIC, PART);
        var offset = 0;
        var numRecords = 10;

        sendRecords(clusterInstance, offset, numRecords);

        TestUtils.waitForCondition(() -> {
            try (var head0 = server0.logManager().logsByTopic(TOPIC).head();
                 var head1 = server1.logManager().logsByTopic(TOPIC).head()) {
                return head0.logEndOffset() == head1.logEndOffset();
            }
        }, "Timeout waiting for records to be replicated");

        server1.shutdown();
        server1.awaitShutdown();

        try (var consumer = clusterInstance.consumer()) {
            // Only the server 0 is available for the consumer during the bootstrap.
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));

            var consumeRecords0 = consumeRecords(consumer, numRecords);
            verifyRecords(consumeRecords0, offset, numRecords, tp);

            // Bring back the server 1 and shut down 0.
            server1.startup();

            TestUtils.waitForCondition(() -> {
                try (var head0 = server0.logManager().logsByTopic(TOPIC).head();
                     var head1 = server1.logManager().logsByTopic(TOPIC).head()) {
                    return head0.logEndOffset() == head1.logEndOffset();
                }
            }, "Timeout waiting for records to be replicated");

            server0.shutdown();
            server0.awaitShutdown();

            offset += numRecords;
            sendRecords(clusterInstance, offset, numRecords);

            // The server 0, originally cached during the bootstrap, is offline.
            // However, the server 1 from the bootstrap list is online.
            var consumeRecords1 = consumeRecords(consumer, numRecords);
            verifyRecords(consumeRecords1, offset, numRecords, tp);

            // Bring back the server 0 and shut down 1.
            server0.startup();

            TestUtils.waitForCondition(() -> {
                try (var head0 = server0.logManager().logsByTopic(TOPIC).head();
                     var head1 = server1.logManager().logsByTopic(TOPIC).head()) {
                    return head0.logEndOffset() == head1.logEndOffset();
                }
            }, "Timeout waiting for records to be replicated");

            server1.shutdown();
            server1.awaitShutdown();

            offset += numRecords;
            sendRecords(clusterInstance, offset, numRecords);

            // The same situation, but the server 1 has gone and server 0 is back.
            var consumeRecords2 = consumeRecords(consumer, numRecords);
            verifyRecords(consumeRecords2, offset, numRecords, tp);
        }
    }

    private static List<ClusterConfig> rebootstrapDisabledConfigGenerator() {
        // Enable unclean leader election for the test topic
        Map<String, String> serverProperties = Map.of(
            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true",
            GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(BROKER_COUNT),
            ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false"
        );

        return Stream.of(false, true)
            .map(ConsumerRebootstrapTest::getRebootstrapDisabledConfig)
            .flatMap(rebootstrapProperties -> Stream.of(
                ConsumerRebootstrapTest.buildDisabledConfig(serverProperties, rebootstrapProperties, GroupProtocol.CLASSIC),
                ConsumerRebootstrapTest.buildDisabledConfig(serverProperties, rebootstrapProperties, GroupProtocol.CONSUMER)
            )).toList();
    }

    private static Map<String, String> getRebootstrapDisabledConfig(boolean useRebootstrapTriggerMs) {
        Map<String, String> properties = new HashMap<>();
        if (useRebootstrapTriggerMs) {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "1000");
        } else {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "3600000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "1000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        }
        properties.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none");
        return properties;
    }

    private static ClusterConfig buildDisabledConfig(Map<String, String> serverProperties, Map<String, String> rebootstrapProperties, GroupProtocol groupProtocol) {
        Map<String, String> producerProperties = new HashMap<>(rebootstrapProperties);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "-1");

        Map<String, String> consumerProperties = new HashMap<>(rebootstrapProperties);
        consumerProperties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());

        return ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setBrokers(BROKER_COUNT)
            .setAdminClientProperties(rebootstrapProperties)
            .setProducerProperties(producerProperties)
            .setConsumerProperties(consumerProperties)
            .setServerProperties(serverProperties)
            .build();
    }

    @ClusterTemplate(value = "rebootstrapDisabledConfigGenerator")
    public void testRebootstrapDisabled(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException, TimeoutException {
        clusterInstance.createTopic(TOPIC, BROKER_COUNT, (short) 2);

        var server0 = clusterInstance.brokers().get(0);
        var server1 = clusterInstance.brokers().get(1);
        var tp = new TopicPartition(TOPIC, PART);
        var numRecords = 1;

        server1.shutdown();
        server1.awaitShutdown();

        var consumer = clusterInstance.consumer();
        try (var adminClient = clusterInstance.admin();
             var producer = clusterInstance.producer()) {

            // Only the server 0 is available during the bootstrap.
            var recordMetadata = producer.send(new ProducerRecord<>(TOPIC, PART, 0L, "key 0".getBytes(), "value 0".getBytes())).get(15, TimeUnit.SECONDS);
            assertEquals(0, recordMetadata.offset());

            adminClient.listTopics().names().get(15, TimeUnit.SECONDS);
            consumer.assign(List.of(tp));
            var consumeRecords = consumeRecords(consumer, numRecords);
            verifyRecords(consumeRecords, 0, numRecords, tp);

            server0.shutdown();
            server0.awaitShutdown();
            server1.startup();

            assertThrows(TimeoutException.class, () -> producer.send(new ProducerRecord<>(TOPIC, PART, "key 2".getBytes(), "value 2".getBytes())).get(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> adminClient.listTopics().names().get(5, TimeUnit.SECONDS));
        }

        try (var producer = clusterInstance.producer()) {
            producer.send(new ProducerRecord<>(TOPIC, PART, 1L, "key 1".getBytes(), "value 1".getBytes())).get(15, TimeUnit.SECONDS);
        }

        assertEquals(0, consumer.poll(Duration.ofSeconds(5)).count());
        consumer.close();
    }

    private void sendRecords(ClusterInstance clusterInstance, int from, int numRecords) {
        try (var producer = clusterInstance.producer()) {
            for (var i = from; i < numRecords + from; i++) {
                var key = ("key " + i).getBytes();
                var value = ("value " + i).getBytes();
                producer.send(new ProducerRecord<>(TOPIC, PART, Long.valueOf(i), key, value));
            }
            producer.flush();
        }
    }

    private List<ConsumerRecord<Object, Object>> consumeRecords(Consumer<Object, Object> consumer, int numRecords) throws InterruptedException {
        List<ConsumerRecord<Object, Object>> records = new ArrayList<>();
        AtomicInteger recordCount = new AtomicInteger(0);
        TestUtils.waitForCondition(() -> {
            var polledRecords = consumer.poll(Duration.ofMillis(100));
            polledRecords.forEach(records::add);
            recordCount.addAndGet(polledRecords.count());
            return records.size() >= numRecords;
        }, 60000, String.format("Timed out before consuming expected %s records. The number consumed was %d.", numRecords, recordCount.get()));
        return records;
    }

    private void verifyRecords(
        List<ConsumerRecord<Object, Object>> records,
        int startingOffset,
        int numRecords,
        TopicPartition tp
    ) {
        for (var i = 0; i < numRecords; i++) {
            var record = records.get(i);
            var offset = startingOffset + i;
            var key = "key " + offset;
            var value = "value " + offset;

            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(offset, record.timestamp());
            assertEquals(offset, record.offset());
            assertEquals(key, new String((byte[]) record.key(), StandardCharsets.UTF_8));
            assertEquals(value, new String((byte[]) record.value(), StandardCharsets.UTF_8));
            assertEquals(key.length(), record.serializedKeySize());
            assertEquals(value.length(), record.serializedValueSize());
        }
    }
}
