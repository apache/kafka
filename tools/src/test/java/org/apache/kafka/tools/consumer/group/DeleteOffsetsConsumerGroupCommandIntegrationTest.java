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
package org.apache.kafka.tools.consumer.group;

import kafka.test.ClusterInstance;
import kafka.test.ClusterGenerator;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("integration")
@ExtendWith(ClusterTestExtensions.class)
public class DeleteOffsetsConsumerGroupCommandIntegrationTest {
    public static final String TOPIC_PREFIX = "foo.";
    public static final String GROUP_PREFIX = "test.group.";
    private final ClusterInstance clusterInstance;

    DeleteOffsetsConsumerGroupCommandIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private static void generator(ClusterGenerator clusterGenerator) {
        ConsumerGroupCommandTestUtils.generator(clusterGenerator);
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsNonExistingGroup() {
        String group = "missing.group";
        String topic = "foo:1";
        try (ConsumerGroupCommand.ConsumerGroupService consumerGroupService = consumerGroupService(getArgs(group, topic))) {
            Entry<Errors, Map<TopicPartition, Throwable>> res = consumerGroupService.deleteOffsets(group, Collections.singletonList(topic));
            assertEquals(Errors.GROUP_ID_NOT_FOUND, res.getKey());
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicPartition() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            Runnable validateRunnable = getValidateRunnable(topic, group, 0, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
            testWithConsumerGroup(topic, group, groupProtocol, true, validateRunnable);
            removeTopic(topic);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicOnly() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            Runnable validateRunnable = getValidateRunnable(topic, group, -1, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
            testWithConsumerGroup(topic, group, groupProtocol, true, validateRunnable);
            removeTopic(topic);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicPartition() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            Runnable validateRunnable = getValidateRunnable("foobar", group, 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            testWithConsumerGroup(topic, group, groupProtocol, true, validateRunnable);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicOnly() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            Runnable validateRunnable = getValidateRunnable("foobar", group, -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            testWithConsumerGroup(topic, group, groupProtocol, true, validateRunnable);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicPartition() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            Runnable validateRunnable = getValidateRunnable(topic, group, 0, 0, Errors.NONE);
            testWithConsumerGroup(topic, group, groupProtocol, false, validateRunnable);
            removeTopic(topic);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            Runnable validateRunnable = getValidateRunnable(topic, group, -1, 0, Errors.NONE);
            testWithConsumerGroup(topic, group, groupProtocol, false, validateRunnable);
            removeTopic(topic);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicPartition() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            Runnable validateRunnable = getValidateRunnable("foobar", group, 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            testWithConsumerGroup(topic, group, groupProtocol, false, validateRunnable);
        }
    }

    @ClusterTemplate("generator")
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicOnly() {
        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            Runnable validateRunnable = getValidateRunnable("foobar", group, -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            testWithConsumerGroup(topic, group, groupProtocol, false, validateRunnable);
        }
    }

    private String[] getArgs(String group, String topic) {
        return new String[] {
            "--bootstrap-server", clusterInstance.bootstrapServers(),
            "--delete-offsets",
            "--group", group,
            "--topic", topic
        };
    }

    private static ConsumerGroupCommand.ConsumerGroupService consumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(
            ConsumerGroupCommandOptions.fromArgs(args),
            Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private Runnable getValidateRunnable(String inputTopic,
                                         String inputGroup,
                                         int inputPartition,
                                         int expectedPartition,
                                         Errors expectedError) {
        return () -> {
            String topic = inputPartition >= 0 ? inputTopic + ":" + inputPartition : inputTopic;
            try (ConsumerGroupCommand.ConsumerGroupService consumerGroupService = consumerGroupService(getArgs(inputGroup, topic))) {
                Entry<Errors, Map<TopicPartition, Throwable>> res = consumerGroupService.deleteOffsets(inputGroup, Collections.singletonList(topic));
                Errors topLevelError = res.getKey();
                Map<TopicPartition, Throwable> partitions = res.getValue();
                TopicPartition tp = new TopicPartition(inputTopic, expectedPartition);
                // Partition level error should propagate to top level, unless this is due to a missed partition attempt.
                if (inputPartition >= 0) {
                    assertEquals(expectedError, topLevelError);
                }
                if (expectedError == Errors.NONE)
                    assertNull(partitions.get(tp));
                else
                    assertEquals(expectedError.exception(), partitions.get(tp).getCause());
            }
        };
    }
    private void testWithConsumerGroup(String inputTopic,
                                       String inputGroup,
                                       GroupProtocol groupProtocol,
                                       boolean isStable,
                                       Runnable validateRunnable) {
        produceRecord(inputTopic);
        try (Consumer<byte[], byte[]> consumer = createConsumer(inputGroup, groupProtocol)) {
            consumer.subscribe(Collections.singletonList(inputTopic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));
            Assertions.assertNotEquals(0, records.count());
            consumer.commitSync();
            if (isStable) {
                validateRunnable.run();
            }
        }
        if (!isStable) {
            validateRunnable.run();
        }
    }

    private void produceRecord(String topic) {
        try (KafkaProducer<byte[], byte[]> producer = createProducer()) {
            assertDoesNotThrow(() -> producer.send(new ProducerRecord<>(topic, 0, null, null)).get());
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties config = new Properties();
        config.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1");
        config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    private Consumer<byte[], byte[]> createConsumer(String group, GroupProtocol groupProtocol) {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        consumerConfig.putIfAbsent(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());
        consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Increase timeouts to avoid having a rebalance during the test
        consumerConfig.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerConfig.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT));

        return new KafkaConsumer<>(consumerConfig);
    }

    private void createTopic(String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).topicId(topic).get());
        }
    }

    private void removeTopic(String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.deleteTopics(Collections.singletonList(topic)).all());
        }
    }
}
