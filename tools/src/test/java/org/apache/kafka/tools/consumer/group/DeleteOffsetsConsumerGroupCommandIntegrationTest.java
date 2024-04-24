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
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("integration")
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true")
})
@ExtendWith(ClusterTestExtensions.class)
public class DeleteOffsetsConsumerGroupCommandIntegrationTest {
    public static final String TOPIC = "foo";
    public static final String GROUP = "test.group";
    private final ClusterInstance clusterInstance;

    private ConsumerGroupCommand.ConsumerGroupService consumerGroupService;
    private final Iterable<Map<String, Object>> consumerConfigs;

    DeleteOffsetsConsumerGroupCommandIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
        this.consumerConfigs = clusterInstance.isKRaftTest()
                ? Arrays.asList(Collections.singletonMap(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name()),
                Collections.singletonMap(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))
                : Collections.singletonList(Collections.emptyMap());
    }

    @AfterEach
    public void tearDown() {
        if (consumerGroupService != null) {
            consumerGroupService.close();
        }
    }

    @ClusterTest
    public void testDeleteOffsetsNonExistingGroup() {
        String group = "missing.group";
        String topic = "foo:1";
        setupConsumerGroupService(getArgs(group, topic));

        Entry<Errors, Map<TopicPartition, Throwable>> res = consumerGroupService.deleteOffsets(group, Collections.singletonList(topic));
        assertEquals(Errors.GROUP_ID_NOT_FOUND, res.getKey());
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicPartition() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            createTopic(TOPIC);
            testWithConsumerGroup(TOPIC, 0, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC, true, consumerConfig);
            removeTopic(TOPIC);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicOnly() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            createTopic(TOPIC);
            testWithConsumerGroup(TOPIC, -1, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC, true, consumerConfig);
            removeTopic(TOPIC);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicPartition() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            testWithConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, true, consumerConfig);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicOnly() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            testWithConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION, true, consumerConfig);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicPartition() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            createTopic(TOPIC);
            testWithConsumerGroup(TOPIC, 0, 0, Errors.NONE, false, consumerConfig);
            removeTopic(TOPIC);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            createTopic(TOPIC);
            testWithConsumerGroup(TOPIC, -1, 0, Errors.NONE, false, consumerConfig);
            removeTopic(TOPIC);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicPartition() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            testWithConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, false, consumerConfig);
        }
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicOnly() {
        for (Map<String, Object> consumerConfig : consumerConfigs) {
            testWithConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION, false, consumerConfig);
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

    private void setupConsumerGroupService(String[] args) {
        consumerGroupService = new ConsumerGroupCommand.ConsumerGroupService(
            ConsumerGroupCommandOptions.fromArgs(args),
            Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private void testWithConsumerGroup(String inputTopic,
                                       int inputPartition,
                                       int expectedPartition,
                                       Errors expectedError,
                                       boolean isStable,
                                       Map<String, Object> consumerConfig) {
        produceRecord();
        this.withConsumerGroup(() -> {
            String topic = inputPartition >= 0 ? inputTopic + ":" + inputPartition : inputTopic;
            setupConsumerGroupService(getArgs(GROUP, topic));

            Entry<Errors, Map<TopicPartition, Throwable>> res = consumerGroupService.deleteOffsets(GROUP, Collections.singletonList(topic));
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
        }, isStable, consumerConfig);
    }

    private void produceRecord() {
        KafkaProducer<byte[], byte[]> producer = createProducer();
        try {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, null)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Utils.closeQuietly(producer, "producer");
        }
    }

    private void withConsumerGroup(Runnable body, boolean isStable, Map<String, Object> consumerConfig) {
        try (Consumer<byte[], byte[]> consumer = createConsumer(consumerConfig)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));
            Assertions.assertNotEquals(0, records.count());
            consumer.commitSync();
            if (isStable) {
                body.run();
            }
        }
        if (!isStable) {
            body.run();
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

    private Consumer<byte[], byte[]> createConsumer(Map<String, Object> config) {
        Map<String, Object> consumerConfig = new HashMap<>(config);
        consumerConfig.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
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
