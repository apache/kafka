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
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.Defaults;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
    @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
})
@Tag("integration")
public class DeleteOffsetsConsumerGroupCommandIntegrationTest {
    private final ClusterInstance clusterInstance;
    public static final String TOPIC = "foo";
    public static final String GROUP = "test.group";

    DeleteOffsetsConsumerGroupCommandIntegrationTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    String[] getArgs(String group, String topic) {
        return new String[] {
            "--bootstrap-server", clusterInstance.bootstrapServers(),
            "--delete-offsets",
            "--group", group,
            "--topic", topic
        };
    }

    ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);

        return new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    @ClusterTest
    public void testDeleteOffsetsNonExistingGroup() {
        String group = "missing.group";
        String topic = "foo:1";
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(getArgs(group, topic));

        Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
        assertEquals(Errors.GROUP_ID_NOT_FOUND, res.getKey());
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicPartition() {
        createTopic(TOPIC);
        testWithStableConsumerGroup(TOPIC, 0, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicOnly() {
        createTopic(TOPIC);
        testWithStableConsumerGroup(TOPIC, -1, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicPartition() {
        testWithStableConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @ClusterTest
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicOnly() {
        testWithStableConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicPartition() {
        createTopic(TOPIC);
        testWithEmptyConsumerGroup(TOPIC, 0, 0, Errors.NONE);
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly() {
        createTopic(TOPIC);
        testWithEmptyConsumerGroup(TOPIC, -1, 0, Errors.NONE);
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicPartition() {
        testWithEmptyConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @ClusterTest
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicOnly() {
        testWithEmptyConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    private void testWithStableConsumerGroup(String inputTopic,
                                            int inputPartition,
                                            int expectedPartition,
                                            Errors expectedError) {
        testWithConsumerGroup(
            this::withStableConsumerGroup,
            inputTopic,
            inputPartition,
            expectedPartition,
            expectedError);
    }

    private void testWithEmptyConsumerGroup(String inputTopic,
                                           int inputPartition,
                                           int expectedPartition,
                                           Errors expectedError) {
        testWithConsumerGroup(
            this::withEmptyConsumerGroup,
            inputTopic,
            inputPartition,
            expectedPartition,
            expectedError);
    }

    private void testWithConsumerGroup(java.util.function.Consumer<Runnable> withConsumerGroup,
                                       String inputTopic,
                                       int inputPartition,
                                       int expectedPartition,
                                       Errors expectedError) {
        produceRecord();
        withConsumerGroup.accept(() -> {
            String topic = inputPartition >= 0 ? inputTopic + ":" + inputPartition : inputTopic;
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(getArgs(GROUP, topic));
            Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(GROUP, Collections.singletonList(topic));
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
        });
    }

    private void produceRecord() {
        KafkaProducer<byte[], byte[]> producer = createProducer(new Properties());
        try {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, null)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Utils.closeQuietly(producer, "producer");
        }
    }

    private void withStableConsumerGroup(Runnable body) {
        Consumer<byte[], byte[]> consumer = createConsumer(new Properties());
        try {
            TestUtils.subscribeAndWaitForRecords(TOPIC, consumer, DEFAULT_MAX_WAIT_MS);
            consumer.commitSync();
            body.run();
        } finally {
            Utils.closeQuietly(consumer, "consumer");
        }
    }

    private void withEmptyConsumerGroup(Runnable body) {
        Consumer<byte[], byte[]> consumer = createConsumer(new Properties());
        try {
            TestUtils.subscribeAndWaitForRecords(TOPIC, consumer, DEFAULT_MAX_WAIT_MS);
            consumer.commitSync();
        } finally {
            Utils.closeQuietly(consumer, "consumer");
        }
        body.run();
    }

    private KafkaProducer<byte[], byte[]> createProducer(Properties config) {
        config.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1");
        config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    private Consumer<byte[], byte[]> createConsumer(Properties config) {
        config.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        config.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Increase timeouts to avoid having a rebalance during the test
        config.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        config.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(Defaults.GROUP_MAX_SESSION_TIMEOUT_MS));

        return new KafkaConsumer<>(config);
    }

    private void createTopic(String topic) {
        TestUtils.createTopicWithAdminRaw(clusterInstance.createAdminClient(), topic, 1, 1, scala.collection.immutable.Map$.MODULE$.empty(), new Properties());
    }
}
