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
package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogManagerIntegrationTest {
    private final ClusterInstance cluster;

    public LogManagerIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testIOExceptionOnLogSegmentCloseResultsInRecovery() throws IOException, InterruptedException, ExecutionException {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic("foo", 1, (short) 1))).all().get();
        }
        cluster.waitTopicCreation("foo", 1);

        // Produce some data into the topic
        Map<String, Object> producerConfigs = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            producer.send(new ProducerRecord<>("foo", 0, null, "bar")).get();
            producer.flush();
        }

        var broker = cluster.brokers().get(0);

        File timeIndexFile = broker.logManager()
                .getLog(new TopicPartition("foo", 0), false).get()
                .activeSegment()
                .timeIndexFile();

        // Set read only so that we throw an IOException on shutdown
        assertTrue(timeIndexFile.exists());
        assertTrue(timeIndexFile.setReadOnly());

        broker.shutdown();

        assertEquals(1, broker.config().logDirs().size());
        String logDir = broker.config().logDirs().get(0);
        CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir);
        assertFalse(cleanShutdownFileHandler.exists(), "Did not expect the clean shutdown file to exist");

        // Ensure we have a corrupt index on broker shutdown
        long maxIndexSize = broker.config().logIndexSizeMaxBytes();
        long expectedIndexSize = 12 * (maxIndexSize / 12);
        assertEquals(expectedIndexSize, timeIndexFile.length());

        // Allow write permissions before startup
        assertTrue(timeIndexFile.setWritable(true));

        broker.startup();
        // make sure there is no error during load logs
        assertTrue(cluster.firstFatalException().isEmpty());
        try (Admin admin = cluster.admin()) {
            TestUtils.waitForCondition(() -> {
                List<TopicPartitionInfo> partitionInfos = admin.describeTopics(List.of("foo"))
                        .topicNameValues().get("foo").get().partitions();
                return partitionInfos.get(0).leader().id() == 0;
            }, "Partition does not have a leader assigned");
        }

        // Ensure that sanity check does not fail
        broker.logManager()
                .getLog(new TopicPartition("foo", 0), false).get()
                .activeSegment()
                .timeIndex()
                .sanityCheck();
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, brokers = 3)
    public void testRestartBrokerNoErrorIfMissingPartitionMetadata() throws IOException, ExecutionException, InterruptedException {

        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic("foo", 1, (short) 3))).all().get();
        }
        cluster.waitTopicCreation("foo", 1);

        Optional<PartitionMetadataFile> partitionMetadataFile = cluster.brokers().get(0).logManager()
                .getLog(new TopicPartition("foo", 0), false).get()
                .partitionMetadataFile();
        assertTrue(partitionMetadataFile.isPresent());

        cluster.brokers().get(0).shutdown();
        try (Admin admin = cluster.admin()) {
            TestUtils.waitForCondition(() -> {
                List<TopicPartitionInfo> partitionInfos = admin.describeTopics(List.of("foo"))
                        .topicNameValues().get("foo").get().partitions();
                return partitionInfos.get(0).isr().size() == 2;
            }, "isr size is not shrink to 2");
        }

        // delete partition.metadata file here to simulate the scenario that partition.metadata not flush to disk yet
        partitionMetadataFile.get().delete();
        assertFalse(partitionMetadataFile.get().exists());
        cluster.brokers().get(0).startup();
        // make sure there is no error during load logs
        assertTrue(cluster.firstFatalException().isEmpty());
        try (Admin admin = cluster.admin()) {
            TestUtils.waitForCondition(() -> {
                List<TopicPartitionInfo> partitionInfos = admin.describeTopics(List.of("foo"))
                        .topicNameValues().get("foo").get().partitions();
                return partitionInfos.get(0).isr().size() == 3;
            }, "isr size is not expand to 3");
        }

        // make sure topic still work fine
        Map<String, Object> producerConfigs = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            producer.send(new ProducerRecord<>("foo", 0, null, "bar")).get();
            producer.flush();
        }

        Map<String, Object> consumerConfigs = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.assign(List.of(new TopicPartition("foo", 0)));
            consumer.seekToBeginning(List.of(new TopicPartition("foo", 0)));
            List<String> values = new ArrayList<>();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
            for (ConsumerRecord<String, String> record : records) {
                values.add(record.value());
            }
            assertEquals(1, values.size());
            assertEquals("bar", values.get(0));
        }
    }
}
