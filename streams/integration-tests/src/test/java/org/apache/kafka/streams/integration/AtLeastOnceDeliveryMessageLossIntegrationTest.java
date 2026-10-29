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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.DEFAULT_TIMEOUT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.consumerConfig;
import static org.apache.kafka.test.TestUtils.producerConfig;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Intended to reproduce KAFKA-19479
 */
@Timeout(600)
@Tag("integration")
public class AtLeastOnceDeliveryMessageLossIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(
        AtLeastOnceDeliveryMessageLossIntegrationTest.class);
    
    private static final int NUM_BROKERS = 1;
    private static final int LARGE_RECORD_COUNT = 50000;
    private static final int SMALL_RECORD_COUNT = 40000;
    
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    
    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }
    
    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }
    
    private String applicationId;
    private String inputTopic;
    private String outputTopic;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    
    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        applicationId = "app-" + testId;
        inputTopic = "input-" + testId;
        outputTopic = "output-" + testId;
        
        cleanStateBeforeTest(CLUSTER, inputTopic, outputTopic);
        CLUSTER.createTopics(inputTopic, outputTopic);
        
        setupStreamsConfiguration();
    }
    
    @AfterEach
    public void cleanUp() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        if (streamsConfiguration != null) {
            purgeLocalStreamsState(streamsConfiguration);
        }
    }

    @Test
    public void shouldCommitOffsetsAndProduceOutputRecordsWhenProducerFailsWithMessageTooLarge() throws Exception {
        
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(Sender.class)) {
            produceInputData(LARGE_RECORD_COUNT);

            kafkaStreams = createStreamsApplication();
            startApplicationAndWaitUntilRunning(Collections.singletonList(kafkaStreams), Duration.ofMillis(DEFAULT_TIMEOUT));

            waitForProcessingAndCommit();

            assertTrue(appender.getMessages().stream()
                    .anyMatch(msg -> msg.contains("MESSAGE_TOO_LARGE") && msg.contains("splitting and retrying")),
                "Should log MESSAGE_TOO_LARGE and splitting retry messages");

            final int outputRecordCount = verifyOutputRecords(LARGE_RECORD_COUNT); // should produce records
            final boolean offsetsCommitted = verifyConsumerOffsetsCommitted(LARGE_RECORD_COUNT); // should commit offset

            assertEquals(LARGE_RECORD_COUNT, outputRecordCount, "Output topic should have " + LARGE_RECORD_COUNT + " records");
            assertTrue(offsetsCommitted, "Consumer offsets should be committed");
        }
    }

    private void setupStreamsConfiguration() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // AT_LEAST_ONCE processing guarantee
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000L);
        
        // Producer configuration that can trigger MESSAGE_TOO_LARGE errors
        streamsConfiguration.put(ProducerConfig.LINGER_MS_CONFIG, 300000);
        streamsConfiguration.put(ProducerConfig.BATCH_SIZE_CONFIG, 33554432);
    }

    private void produceInputData(final int recordCount) throws Exception {
        final List<KeyValue<String, String>> inputRecords = new ArrayList<>();
        for (int i = 1; i <= recordCount; i++) {
            inputRecords.add(new KeyValue<>(String.valueOf(i), "item-" + i));
        }
        
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            inputRecords,
            producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            CLUSTER.time
        );
    }
    
    private void waitForProcessingAndCommit() throws Exception {
        // Wait slightly longer than commit interval to ensure processing and offset commits
        waitForCondition(
            () -> {
                try (final Admin adminClient = Admin.create(mkMap(
                        mkEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())))) {
                    final TopicPartition topicPartition = new TopicPartition(inputTopic, 0);
                    return adminClient
                        .listConsumerGroupOffsets(applicationId)
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .containsKey(topicPartition);
                } catch (final Exception e) {
                    return false;
                }
            },
            35000L,
            "Waiting for consumer offsets to be committed"
        );
    }
    
    private boolean verifyConsumerOffsetsCommitted(final int expectedOffset) throws Exception {
        try (final Admin adminClient = Admin.create(mkMap(
                mkEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())))) {
            
            final TopicPartition topicPartition = new TopicPartition(inputTopic, 0);
            
            final long committedOffset = adminClient
                .listConsumerGroupOffsets(applicationId) 
                .partitionsToOffsetAndMetadata()
                .get()
                .get(topicPartition)
                .offset();
            
            log.info("Consumer group {} committed offset: {} (expected: {})", applicationId, committedOffset, expectedOffset);
            return committedOffset == expectedOffset;
        }
    }
    
    private int verifyOutputRecords(final int expectedRecordCount) {
        try {
            final List<KeyValue<String, String>> outputRecords =
                waitUntilMinKeyValueRecordsReceived(
                    consumerConfig(
                        CLUSTER.bootstrapServers(),
                        applicationId + "-test-consumer-" + System.currentTimeMillis(),
                        StringDeserializer.class,
                        StringDeserializer.class
                    ),
                    outputTopic,
                    expectedRecordCount,
                    30000L
                );
            log.info("Output topic {} contains {} records", outputTopic, outputRecords.size());
            return outputRecords.size();
        } catch (final Exception e) {
            log.info("Exception while reading output records: {}", e.getMessage());
            return 0;
        }
    }

    private KafkaStreams createStreamsApplication() {
        final StreamsBuilder builder = new StreamsBuilder();
        
        final KStream<String, String> input = builder.stream(inputTopic);
        input.peek((key, value) -> {
            if (Integer.parseInt(key) % 1000 == 0) {
                log.debug("Processing record {}: {} -> {}", key, key, value);
            }
        }).to(outputTopic);
        
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}