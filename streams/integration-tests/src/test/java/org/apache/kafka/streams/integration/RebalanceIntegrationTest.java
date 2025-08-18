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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
@Timeout(600)
public class RebalanceIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceIntegrationTest.class);
    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = 30_000;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(mkMap(
            mkEntry("auto.create.topics.enable", "true"),
            mkEntry("transaction.max.timeout.ms", "" + Integer.MAX_VALUE)
        ))
    );

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private String applicationId;
    private static final int NUM_TOPIC_PARTITIONS = 2;
    private static final String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private static final String SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";

    private static final AtomicInteger TEST_NUMBER = new AtomicInteger(0);

    @BeforeEach
    public void createTopics() throws Exception {
        applicationId = "appId-" + TEST_NUMBER.getAndIncrement();
        CLUSTER.deleteTopics(MULTI_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);

        CLUSTER.createTopics(SINGLE_PARTITION_OUTPUT_TOPIC);
        CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    private void checkResultPerKey(final List<KeyValue<Long, Long>> result,
                                   final List<KeyValue<Long, Long>> expectedResult) {
        final Set<Long> allKeys = new HashSet<>();
        addAllKeys(allKeys, result);
        addAllKeys(allKeys, expectedResult);

        for (final Long key : allKeys) {
            assertThat("The records do not match what expected", getAllRecordPerKey(key, result), equalTo(getAllRecordPerKey(key, expectedResult)));
        }
    }

    private void addAllKeys(final Set<Long> allKeys, final List<KeyValue<Long, Long>> records) {
        for (final KeyValue<Long, Long> record : records) {
            allKeys.add(record.key);
        }
    }

    private List<KeyValue<Long, Long>> getAllRecordPerKey(final Long key, final List<KeyValue<Long, Long>> records) {
        final List<KeyValue<Long, Long>> recordsPerKey = new ArrayList<>(records.size());

        for (final KeyValue<Long, Long> record : records) {
            if (record.key.equals(key)) {
                recordsPerKey.add(record);
            }
        }

        return recordsPerKey;
    }

    @Test
    public void shouldCommitAllTasksIfRevokedTaskTriggerPunctuation() throws Exception {
        final AtomicBoolean requestCommit = new AtomicBoolean(false);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.<Long, Long>stream(MULTI_PARTITION_INPUT_TOPIC)
            .process(() -> new Processor<Long, Long, Long, Long>() {
                ProcessorContext<Long, Long> context;
                @Override
                public void init(final ProcessorContext<Long, Long> context) {
                    this.context = context;

                    final AtomicReference<Cancellable> cancellable = new AtomicReference<>();
                    cancellable.set(context.schedule(
                        Duration.ofSeconds(1),
                        PunctuationType.WALL_CLOCK_TIME,
                        time -> {
                            context.forward(new Record<>(
                                (context.taskId().partition() + 1) * 100L,
                                -(context.taskId().partition() + 1L),
                                context.currentSystemTimeMs()));
                            cancellable.get().cancel();
                        }
                    ));
                }

                @Override

                public void process(final Record<Long, Long> record) {
                    context.forward(record.withValue(context.recordMetadata().get().offset()));

                    if (requestCommit.get()) {
                        context.commit();
                    }
                }
            })
            .to(SINGLE_PARTITION_OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_INTERVAL_MS - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), Integer.MAX_VALUE);
        properties.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, TestTaskAssignor.class.getName());

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties
        );

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
            startApplicationAndWaitUntilRunning(streams);

            // PHASE 1:
            // produce single output record via punctuation (uncommitted) [this happens for both tasks]
            // StreamThread-1 now has a task with progress, and one task w/o progress
            final List<KeyValue<Long, Long>> expectedUncommittedResultBeforeRebalance = Arrays.asList(KeyValue.pair(100L, -1L), KeyValue.pair(200L, -2L));
            final List<KeyValue<Long, Long>> uncommittedRecordsBeforeRebalance = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
                SINGLE_PARTITION_OUTPUT_TOPIC,
                expectedUncommittedResultBeforeRebalance.size()
            );
            checkResultPerKey(uncommittedRecordsBeforeRebalance, expectedUncommittedResultBeforeRebalance);

            // PHASE 2:
            // add second thread, to trigger rebalance
            // both task should get committed
            streams.addStreamThread();

            final List<KeyValue<Long, Long>> expectedUncommittedResultAfterRebalance = Arrays.asList(KeyValue.pair(100L, -1L), KeyValue.pair(200L, -2L));
            final List<KeyValue<Long, Long>> uncommittedRecordsAfterRebalance = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
                SINGLE_PARTITION_OUTPUT_TOPIC,
                expectedUncommittedResultAfterRebalance.size()
            );
            checkResultPerKey(uncommittedRecordsAfterRebalance, expectedUncommittedResultAfterRebalance);
        }
    }
}