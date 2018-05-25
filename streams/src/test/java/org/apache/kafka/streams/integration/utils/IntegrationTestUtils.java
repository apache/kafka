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
package org.apache.kafka.streams.integration.utils;

import kafka.api.Request;
import kafka.server.KafkaServer;
import kafka.server.MetadataCache;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Utility functions to make integration testing more convenient.
 */
public class IntegrationTestUtils {

    public static final long DEFAULT_TIMEOUT = 30 * 1000L;
    public static final String INTERNAL_LEAVE_GROUP_ON_CLOSE = "internal.leave.group.on.close";

    /**
     * Removes local state stores.  Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration Streams configuration settings
     */
    public static void purgeLocalStreamsState(final Properties streamsConfiguration) throws
        IOException {
        final String tmpDir = TestUtils.IO_TMP_DIR.getPath();
        final String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            final File node = Paths.get(path).normalize().toFile();
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir)) {
                Utils.delete(new File(node.getAbsolutePath()));
            }
        }
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
        final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig, final Time time)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig, time, false);
    }

    public static <K, V> void produceKeyValuesSynchronously(
        final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig, final Headers headers, final Time time)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig, headers, time, false);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param enableTransactions  Send messages in a transaction
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
        final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig, final Time time, final boolean enableTransactions)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig, null, time, enableTransactions);
    }

    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Headers headers,
                                                            final Time time,
                                                            final boolean enableTransactions)
        throws ExecutionException, InterruptedException {
        for (final KeyValue<K, V> record : records) {
            produceKeyValuesSynchronouslyWithTimestamp(topic,
                Collections.singleton(record),
                producerConfig,
                headers,
                time.milliseconds(),
                enableTransactions);
            time.sleep(1L);
        }
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, timestamp, false);
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Headers headers,
                                                                         final Long timestamp)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, headers, timestamp, false);
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp,
                                                                         final boolean enableTransactions)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, null, timestamp, enableTransactions);
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Headers headers,
                                                                         final Long timestamp,
                                                                         final boolean enabledTransactions)
        throws ExecutionException, InterruptedException {
        try (Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (enabledTransactions) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            for (final KeyValue<K, V> record : records) {
                final Future<RecordMetadata> f = producer.send(
                    new ProducerRecord<>(topic, null, timestamp, record.key, record.value, headers));
                f.get();
            }
            if (enabledTransactions) {
                producer.commitTransaction();
            }
            producer.flush();
        }
    }

    public static <V> void produceValuesSynchronously(
        final String topic, final Collection<V> records, final Properties producerConfig, final Time time)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceValuesSynchronously(topic, records, producerConfig, time, false);
    }

    public static <V> void produceValuesSynchronously(
        final String topic, final Collection<V> records, final Properties producerConfig, final Time time, final boolean enableTransactions)
        throws ExecutionException, InterruptedException {
        final Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (final V value : records) {
            final KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig, time, enableTransactions);
    }

    /**
     * Wait for streams to "finish", based on the consumer lag metric.
     *
     * Caveats:
     * - Inputs must be finite, fully loaded, and flushed before this method is called
     * - expectedPartitions is the total number of partitions to watch the lag on, including both input and internal.
     *   It's somewhat ok to get this wrong, as the main failure case would be an immediate return due to the clients
     *   not being initialized, which you can avoid with any non-zero value. But it's probably better to get it right ;)
     */
    public static void waitForCompletion(final KafkaStreams streams,
                                         final int expectedPartitions,
                                         final int timeoutMilliseconds) {
        final long start = System.currentTimeMillis();
        while (true) {
            int lagMetrics = 0;
            double totalLag = 0.0;
            for (final Metric metric : streams.metrics().values()) {
                if (metric.metricName().name().equals("records-lag")) {
                    lagMetrics++;
                    totalLag += ((Number) metric.metricValue()).doubleValue();
                }
            }
            if (lagMetrics >= expectedPartitions && totalLag == 0.0) {
                return;
            }
            if (System.currentTimeMillis() - start >= timeoutMilliseconds) {
                throw new RuntimeException(String.format(
                    "Timed out waiting for completion. lagMetrics=[%s/%s] totalLag=[%s]",
                    lagMetrics, expectedPartitions, totalLag
                ));
            }
        }
    }

    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(final Properties consumerConfig,
                                                                                final String topic,
                                                                                final int expectedNumRecords) throws InterruptedException {
        return waitUntilMinRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords) throws InterruptedException {

        return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError       if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  final long waitTime) throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = new TestCondition() {
                @Override
                public boolean conditionMet() {
                    final List<KeyValue<K, V>> readData =
                        readKeyValues(topic, consumer, waitTime, expectedNumRecords);
                    accumData.addAll(readData);
                    return accumData.size() >= expectedNumRecords;
                }
            };
            final String conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(final Properties consumerConfig,
                                                                                final String topic,
                                                                                final int expectedNumRecords,
                                                                                final long waitTime) throws InterruptedException {
        final List<ConsumerRecord<K, V>> accumData = new ArrayList<>();
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = new TestCondition() {
                @Override
                public boolean conditionMet() {
                    final List<ConsumerRecord<K, V>> readData =
                        readRecords(topic, consumer, waitTime, expectedNumRecords);
                    accumData.addAll(readData);
                    return accumData.size() >= expectedNumRecords;
                }
            };
            final String conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords) throws InterruptedException {

        return waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError       if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                final long waitTime) throws InterruptedException {
        final List<V> accumData = new ArrayList<>();
        try (final Consumer<Object, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = new TestCondition() {
                @Override
                public boolean conditionMet() {
                    final List<V> readData =
                        readValues(topic, consumer, waitTime, expectedNumRecords);
                    accumData.addAll(readData);
                    return accumData.size() >= expectedNumRecords;
                }
            };
            final String conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    public static void waitForTopicPartitions(final List<KafkaServer> servers,
                                              final List<TopicPartition> partitions,
                                              final long timeout) throws InterruptedException {
        final long end = System.currentTimeMillis() + timeout;
        for (final TopicPartition partition : partitions) {
            final long remaining = end - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new AssertionError("timed out while waiting for partitions to become available. Timeout=" + timeout);
            }
            waitUntilMetadataIsPropagated(servers, partition.topic(), partition.partition(), remaining);
        }
    }

    public static void waitUntilMetadataIsPropagated(final List<KafkaServer> servers,
                                                     final String topic,
                                                     final int partition,
                                                     final long timeout) throws InterruptedException {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                for (final KafkaServer server : servers) {
                    final MetadataCache metadataCache = server.apis().metadataCache();
                    final Option<UpdateMetadataRequest.PartitionState> partitionInfo =
                            metadataCache.getPartitionInfo(topic, partition);
                    if (partitionInfo.isEmpty()) {
                        return false;
                    }
                    final UpdateMetadataRequest.PartitionState metadataPartitionState = partitionInfo.get();
                    if (!Request.isValidBrokerId(metadataPartitionState.basePartitionState.leader)) {
                        return false;
                    }
                }
                return true;
            }
        }, timeout, "metadata for topic=" + topic + " partition=" + partition + " not propagated to all brokers");

    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer config
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <V> List<V> readValues(final String topic, final Properties consumerConfig,
        final long waitTime, final int maxMessages) {
        final List<V> returnList;
        try (final Consumer<Object, V> consumer = createConsumer(consumerConfig)) {
            returnList = readValues(topic, consumer, waitTime, maxMessages);
        }
        return returnList;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer config
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
        final Properties consumerConfig, final long waitTime, final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues;
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            consumedValues = readKeyValues(topic, consumer, waitTime, maxMessages);
        }
        return consumedValues;
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    private static <V> List<V> readValues(final String topic,
                                          final Consumer<Object, V> consumer,
                                          final long waitTime,
                                          final int maxMessages) {
        final List<V> returnList = new ArrayList<>();
        final List<KeyValue<Object, V>> kvs = readKeyValues(topic, consumer, waitTime, maxMessages);
        for (final KeyValue<?, V> kv : kvs) {
            returnList.add(kv.value);
        }
        return returnList;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    private static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
                                                             final Consumer<K, V> consumer,
                                                             final long waitTime,
                                                             final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        final List<ConsumerRecord<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
        for (final ConsumerRecord<K, V> record : records) {
            consumedValues.add(new KeyValue<>(record.key(), record.value()));
        }
        return consumedValues;
    }

    private static <K, V> List<ConsumerRecord<K, V>> readRecords(final String topic,
                                                                 final Consumer<K, V> consumer,
                                                                 final long waitTime,
                                                                 final int maxMessages) {
        final List<ConsumerRecord<K, V>> consumerRecords;
        consumer.subscribe(Collections.singletonList(topic));
        final int pollIntervalMs = 100;
        consumerRecords = new ArrayList<>();
        int totalPollTimeMs = 0;
        while (totalPollTimeMs < waitTime &&
            continueConsuming(consumerRecords.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            final ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs);

            for (final ConsumerRecord<K, V> record : records) {
                consumerRecords.add(record);
            }
        }
        return consumerRecords;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Sets up a {@link KafkaConsumer} from a copy of the given configuration that has
     * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} set to "earliest" and {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     * set to "true" to prevent missing events as well as repeat consumption.
     * @param consumerConfig Consumer configuration
     * @return Consumer
     */
    private static <K, V> KafkaConsumer<K, V> createConsumer(final Properties consumerConfig) {
        final Properties filtered = new Properties();
        filtered.putAll(consumerConfig);
        filtered.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        filtered.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(filtered);
    }
}
