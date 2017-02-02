/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.integration.utils;

import kafka.api.PartitionStateInfo;
import kafka.api.Request;
import kafka.server.KafkaServer;
import kafka.server.MetadataCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
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

    public static final int UNLIMITED_MESSAGES = -1;
    public static final long DEFAULT_TIMEOUT = 30 * 1000L;

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <V> List<V> readValues(final String topic, final Properties consumerConfig, final long waitTime, final int maxMessages) {
        final List<V> returnList = new ArrayList<>();
        final List<KeyValue<Object, V>> kvs = readKeyValues(topic, consumerConfig, waitTime, maxMessages);
        for (final KeyValue<?, V> kv : kvs) {
            returnList.add(kv.value);
        }
        return returnList;
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
     * reached.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param waitTime       Maximum wait time in milliseconds
     * @return The KeyValue elements retrieved via the consumer.
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig, final long waitTime) {
        return readKeyValues(topic, consumerConfig, waitTime, UNLIMITED_MESSAGES);
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig, final long waitTime, final int maxMessages) {
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList(topic));
        final int pollIntervalMs = 100;
        final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        int totalPollTimeMs = 0;
        while (totalPollTimeMs < waitTime && continueConsuming(consumedValues.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            final ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs);
            for (final ConsumerRecord<K, V> record : records) {
                consumedValues.add(new KeyValue<>(record.key(), record.value()));
            }
        }

        consumer.close();

        return consumedValues;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Removes local state stores.  Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration Streams configuration settings
     */
    public static void purgeLocalStreamsState(final Properties streamsConfiguration) throws IOException {
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
        for (final KeyValue<K, V> record : records) {
            produceKeyValuesSynchronouslyWithTimestamp(topic,
                Collections.singleton(record),
                producerConfig,
                time.milliseconds());
            time.sleep(1L);
        }
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp)
        throws ExecutionException, InterruptedException {
        final Producer<K, V> producer = new KafkaProducer<>(producerConfig);
        for (final KeyValue<K, V> record : records) {
            final Future<RecordMetadata> f = producer.send(
                new ProducerRecord<>(topic, null, timestamp, record.key, record.value));
            f.get();
        }
        producer.flush();
        producer.close();
    }

    public static <V> void produceValuesSynchronously(
        final String topic, final Collection<V> records, final Properties producerConfig, final Time time)
        throws ExecutionException, InterruptedException {
        final Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (final V value : records) {
            final KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig, time);
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
     * @throws InterruptedException
     * @throws AssertionError       if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  final long waitTime) throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();

        final TestCondition valuesRead = new TestCondition() {
            @Override
            public boolean conditionMet() {
                final List<KeyValue<K, V>> readData = readKeyValues(topic, consumerConfig, waitTime);
                accumData.addAll(readData);
                return accumData.size() >= expectedNumRecords;
            }
        };

        final String conditionDetails = "Expecting " + expectedNumRecords + " records from topic " + topic + " while only received " + accumData.size() + ": " + accumData;

        TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);

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
     * @throws InterruptedException
     * @throws AssertionError       if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                final long waitTime) throws InterruptedException {
        final List<V> accumData = new ArrayList<>();

        final TestCondition valuesRead = new TestCondition() {
            @Override
            public boolean conditionMet() {
                final List<V> readData = readValues(topic, consumerConfig, waitTime, expectedNumRecords);
                accumData.addAll(readData);

                return accumData.size() >= expectedNumRecords;
            }
        };

        final String conditionDetails = "Expecting " + expectedNumRecords + " records from topic " + topic + " while only received " + accumData.size() + ": " + accumData;

        TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);

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
                    final Option<PartitionStateInfo> partitionInfo =
                            metadataCache.getPartitionInfo(topic, partition);
                    if (partitionInfo.isEmpty()) {
                        return false;
                    }
                    final PartitionStateInfo partitionStateInfo = partitionInfo.get();
                    if (!Request.isValidBrokerId(partitionStateInfo.leaderIsrAndControllerEpoch().leaderAndIsr().leader())) {
                        return false;
                    }
                }
                return true;
            }
        }, timeout, "metatadata for topic=" + topic + " partition=" + partition + " not propogated to all brokers");

    }
}
