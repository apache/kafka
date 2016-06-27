/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

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
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <V> List<V> readValues(String topic, Properties consumerConfig, int maxMessages) {
        List<V> returnList = new ArrayList<>();
        List<KeyValue<Object, V>> kvs = readKeyValues(topic, consumerConfig, maxMessages);
        for (KeyValue<?, V> kv : kvs) {
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
     * @return The KeyValue elements retrieved via the consumer.
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(String topic, Properties consumerConfig) {
        return readKeyValues(topic, consumerConfig, UNLIMITED_MESSAGES);
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(String topic, Properties consumerConfig, int maxMessages) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList(topic));
        int pollIntervalMs = 100;
        int maxTotalPollTimeMs = 2000;
        int totalPollTimeMs = 0;
        List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs);
            for (ConsumerRecord<K, V> record : records) {
                consumedValues.add(new KeyValue<>(record.key(), record.value()));
            }
        }
        consumer.close();
        return consumedValues;
    }

    private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Removes local state stores.  Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration Streams configuration settings
     */
    public static void purgeLocalStreamsState(Properties streamsConfiguration) throws IOException {
        String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            File node = Paths.get(path).normalize().toFile();
            // Only purge state when it's under /tmp.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith("/tmp")) {
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
        String topic, Collection<KeyValue<K, V>> records, Properties producerConfig)
        throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronouslyWithTimestamp(topic,
                                                   records,
                                                   producerConfig,
                                                   null);
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(String topic,
                                                                         Collection<KeyValue<K, V>> records,
                                                                         Properties producerConfig,
                                                                         Long timestamp)
        throws ExecutionException, InterruptedException {
        Producer<K, V> producer = new KafkaProducer<>(producerConfig);
        for (KeyValue<K, V> record : records) {
            Future<RecordMetadata> f = producer.send(
                new ProducerRecord<>(topic, null, timestamp, record.key, record.value));
            f.get();
        }
        producer.flush();
        producer.close();
    }

    public static <V> void produceValuesSynchronously(
        String topic, Collection<V> records, Properties producerConfig)
        throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (V value : records) {
            KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig);
    }

    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(Properties consumerConfig,
                                                                                  String topic,
                                                                                  int expectedNumRecords) throws InterruptedException {

        return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     * @param consumerConfig Kafka Consumer configuration
     * @param topic          Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime       Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws InterruptedException
     * @throws AssertionError if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  long waitTime) throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();

        TestCondition valuesRead = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<KeyValue<K, V>> readData = readKeyValues(topic, consumerConfig);
                accumData.addAll(readData);
                return accumData.size() >= expectedNumRecords;
            }
        };

        TestUtils.waitForCondition(valuesRead, waitTime);

        if (accumData.size() < expectedNumRecords) {
            throw new AssertionError("Expected " + expectedNumRecords +
                    " but received only " + accumData.size() +
                    " records before timeout " + waitTime + " ms");
        }

        return accumData;
    }

    public static <V> List<V> waitUntilMinValuesRecordsReceived(Properties consumerConfig,
                                                                String topic,
                                                                int expectedNumRecords) throws InterruptedException {

        return waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (value records) has been consumed.
     * @param consumerConfig Kafka Consumer configuration
     * @param topic          Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime       Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws InterruptedException
     * @throws AssertionError if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                long waitTime) throws InterruptedException {
        final List<V> accumData = new ArrayList<>();

        TestCondition valuesRead = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<V> readData = readValues(topic, consumerConfig, expectedNumRecords);
                accumData.addAll(readData);
                return accumData.size() >= expectedNumRecords;
            }
        };

        TestUtils.waitForCondition(valuesRead, waitTime);

        if (accumData.size() < expectedNumRecords) {
            throw new AssertionError("Expected " + expectedNumRecords +
                    " but received only " + accumData.size() +
                    " records before timeout " + waitTime + " ms");
        }

        return accumData;

    }

}