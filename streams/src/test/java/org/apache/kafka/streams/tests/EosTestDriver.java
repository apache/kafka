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
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EosTestDriver extends SmokeTestUtil {

    private static final int MAX_NUMBER_OF_KEYS = 20000;
    private static final long MAX_IDLE_TIME_MS = 600000L;

    private volatile static boolean isRunning = true;
    private static CountDownLatch terminated = new CountDownLatch(1);

    private static int numRecordsProduced = 0;

    private static synchronized void updateNumRecordsProduces(final int delta) {
        numRecordsProduced += delta;
    }

    static void generate(final String kafka) {
        Exit.addShutdownHook("streams-eos-test-driver-shutdown-hook", () -> {
            System.out.println("Terminating");
            isRunning = false;

            try {
                if (terminated.await(5L, TimeUnit.MINUTES)) {
                    System.out.println("Terminated");
                } else {
                    System.out.println("Terminated with timeout");
                }
            } catch (final InterruptedException swallow) {
                swallow.printStackTrace(System.err);
                System.out.println("Terminated with error");
            }
            System.err.flush();
            System.out.flush();
        });

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "EosTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        final Map<Integer, List<Long>> offsets = new HashMap<>();

        try {
            try (final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps)) {
                final Random rand = new Random(System.currentTimeMillis());

                while (isRunning) {
                    final String key = "" + rand.nextInt(MAX_NUMBER_OF_KEYS);
                    final int value = rand.nextInt(10000);

                    final ProducerRecord<String, Integer> record = new ProducerRecord<>("data", key, value);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace(System.err);
                            System.err.flush();
                            if (exception instanceof TimeoutException) {
                                try {
                                    // message == org.apache.kafka.common.errors.TimeoutException: Expiring 4 record(s) for data-0: 30004 ms has passed since last attempt plus backoff time
                                    final int expired = Integer.parseInt(exception.getMessage().split(" ")[2]);
                                    updateNumRecordsProduces(-expired);
                                } catch (final Exception ignore) {
                                }
                            }
                        } else {
                            offsets.getOrDefault(metadata.partition(), new LinkedList<>()).add(metadata.offset());
                        }
                    });

                    updateNumRecordsProduces(1);
                    if (numRecordsProduced % 1000 == 0) {
                        System.out.println(numRecordsProduced + " records produced");
                        System.out.flush();
                    }
                    Utils.sleep(rand.nextInt(10));
                }
            }
            System.out.println("Producer closed: " + numRecordsProduced + " records produced");
            System.out.flush();

            // verify offsets
            for (final Map.Entry<Integer, List<Long>> offsetsOfPartition : offsets.entrySet()) {
                offsetsOfPartition.getValue().sort(Long::compareTo);
                for (int i = 0; i < offsetsOfPartition.getValue().size() - 1; ++i) {
                    if (offsetsOfPartition.getValue().get(i) != i) {
                        System.err.println("Offset for partition " + offsetsOfPartition.getKey() + " is not " + i + " as expected but " + offsetsOfPartition.getValue().get(i));
                        System.err.flush();
                    }
                }
                System.out.println("Max offset of partition " + offsetsOfPartition.getKey() + " is " + offsetsOfPartition.getValue().get(offsetsOfPartition.getValue().size() - 1));
            }

            final Properties props = new Properties();
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

            try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
                final List<TopicPartition> partitions = getAllPartitions(consumer, "data");
                System.out.println("Partitions: " + partitions);
                System.out.flush();
                consumer.assign(partitions);
                consumer.seekToEnd(partitions);

                for (final TopicPartition tp : partitions) {
                    System.out.println("End-offset for " + tp + " is " + consumer.position(tp));
                    System.out.flush();
                }
            }
            System.out.flush();
        } finally {
            terminated.countDown();
        }
    }

    public static void verify(final String kafka, final boolean withRepartitioning) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            verifyAllTransactionFinished(consumer, kafka, withRepartitioning);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            System.out.println("FAILED");
            return;
        }

        final Map<TopicPartition, Long> committedOffsets;
        try (final Admin adminClient = Admin.create(props)) {
            ensureStreamsApplicationDown(adminClient);

            committedOffsets = getCommittedOffsets(adminClient, withRepartitioning);
        }

        final String[] allInputTopics;
        final String[] allOutputTopics;
        if (withRepartitioning) {
            allInputTopics = new String[] {"data", "repartition"};
            allOutputTopics = new String[] {"echo", "min", "sum", "repartition", "max", "cnt"};
        } else {
            allInputTopics = new String[] {"data"};
            allOutputTopics = new String[] {"echo", "min", "sum"};
        }

        final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> inputRecordsPerTopicPerPartition;
        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final List<TopicPartition> partitions = getAllPartitions(consumer, allInputTopics);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            inputRecordsPerTopicPerPartition = getRecords(consumer, committedOffsets, withRepartitioning, true);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            System.out.println("FAILED");
            return;
        }

        final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> outputRecordsPerTopicPerPartition;
        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final List<TopicPartition> partitions = getAllPartitions(consumer, allOutputTopics);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            outputRecordsPerTopicPerPartition = getRecords(consumer, consumer.endOffsets(partitions), withRepartitioning, false);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            System.out.println("FAILED");
            return;
        }

        verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("echo"));
        if (withRepartitioning) {
            verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("repartition"));
        }

        verifyMin(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("min"));
        verifySum(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("sum"));

        if (withRepartitioning) {
            verifyMax(inputRecordsPerTopicPerPartition.get("repartition"), outputRecordsPerTopicPerPartition.get("max"));
            verifyCnt(inputRecordsPerTopicPerPartition.get("repartition"), outputRecordsPerTopicPerPartition.get("cnt"));
        }

        // do not modify: required test output
        System.out.println("ALL-RECORDS-DELIVERED");
        System.out.flush();
    }

    private static void ensureStreamsApplicationDown(final Admin adminClient) {

        final long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
        ConsumerGroupDescription description;
        do {
            description = getConsumerGroupDescription(adminClient);

            if (System.currentTimeMillis() > maxWaitTime && !description.members().isEmpty()) {
                throw new RuntimeException(
                    "Streams application not down after " + (MAX_IDLE_TIME_MS / 1000L) + " seconds. " +
                        "Group: " + description
                );
            }
            sleep(1000L);
        } while (!description.members().isEmpty());
    }


    private static Map<TopicPartition, Long> getCommittedOffsets(final Admin adminClient,
                                                                 final boolean withRepartitioning) {
        final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;

        try {
            final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(EosTestClient.APP_ID);
            topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        final Map<TopicPartition, Long> committedOffsets = new HashMap<>();

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            final String topic = entry.getKey().topic();
            if (topic.equals("data") || withRepartitioning && topic.equals("repartition")) {
                committedOffsets.put(entry.getKey(), entry.getValue().offset());
            }
        }

        return committedOffsets;
    }

    private static Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> getRecords(final KafkaConsumer<byte[], byte[]> consumer,
                                                                                                     final Map<TopicPartition, Long> readEndOffsets,
                                                                                                     final boolean withRepartitioning,
                                                                                                     final boolean isInputTopic) {
        System.out.println("read end offset: " + readEndOffsets);
        final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition = new HashMap<>();
        final Map<TopicPartition, Long> maxReceivedOffsetPerPartition = new HashMap<>();
        final Map<TopicPartition, Long> maxConsumerPositionPerPartition = new HashMap<>();

        long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
        boolean allRecordsReceived = false;
        while (!allRecordsReceived && System.currentTimeMillis() < maxWaitTime) {
            final ConsumerRecords<byte[], byte[]> receivedRecords = consumer.poll(Duration.ofSeconds(1L));

            for (final ConsumerRecord<byte[], byte[]> record : receivedRecords) {
                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
                final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                maxReceivedOffsetPerPartition.put(tp, record.offset());
                final long readEndOffset = readEndOffsets.get(tp);
                if (record.offset() < readEndOffset) {
                    addRecord(record, recordPerTopicPerPartition, withRepartitioning);
                } else if (!isInputTopic) {
                    throw new RuntimeException("FAIL: did receive more records than expected for " + tp
                        + " (expected EOL offset: " + readEndOffset + "; current offset: " + record.offset());
                }
            }

            for (final TopicPartition tp : readEndOffsets.keySet()) {
                maxConsumerPositionPerPartition.put(tp, consumer.position(tp));
                if (consumer.position(tp) >= readEndOffsets.get(tp)) {
                    consumer.pause(Collections.singletonList(tp));
                }
            }

            allRecordsReceived = consumer.paused().size() == readEndOffsets.keySet().size();
        }

        if (!allRecordsReceived) {
            System.err.println("Pause partitions (ie, received all data): " + consumer.paused());
            System.err.println("Max received offset per partition: " + maxReceivedOffsetPerPartition);
            System.err.println("Max consumer position per partition: " + maxConsumerPositionPerPartition);
            throw new RuntimeException("FAIL: did not receive all records after " + (MAX_IDLE_TIME_MS / 1000L) + " sec idle time.");
        }

        return recordPerTopicPerPartition;
    }

    private static void addRecord(final ConsumerRecord<byte[], byte[]> record,
                                  final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition,
                                  final boolean withRepartitioning) {

        final String topic = record.topic();
        final TopicPartition partition = new TopicPartition(topic, record.partition());

        if (verifyTopic(topic, withRepartitioning)) {
            final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> topicRecordsPerPartition =
                recordPerTopicPerPartition.computeIfAbsent(topic, k -> new HashMap<>());

            final List<ConsumerRecord<byte[], byte[]>> records =
                topicRecordsPerPartition.computeIfAbsent(partition, k -> new ArrayList<>());

            records.add(record);
        } else {
            throw new RuntimeException("FAIL: received data from unexpected topic: " + record);
        }
    }

    private static boolean verifyTopic(final String topic,
                                       final boolean withRepartitioning) {
        final boolean validTopic = "data".equals(topic) || "echo".equals(topic) || "min".equals(topic) || "sum".equals(topic);

        if (withRepartitioning) {
            return validTopic || "repartition".equals(topic) || "max".equals(topic) || "cnt".equals(topic);
        }

        return validTopic;
    }

    private static void verifyReceivedAllRecords(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> expectedRecords,
                                                 final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> receivedRecords) {
        if (expectedRecords.size() != receivedRecords.size()) {
            throw new RuntimeException("Result verification failed. Received " + receivedRecords.size() + " records but expected " + expectedRecords.size());
        }

        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : receivedRecords.entrySet()) {
            final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
            final List<ConsumerRecord<byte[], byte[]>> receivedRecordsForPartition = partitionRecords.getValue();
            final List<ConsumerRecord<byte[], byte[]>> expectedRecordsForPartition = expectedRecords.get(inputTopicPartition);

            System.out.println(partitionRecords.getKey() + " with " + receivedRecordsForPartition.size() + ", " +
                    inputTopicPartition + " with " + expectedRecordsForPartition.size());

            final Iterator<ConsumerRecord<byte[], byte[]>> expectedRecord = expectedRecordsForPartition.iterator();
            RuntimeException exception = null;
            for (final ConsumerRecord<byte[], byte[]> receivedRecord : receivedRecordsForPartition) {
                if (!expectedRecord.hasNext()) {
                    exception = new RuntimeException("Result verification failed for " + receivedRecord + " since there's no more expected record");
                }

                final ConsumerRecord<byte[], byte[]> expected = expectedRecord.next();

                final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                final int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                final String expectedKey = stringDeserializer.deserialize(expected.topic(), expected.key());
                final int expectedValue = integerDeserializer.deserialize(expected.topic(), expected.value());

                if (!receivedKey.equals(expectedKey) || receivedValue != expectedValue) {
                    exception = new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + expectedKey + "," + expectedValue + "> but was <" + receivedKey + "," + receivedValue + ">");
                }
            }

            if (exception != null) {
                throw exception;
            }
        }
    }

    private static void verifyMin(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> minPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        final HashMap<String, Integer> currentMinPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : minPerTopicPerPartition.entrySet()) {
            final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
            final List<ConsumerRecord<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
            final List<ConsumerRecord<byte[], byte[]>> partitionMin = partitionRecords.getValue();

            if (partitionInput.size() != partitionMin.size()) {
                throw new RuntimeException("Result verification failed: expected " + partitionInput.size() + " records for "
                    + partitionRecords.getKey() + " but received " + partitionMin.size());
            }

            final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = partitionInput.iterator();

            for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionMin) {
                final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                final int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                final String key = stringDeserializer.deserialize(input.topic(), input.key());
                final int value = integerDeserializer.deserialize(input.topic(), input.value());

                Integer min = currentMinPerKey.get(key);
                if (min == null) {
                    min = value;
                } else {
                    min = Math.min(min, value);
                }
                currentMinPerKey.put(key, min);

                if (!receivedKey.equals(key) || receivedValue != min) {
                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + min + "> but was <" + receivedKey + "," + receivedValue + ">");
                }
            }
        }
    }

    private static void verifySum(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> minPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();
        final LongDeserializer longDeserializer = new LongDeserializer();

        final HashMap<String, Long> currentSumPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : minPerTopicPerPartition.entrySet()) {
            final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
            final List<ConsumerRecord<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
            final List<ConsumerRecord<byte[], byte[]>> partitionSum = partitionRecords.getValue();

            if (partitionInput.size() != partitionSum.size()) {
                throw new RuntimeException("Result verification failed: expected " + partitionInput.size() + " records for "
                    + partitionRecords.getKey() + " but received " + partitionSum.size());
            }

            final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = partitionInput.iterator();

            for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionSum) {
                final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                final long receivedValue = longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                final String key = stringDeserializer.deserialize(input.topic(), input.key());
                final int value = integerDeserializer.deserialize(input.topic(), input.value());

                Long sum = currentSumPerKey.get(key);
                if (sum == null) {
                    sum = (long) value;
                } else {
                    sum += value;
                }
                currentSumPerKey.put(key, sum);

                if (!receivedKey.equals(key) || receivedValue != sum) {
                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + sum + "> but was <" + receivedKey + "," + receivedValue + ">");
                }
            }
        }
    }

    private static void verifyMax(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> maxPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        final HashMap<String, Integer> currentMinPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : maxPerTopicPerPartition.entrySet()) {
            final TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.getKey().partition());
            final List<ConsumerRecord<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
            final List<ConsumerRecord<byte[], byte[]>> partitionMax = partitionRecords.getValue();

            if (partitionInput.size() != partitionMax.size()) {
                throw new RuntimeException("Result verification failed: expected " + partitionInput.size() + " records for "
                    + partitionRecords.getKey() + " but received " + partitionMax.size());
            }

            final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = partitionInput.iterator();

            for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionMax) {
                final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                final int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                final String key = stringDeserializer.deserialize(input.topic(), input.key());
                final int value = integerDeserializer.deserialize(input.topic(), input.value());


                Integer max = currentMinPerKey.get(key);
                if (max == null) {
                    max = Integer.MIN_VALUE;
                }
                max = Math.max(max, value);
                currentMinPerKey.put(key, max);

                if (!receivedKey.equals(key) || receivedValue != max) {
                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + max + "> but was <" + receivedKey + "," + receivedValue + ">");
                }
            }
        }
    }

    private static void verifyCnt(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> cntPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final LongDeserializer longDeserializer = new LongDeserializer();

        final HashMap<String, Long> currentSumPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : cntPerTopicPerPartition.entrySet()) {
            final TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.getKey().partition());
            final List<ConsumerRecord<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
            final List<ConsumerRecord<byte[], byte[]>> partitionCnt = partitionRecords.getValue();

            if (partitionInput.size() != partitionCnt.size()) {
                throw new RuntimeException("Result verification failed: expected " + partitionInput.size() + " records for "
                    + partitionRecords.getKey() + " but received " + partitionCnt.size());
            }

            final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = partitionInput.iterator();

            for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionCnt) {
                final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                final long receivedValue = longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                final String key = stringDeserializer.deserialize(input.topic(), input.key());

                Long cnt = currentSumPerKey.get(key);
                if (cnt == null) {
                    cnt = 0L;
                }
                currentSumPerKey.put(key, ++cnt);

                if (!receivedKey.equals(key) || receivedValue != cnt) {
                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + cnt + "> but was <" + receivedKey + "," + receivedValue + ">");
                }
            }
        }
    }

    private static void verifyAllTransactionFinished(final KafkaConsumer<byte[], byte[]> consumer,
                                                     final String kafka,
                                                     final boolean withRepartitioning) {
        final String[] topics;
        if (withRepartitioning) {
            topics = new String[] {"echo", "min", "sum", "repartition", "max", "cnt"};
        } else {
            topics = new String[] {"echo", "min", "sum"};
        }

        final List<TopicPartition> partitions = getAllPartitions(consumer, topics);
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        for (final TopicPartition tp : partitions) {
            System.out.println(tp + " at position " + consumer.position(tp));
        }

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-uncommitted");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);


        final long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
        try (final KafkaConsumer<byte[], byte[]> consumerUncommitted = new KafkaConsumer<>(consumerProps)) {
            while (System.currentTimeMillis() < maxWaitTime) {
                consumer.seekToEnd(partitions);
                final Map<TopicPartition, Long> topicEndOffsets = consumerUncommitted.endOffsets(partitions);

                final Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()) {
                    final TopicPartition topicPartition = iterator.next();
                    final long position = consumer.position(topicPartition);

                    if (position == topicEndOffsets.get(topicPartition)) {
                        iterator.remove();
                        System.out.println("Removing " + topicPartition + " at position " + position);
                    } else if (consumer.position(topicPartition) > topicEndOffsets.get(topicPartition)) {
                        throw new IllegalStateException("Offset for partition " + topicPartition + " is larger than topic endOffset: " + position + " > " + topicEndOffsets.get(topicPartition));
                    } else {
                        System.out.println("Retry " + topicPartition + " at position " + position);
                    }
                }
                sleep(1000L);
            }
        }

        if (!partitions.isEmpty()) {
            throw new RuntimeException("Could not read all verification records. Did not receive any new record within the last " + (MAX_IDLE_TIME_MS / 1000L) + " sec.");
        }
    }

    private static List<TopicPartition> getAllPartitions(final KafkaConsumer<?, ?> consumer,
                                                         final String... topics) {
        final ArrayList<TopicPartition> partitions = new ArrayList<>();

        for (final String topic : topics) {
            for (final PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }


    private static ConsumerGroupDescription getConsumerGroupDescription(final Admin adminClient) {
        final ConsumerGroupDescription description;
        try {
            description = adminClient.describeConsumerGroups(Collections.singleton(EosTestClient.APP_ID))
                .describedGroups()
                .get(EosTestClient.APP_ID)
                .get(10, TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException("Unexpected Exception getting group description", e);
        }
        return description;
    }
}
