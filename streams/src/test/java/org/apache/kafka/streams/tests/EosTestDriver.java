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

import kafka.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class EosTestDriver extends SmokeTestUtil {

    private static final int MAX_NUMBER_OF_KEYS = 100;

    private static boolean isRunning = true;

    static void generate(final String kafka) throws Exception {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isRunning = false;
            }
        });

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

        final Random rand = new Random();

        int numRecordsProduced = 0;
        while (isRunning) {
            final String key = "" + rand.nextInt(MAX_NUMBER_OF_KEYS);
            final int value = rand.nextInt(Integer.MAX_VALUE);

            final ProducerRecord<String, Integer> record = new ProducerRecord<>("data", key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                        Exit.exit(1);
                    }
                }
            });

            numRecordsProduced++;
            if (numRecordsProduced % 1000 == 0) {
                System.out.println(numRecordsProduced + " records produced");
            }
            Utils.sleep(rand.nextInt(50));
        }
        producer.close();
        System.out.println(numRecordsProduced + " records produced");
    }

    public static void verify(final String kafka) {
        ensureStreamsApplicationDown(kafka);

        final Map<TopicPartition, Long> committedOffsets = getCommittedOffsets(kafka);

        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
//            final List<TopicPartition> partitions = getAllPartitions(consumer, "data", "echo", "repartition", "max", "min", "sum", "cnt");
            final List<TopicPartition> partitions = getAllPartitions(consumer, "data", "echo", "min", "sum", "repartition");
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            for (final TopicPartition tp : partitions) {
                System.out.println("end-of-log: " + tp + " = " + consumer.position(tp));
            }
            consumer.seekToBeginning(partitions);

            final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition = new HashMap<>();

            long maxWaitTime = System.currentTimeMillis() + 30000;
            boolean allRecordsReceived = false;
            while (!allRecordsReceived && System.currentTimeMillis() < maxWaitTime) {
                final ConsumerRecords<byte[], byte[]> receivedRecords = consumer.poll(500);

                for (final ConsumerRecord<byte[], byte[]> record : receivedRecords) {
                    System.out.println((System.currentTimeMillis()) % 1000000 + " received " + receivedRecords.count());
                    maxWaitTime = System.currentTimeMillis() + 30000;

                    final String topic = record.topic();
                    final TopicPartition partition = new TopicPartition(topic, record.partition());

                    if ("data".equals(topic)
                        || "echo".equals(topic)
                        || "min".equals(topic)
                        || "sum".equals(topic)
                        || "repartition".equals(topic)
                        /*|| "max".equals(topic)
                        || "cnt".equals(topic)*/) {

                        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> topicRecordsPerPartition
                            = recordPerTopicPerPartition.get(topic);

                        if (topicRecordsPerPartition == null) {
                            topicRecordsPerPartition = new HashMap<>();
                            recordPerTopicPerPartition.put(topic, topicRecordsPerPartition);
                        }

                        List<ConsumerRecord<byte[], byte[]>> records = topicRecordsPerPartition.get(partition);
                        if (records == null) {
                            records = new ArrayList<>();
                            topicRecordsPerPartition.put(partition, records);
                        }
                        records.add(record);
                    } else {
                        System.err.println("FAIL: received data from unexpected topic: " + record);
                        return;
                    }
                }

                if (receivedRecords.count() > 0) {
                    allRecordsReceived =
                        receivedAllRecords(
                            recordPerTopicPerPartition.get("data"),
                            recordPerTopicPerPartition.get("echo"),
                            committedOffsets)
                            && receivedAllRecords(
                            recordPerTopicPerPartition.get("data"),
                            recordPerTopicPerPartition.get("repartitioning"),
                            committedOffsets);
                } else {
                    System.err.println("Did not receive any date " + System.currentTimeMillis());
                }
            }

            if (!allRecordsReceived) {
                System.err.println("FAIL: did not receive all records after 30 sec idle time.");
                for (final TopicPartition tp : partitions) {
                    try {
                        final List<ConsumerRecord<byte[], byte[]>> xxx = recordPerTopicPerPartition.get(tp.topic()).get(tp);
                        long waitingForOffset = consumer.position(tp);

                        if (xxx.size() < waitingForOffset) {
                            System.err.println("waiting for offset: " + tp + " = " + consumer.position(tp));
                            System.err.println("read: " + xxx.size() + "; last offset: " + xxx.get(xxx.size() - 1).offset());
                        }
                    } catch (final NullPointerException ignore) { }
                }
                return;
            }

            truncate("data", recordPerTopicPerPartition, committedOffsets);
            truncate("repartitioning", recordPerTopicPerPartition, committedOffsets);

            verifyMin(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("min"));
            verifySum(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("sum"));
//            verifyMax(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("max"));
//            verifyCnt(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("cnt"));

            //verifyAllTransactionFinished(consumer, kafka);

            // no not modify: required test output
            System.out.println("ALL-RECORDS-DELIVERED");
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            System.out.println("FAILED");
        }
    }

    private static void ensureStreamsApplicationDown(final String kafka) {
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.createSimplePlaintext(kafka);

            final long maxWaitTime = System.currentTimeMillis() + 30000;
            while (!adminClient.describeConsumerGroup(EosTestClient.appId, 10000).consumers().get().isEmpty()) {
                if (System.currentTimeMillis() > maxWaitTime) {
                    throw new IllegalStateException("Streams application not down after 30 seconds.");
                }
                sleep(1000);
            }
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
    }

    private static Map<TopicPartition, Long> getCommittedOffsets(final String kafka) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, EosTestClient.appId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "OffsetsClient");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        final Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final Set<String> topics = new HashSet<>();
            topics.add("data");
            //topics.add("repartition");
            consumer.subscribe(topics);
            consumer.poll(0);

            final Set<TopicPartition> partitions = new HashSet<>();
            for (final String topic : topics) {
                for (final PartitionInfo partition : consumer.partitionsFor(topic)) {
                    partitions.add(new TopicPartition(partition.topic(), partition.partition()));
                }
            }

            for (final TopicPartition tp : partitions) {
                final long offset = consumer.position(tp);
                System.out.println("committed offset: " + tp + " = " + offset);
                committedOffsets.put(tp, offset);
            }
        }

        return committedOffsets;
    }

    private static boolean receivedAllRecords(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> supersetExpectedRecords,
                                              final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> receivedRecords,
                                              final Map<TopicPartition, Long> committedOffsets) {
        if (supersetExpectedRecords == null
            || receivedRecords == null
            || supersetExpectedRecords.keySet().size() < committedOffsets.keySet().size()
            || receivedRecords.keySet().size() < committedOffsets.keySet().size()) {
            if (supersetExpectedRecords != null && receivedRecords != null) {
                System.err.println(committedOffsets);
                System.err.println("first: " + supersetExpectedRecords.keySet().size() + " < " + committedOffsets.keySet().size());
                System.err.println("first: " + receivedRecords.keySet().size() + " < " + committedOffsets.keySet().size());
            }
            return false;
        }

        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : receivedRecords.entrySet()) {
            try {
                final Long committed = committedOffsets.get(partitionRecords.getKey());
                if (partitionRecords.getValue().size() < (committed == null ? 0 : committed)) {
                    System.err.println("second: " + partitionRecords.getValue().size() + " < " + committed);
                    return false;
                }
            } catch (final NullPointerException foo) {
                System.err.println("error: " + partitionRecords.getValue());
                System.err.println("error: " + partitionRecords.getKey());
                System.err.println("error: " + committedOffsets);
                throw foo;
            }
        }

        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : receivedRecords.entrySet()) {
            try {
                final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
                final Iterator<ConsumerRecord<byte[], byte[]>> expectedRecords = supersetExpectedRecords.get(inputTopicPartition).iterator();

                for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionRecords.getValue()) {
                    final ConsumerRecord<byte[], byte[]> expected = expectedRecords.next();
                    if (!(stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key())
                        .equals(stringDeserializer.deserialize(expected.topic(), expected.key()))
                        && integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value())
                        .equals(integerDeserializer.deserialize(expected.topic(), expected.value())))) {
                        System.err.println("Result Verification failed:");
                        System.err.println("Expected " + expected + " but got " + receivedRecord);
                        throw new RuntimeException("Result verification failed.");
                    }
                }
            } catch (final NullPointerException e) {
                System.err.println(supersetExpectedRecords);
                e.printStackTrace(System.err);
                return false;
            }
        }

        return true;
    }

    private static void truncate(final String topic,
                                 final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition,
                                 final Map<TopicPartition, Long> committedOffsets) {
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> topicRecords = recordPerTopicPerPartition.get(topic);
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> truncatedTopicRecords = recordPerTopicPerPartition.get(topic);

        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : topicRecords.entrySet()) {
            final TopicPartition tp = partitionRecords.getKey();
            truncatedTopicRecords.put(tp, partitionRecords.getValue().subList(0, committedOffsets.get(tp).intValue()));
        }

        recordPerTopicPerPartition.put(topic, truncatedTopicRecords);
    }

    private static void verifyMin(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                     final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> minPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        final HashMap<String, Integer> currentMinPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : minPerTopicPerPartition.entrySet()) {
            try {
                final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
                final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = inputPerTopicPerPartition.get(inputTopicPartition).iterator();

                for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionRecords.getValue()) {
                    final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                    final String key = stringDeserializer.deserialize(input.topic(), input.key());
                    final Integer value = integerDeserializer.deserialize(input.topic(), input.value());

                    Integer min = currentMinPerKey.get(key);
                    if (min == null) {
                        min = value;
                    }
                    min = Math.min(min, value);
                    currentMinPerKey.put(key, min);

                    if (!(stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key()).equals(key)
                        && integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value()).equals(min))) {
                        System.err.println("Result Verification failed:");
                        System.err.println("Expected <" + key + "," + min + "> but got " + receivedRecord);
                        throw new RuntimeException("Result verification failed.");
                    }
                }
            } catch (final NullPointerException e) {
                System.err.println(inputPerTopicPerPartition);
                e.printStackTrace(System.err);
                throw e;
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
            try {
                final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
                final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = inputPerTopicPerPartition.get(inputTopicPartition).iterator();

                for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionRecords.getValue()) {
                    final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                    final String key = stringDeserializer.deserialize(input.topic(), input.key());
                    final Integer value = integerDeserializer.deserialize(input.topic(), input.value());

                    Long sum = currentSumPerKey.get(key);
                    if (sum == null) {
                        sum = 0L;
                    }
                    sum += value;
                    currentSumPerKey.put(key, sum);

                    if (!(stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key()).equals(key)
                        && longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value()).equals(sum))) {
                        System.err.println("Result Verification failed:");
                        System.err.println("Expected <" + key + "," + sum + "> but got " + receivedRecord);
                        throw new RuntimeException("Result verification failed.");
                    }
                }
            } catch (final NullPointerException e) {
                System.err.println(inputPerTopicPerPartition);
                e.printStackTrace(System.err);
                throw e;
            }
        }
    }

    private static void verifyMax(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> minPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        final HashMap<String, Integer> currentMaxPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : minPerTopicPerPartition.entrySet()) {
            try {
                final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
                final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = inputPerTopicPerPartition.get(inputTopicPartition).iterator();

                for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionRecords.getValue()) {
                    final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                    final String key = stringDeserializer.deserialize(input.topic(), input.key());
                    final Integer value = integerDeserializer.deserialize(input.topic(), input.value());

                    Integer max = currentMaxPerKey.get(key);
                    if (max == null) {
                        max = value;
                    }
                    max = Math.max(max, value);
                    currentMaxPerKey.put(key, max);

                    if (!(stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key()).equals(key)
                        && integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value()).equals(max))) {
                        System.err.println("Result Verification failed:");
                        System.err.println("Expected <" + key + "," + max + "> but got " + receivedRecord);
                        throw new RuntimeException("Result verification failed.");
                    }
                }
            } catch (final NullPointerException e) {
                System.err.println(inputPerTopicPerPartition);
                e.printStackTrace(System.err);
                throw e;
            }
        }
    }

    private static void verifyCnt(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> inputPerTopicPerPartition,
                                  final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> minPerTopicPerPartition) {
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final LongDeserializer longDeserializer = new LongDeserializer();

        final HashMap<String, Long> currentCntPerKey = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : minPerTopicPerPartition.entrySet()) {
            try {
                final TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
                final Iterator<ConsumerRecord<byte[], byte[]>> inputRecords = inputPerTopicPerPartition.get(inputTopicPartition).iterator();

                for (final ConsumerRecord<byte[], byte[]> receivedRecord : partitionRecords.getValue()) {
                    final ConsumerRecord<byte[], byte[]> input = inputRecords.next();

                    final String key = stringDeserializer.deserialize(input.topic(), input.key());

                    Long cnt = currentCntPerKey.get(key);
                    if (cnt == null) {
                        cnt = 0L;
                    }
                    currentCntPerKey.put(key, ++cnt);

                    if (!(stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key()).equals(key)
                        && longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value()).equals(cnt))) {
                        System.err.println("Result Verification failed:");
                        System.err.println("Expected <" + key + "," + cnt + "> but got " + receivedRecord);
                        throw new RuntimeException("Result verification failed.");
                    }
                }
            } catch (final NullPointerException e) {
                System.err.println(inputPerTopicPerPartition);
                e.printStackTrace(System.err);
                throw e;
            }
        }
    }

    private static void verifyAllTransactionFinished(final KafkaConsumer<byte[], byte[]> consumer,
                                                     final String kafka) {
        final List<TopicPartition> partitions = getAllPartitions(consumer, "echo", "repartition", "max", "min", "sum", "cnt");
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        consumer.poll(0);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "VerifyProducer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (final TopicPartition tp : partitions) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(tp.topic(), tp.partition(), "key", "value");

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                            Exit.exit(1);
                        }
                    }
                });
            }
        }

        final StringDeserializer stringDeserializer = new StringDeserializer();

        final long maxWaitTime = System.currentTimeMillis() + 30000;
        while (!partitions.isEmpty() && System.currentTimeMillis() < maxWaitTime) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                final String topic = record.topic();
                final TopicPartition tp = new TopicPartition(topic, record.partition());
                final String key = stringDeserializer.deserialize(topic, record.key());
                final String value = stringDeserializer.deserialize(topic, record.value());
                if (!("key".equals(key) && "value".equals(value) && partitions.remove(tp))) {
                    System.err.println("Received unexpected verification record:");
                    System.err.println("Expected record <'key','value'> from one of " + partitions + " but got"
                        + " <" + key + "," + value + "> [" + record.topic() + ", " + record.partition() + "]");
                    throw new RuntimeException("Post transactions verification failed.");
                }

            }
        }
        if (!partitions.isEmpty()) {
            System.err.println("Could not read all verification records within 1 minute.");
            throw new RuntimeException("Could not read all verification records within 1 minute.");
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

}
