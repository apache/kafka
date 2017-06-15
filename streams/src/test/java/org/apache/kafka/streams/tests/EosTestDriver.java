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
import org.apache.kafka.common.errors.SerializationException;
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
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class EosTestDriver extends SmokeTestUtil {

    private static final int MAX_NUMBER_OF_KEYS = 100;
    private static final long MAX_IDLE_TIME_MS = 300000L;

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

        final Random rand = new Random(System.currentTimeMillis());

        int numRecordsProduced = 0;
        while (isRunning) {
            final String key = "" + rand.nextInt(MAX_NUMBER_OF_KEYS);
            final int value = rand.nextInt(10000);

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
            final List<TopicPartition> partitions = getAllPartitions(consumer, "data", "echo", "min", "sum");
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition
                = getOutputRecords(consumer, committedOffsets);

            truncate("data", recordPerTopicPerPartition, committedOffsets);

            verifyMin(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("min"));
            verifySum(recordPerTopicPerPartition.get("data"), recordPerTopicPerPartition.get("sum"));

            verifyAllTransactionFinished(consumer, kafka);

            // do not modify: required test output
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

            final long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
            while (!adminClient.describeConsumerGroup(EosTestClient.APP_ID, 10000).consumers().get().isEmpty()) {
                if (System.currentTimeMillis() > maxWaitTime) {
                    throw new RuntimeException("Streams application not down after 30 seconds.");
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, EosTestClient.APP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "OffsetsClient");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        final Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final Set<String> topics = new HashSet<>();
            topics.add("data");
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
                committedOffsets.put(tp, offset);
            }
        }

        return committedOffsets;
    }

    private static Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> getOutputRecords(final KafkaConsumer<byte[], byte[]> consumer,
                                                                                                           final Map<TopicPartition, Long> committedOffsets) {
        final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition = new HashMap<>();

        long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
        boolean allRecordsReceived = false;
        while (!allRecordsReceived && System.currentTimeMillis() < maxWaitTime) {
            final ConsumerRecords<byte[], byte[]> receivedRecords = consumer.poll(500);

            for (final ConsumerRecord<byte[], byte[]> record : receivedRecords) {
                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
                addRecord(record, recordPerTopicPerPartition);
            }

            if (receivedRecords.count() > 0) {
                allRecordsReceived =
                    receivedAllRecords(
                        recordPerTopicPerPartition.get("data"),
                        recordPerTopicPerPartition.get("echo"),
                        committedOffsets);
            }
        }

        if (!allRecordsReceived) {
            throw new RuntimeException("FAIL: did not receive all records after 30 sec idle time.");
        }

        return recordPerTopicPerPartition;
    }

    private static void addRecord(final ConsumerRecord<byte[], byte[]> record,
                                  final Map<String, Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>> recordPerTopicPerPartition) {

        final String topic = record.topic();
        final TopicPartition partition = new TopicPartition(topic, record.partition());

        if ("data".equals(topic)
            || "echo".equals(topic)
            || "min".equals(topic)
            || "sum".equals(topic)) {

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
            throw new RuntimeException("FAIL: received data from unexpected topic: " + record);
        }
    }

    private static boolean receivedAllRecords(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> supersetExpectedRecords,
                                              final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> receivedRecords,
                                              final Map<TopicPartition, Long> committedOffsets) {
        if (supersetExpectedRecords == null
            || receivedRecords == null
            || supersetExpectedRecords.keySet().size() < committedOffsets.keySet().size()
            || receivedRecords.keySet().size() < committedOffsets.keySet().size()) {

            return false;
        }

        for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords : receivedRecords.entrySet()) {
            final TopicPartition tp = partitionRecords.getKey();
            final int numberOfReceivedRecords = partitionRecords.getValue().size();
            final Long committed = committedOffsets.get(new TopicPartition("data", tp.partition()));
            if (committed != null) {
                if (numberOfReceivedRecords < committed) {
                    return false;
                }
            } else if (numberOfReceivedRecords > 0) {
                throw new RuntimeException("Result verification failed for partition " + tp
                    + ". No offset was committed but we received " + numberOfReceivedRecords + " records.");
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

                    final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                    final int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                    final String expectedKey = stringDeserializer.deserialize(expected.topic(), expected.key());
                    final int expectedValue = integerDeserializer.deserialize(expected.topic(), expected.value());

                    if (!receivedKey.equals(expectedKey) || receivedValue != expectedValue) {
                        throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + expectedKey + "," + expectedValue + "> but was <" + receivedKey + "," + receivedValue + ">");
                    }
                }
            } catch (final NullPointerException | NoSuchElementException e) {
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
            final Long committed = committedOffsets.get(new TopicPartition("data", tp.partition()));
            truncatedTopicRecords.put(tp, partitionRecords.getValue().subList(0, committed != null ? committed.intValue() : 0));
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

                    final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                    final int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                    final String key = stringDeserializer.deserialize(input.topic(), input.key());
                    final Integer value = integerDeserializer.deserialize(input.topic(), input.value());


                    Integer min = currentMinPerKey.get(key);
                    if (min == null) {
                        min = value;
                    }
                    min = Math.min(min, value);
                    currentMinPerKey.put(key, min);

                    if (!receivedKey.equals(key) || receivedValue != min) {
                        throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + value + "> but was <" + receivedKey + "," + receivedValue + ">");
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

                    final String receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.key());
                    final long receivedValue = longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.value());
                    final String key = stringDeserializer.deserialize(input.topic(), input.key());
                    final Integer value = integerDeserializer.deserialize(input.topic(), input.value());

                    Long sum = currentSumPerKey.get(key);
                    if (sum == null) {
                        sum = 0L;
                    }
                    sum += value;
                    currentSumPerKey.put(key, sum);

                    if (!receivedKey.equals(key) || receivedValue != sum) {
                        throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + value + "> but was <" + receivedKey + "," + receivedValue + ">");
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
        final List<TopicPartition> partitions = getAllPartitions(consumer, "echo", "min", "sum");
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

        final long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
        while (!partitions.isEmpty() && System.currentTimeMillis() < maxWaitTime) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                final String topic = record.topic();
                final TopicPartition tp = new TopicPartition(topic, record.partition());

                try {
                    final String key = stringDeserializer.deserialize(topic, record.key());
                    final String value = stringDeserializer.deserialize(topic, record.value());

                    if (!("key".equals(key) && "value".equals(value) && partitions.remove(tp))) {
                        throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
                            "Expected record <'key','value'> from one of " + partitions + " but got"
                            + " <" + key + "," + value + "> [" + record.topic() + ", " + record.partition() + "]");
                    }
                } catch (final SerializationException e) {
                    throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
                        "Expected record <'key','value'> from one of " + partitions + " but got " + record, e);
                }

            }
        }
        if (!partitions.isEmpty()) {
            throw new RuntimeException("Could not read all verification records. Did not receive any new record within the last 30 sec.");
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
