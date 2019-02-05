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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;

public class SmokeTestDriver extends SmokeTestUtil {

    private static class ValueList {
        public final String key;
        private final int[] values;
        private int index;

        ValueList(final int min, final int max) {
            this.key = min + "-" + max;

            this.values = new int[max - min + 1];
            for (int i = 0; i < this.values.length; i++) {
                this.values[i] = min + i;
            }
            // We want to randomize the order of data to test not completely predictable processing order
            // However, values are also use as a timestamp of the record. (TODO: separate data and timestamp)
            // We keep some correlation of time and order. Thus, the shuffling is done with a sliding window
            shuffle(this.values, 10);

            this.index = 0;
        }

        int next() {
            return (index < values.length) ? values[index++] : -1;
        }
    }

    static Map<String, Set<Integer>> generate(final String kafka,
                                              final int numKeys,
                                              final int maxRecordsPerKey,
                                              final boolean autoTerminate) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);

        int numRecordsProduced = 0;

        final Map<String, Set<Integer>> allData = new HashMap<>();
        final ValueList[] data = new ValueList[numKeys];
        for (int i = 0; i < numKeys; i++) {
            data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
            allData.put(data[i].key, new HashSet<>());
        }
        final Random rand = new Random();

        int remaining = 1; // dummy value must be positive if <autoTerminate> is false
        if (autoTerminate) {
            remaining = data.length;
        }

        List<ProducerRecord<byte[], byte[]>> needRetry = new ArrayList<>();

        while (remaining > 0) {
            final int index = autoTerminate ? rand.nextInt(remaining) : rand.nextInt(numKeys);
            final String key = data[index].key;
            final int value = data[index].next();

            if (autoTerminate && value < 0) {
                remaining--;
                data[index] = data[remaining];
            } else {

                final ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(
                        "data",
                        stringSerde.serializer().serialize("", key),
                        intSerde.serializer().serialize("", value)
                    );

                producer.send(record, new TestCallback(record, needRetry));

                numRecordsProduced++;
                allData.get(key).add(value);
                if (numRecordsProduced % 100 == 0) {
                    System.out.println(numRecordsProduced + " records produced");
                }
                Utils.sleep(2);
            }
        }
        producer.flush();

        int remainingRetries = 5;
        while (!needRetry.isEmpty()) {
            final List<ProducerRecord<byte[], byte[]>> needRetry2 = new ArrayList<>();
            for (final ProducerRecord<byte[], byte[]> record : needRetry) {
                producer.send(record, new TestCallback(record, needRetry2));
            }
            producer.flush();
            needRetry = needRetry2;

            if (--remainingRetries == 0 && !needRetry.isEmpty()) {
                System.err.println("Failed to produce all records after multiple retries");
                Exit.exit(1);
            }
        }

        producer.close();
        return Collections.unmodifiableMap(allData);
    }

    private static class TestCallback implements Callback {
        private final ProducerRecord<byte[], byte[]> originalRecord;
        private final List<ProducerRecord<byte[], byte[]>> needRetry;

        TestCallback(final ProducerRecord<byte[], byte[]> originalRecord,
                     final List<ProducerRecord<byte[], byte[]>> needRetry) {
            this.originalRecord = originalRecord;
            this.needRetry = needRetry;
        }

        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
            if (exception != null) {
                if (exception instanceof TimeoutException) {
                    needRetry.add(originalRecord);
                } else {
                    exception.printStackTrace();
                    Exit.exit(1);
                }
            }
        }
    }

    private static void shuffle(final int[] data, @SuppressWarnings("SameParameterValue") final int windowSize) {
        final Random rand = new Random();
        for (int i = 0; i < data.length; i++) {
            // we shuffle data within windowSize
            final int j = rand.nextInt(Math.min(data.length - i, windowSize)) + i;

            // swap
            final int tmp = data[i];
            data[i] = data[j];
            data[j] = tmp;
        }
    }

    public static boolean verify(final String kafka,
                                 final Map<String, Set<Integer>> inputs,
                                 final int maxRecordsPerKey) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        final String[] topics = {"data", "echo", "max", "min", "dif", "sum", "cnt", "avg", "tagg"};
        final List<TopicPartition> partitions = getAllPartitions(consumer, topics);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        final int recordsGenerated = inputs.size() * maxRecordsPerKey;
        int recordsProcessed = 0;
        final Map<String, AtomicInteger> processed =
            Stream.of(topics)
                  .collect(Collectors.toMap(t -> t, t -> new AtomicInteger(0)));

        final Map<String, Map<String, LinkedList<Number>>> events = new HashMap<>();

        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(6)) {
            final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));
            if (records.isEmpty() && recordsProcessed >= recordsGenerated) {
                break;
            } else {
                for (final ConsumerRecord<String, byte[]> record : records) {
                    final String key = record.key();

                    final String topic = record.topic();
                    processed.get(topic).incrementAndGet();

                    if (topic.equals("echo")) {
                        recordsProcessed++;
                        if (recordsProcessed % 100 == 0) {
                            System.out.println("Echo records processed = " + recordsProcessed);
                        }
                    }

                    events.computeIfAbsent(topic, t -> new HashMap<>())
                          .computeIfAbsent(key, k -> new LinkedList<>())
                          .add(deserializeValue(record));
                }

                System.out.println(processed);
            }
        }
        consumer.close();
        final long finished = System.currentTimeMillis() - start;
        System.out.println("Verification time=" + finished);
        System.out.println("-------------------");
        System.out.println("Result Verification");
        System.out.println("-------------------");
        System.out.println("recordGenerated=" + recordsGenerated);
        System.out.println("recordProcessed=" + recordsProcessed);

        if (recordsProcessed > recordsGenerated) {
            System.out.println("PROCESSED-MORE-THAN-GENERATED");
        } else if (recordsProcessed < recordsGenerated) {
            System.out.println("PROCESSED-LESS-THAN-GENERATED");
        }

        boolean success;

        final Map<String, HashSet<Number>> received =
            events.get("echo")
                  .entrySet()
                  .stream()
                  .map(entry -> mkEntry(entry.getKey(), new HashSet<>(entry.getValue())))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        success = inputs.equals(received);

        if (success) {
            System.out.println("ALL-RECORDS-DELIVERED");
        } else {
            int missedCount = 0;
            for (final Map.Entry<String, Set<Integer>> entry : inputs.entrySet()) {
                missedCount += received.get(entry.getKey()).size();
            }
            System.out.println("missedRecords=" + missedCount);
        }

        success &= verifyAll(inputs, events);

        System.out.println(success ? "SUCCESS" : "FAILURE");
        return success;
    }

    private static Number deserializeValue(final ConsumerRecord<String, byte[]> record) {
        final Number value;
        switch (record.topic()) {
            case "data": {
                value = intSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "echo": {
                value = intSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "min": {
                value = intSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "max": {
                value = intSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "dif": {
                value = intSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "sum": {
                value = longSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "cnt": {
                value = longSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "avg": {
                value = doubleSerde.deserializer().deserialize("", record.value());
                break;
            }
            case "tagg": {
                value = longSerde.deserializer().deserialize("", record.value());
                break;
            }
            default:
                throw new RuntimeException("unknown topic: " + record.topic());
        }
        return value;
    }

    private static boolean verifyAll(final Map<String, Set<Integer>> inputs,
                                     final Map<String, Map<String, LinkedList<Number>>> events) {
        final Map<String, LinkedList<Number>> observedInputEvents = events.get("data");
        boolean pass;
        pass = verifyTAgg(inputs, events.get("tagg"));
        pass &= verify("min", inputs, observedInputEvents, events, SmokeTestDriver::getMin);
        pass &= verify("max", inputs, observedInputEvents, events, SmokeTestDriver::getMax);
        pass &= verify("dif", inputs, observedInputEvents, events, key -> getMax(key).intValue() - getMin(key).intValue());
        pass &= verify("sum", inputs, observedInputEvents, events, SmokeTestDriver::getSum);
        pass &= verify("cnt", inputs, observedInputEvents, events, key1 -> getMax(key1).intValue() - getMin(key1).intValue() + 1L);
        pass &= verify("avg", inputs, observedInputEvents, events, SmokeTestDriver::getAvg);
        return pass;
    }

    private static boolean verify(final String topicName,
                                  final Map<String, Set<Integer>> inputData,
                                  final Map<String, LinkedList<Number>> consumedInputEvents,
                                  final Map<String, Map<String, LinkedList<Number>>> allEvents,
                                  final Function<String, Number> keyToExpectation) {
        final Map<String, LinkedList<Number>> outputEvents = allEvents.get(topicName);
        if (outputEvents.isEmpty()) {
            System.out.println(topicName + " is empty");
            return false;
        } else {
            System.out.println("verifying " + topicName);

            if (outputEvents.size() != inputData.size()) {
                System.out.println("fail: resultCount=" + outputEvents.size() + " expectedCount=" + inputData.size());
                return false;
            }
            for (final Map.Entry<String, LinkedList<Number>> entry : outputEvents.entrySet()) {
                final String key = entry.getKey();
                final Number expected = keyToExpectation.apply(key);
                final Number actual = entry.getValue().getLast();
                if (!expected.equals(actual)) {
                    System.out.printf("fail: key=%s %s=%s expected=%s%n\t inputEvents=%s%n\toutputEvents=%s%n",
                                      key,
                                      topicName,
                                      actual,
                                      expected,
                                      consumedInputEvents.get(key),
                                      entry.getValue());
                    return false;
                }
            }
            return true;
        }
    }

    private static Long getSum(final String key) {
        final int min = getMin(key).intValue();
        final int max = getMax(key).intValue();
        return ((long) min + (long) max) * (max - min + 1L) / 2L;
    }

    private static Double getAvg(final String key) {
        final int min = getMin(key).intValue();
        final int max = getMax(key).intValue();
        return ((long) min + (long) max) / 2.0;
    }


    private static boolean verifyTAgg(final Map<String, Set<Integer>> allData,
                                      final Map<String, LinkedList<Number>> taggEvents) {
        if (taggEvents.isEmpty()) {
            System.out.println("tagg is empty");
            return false;
        } else {
            System.out.println("verifying tagg");

            // generate expected answer
            final Map<String, Long> expected = new HashMap<>();
            for (final String key : allData.keySet()) {
                final int min = getMin(key).intValue();
                final int max = getMax(key).intValue();
                final String cnt = Long.toString(max - min + 1L);

                expected.put(cnt, expected.getOrDefault(cnt, 0L) + 1);
            }

            // check the result
            for (final Map.Entry<String, LinkedList<Number>> entry : taggEvents.entrySet()) {
                final String key = entry.getKey();
                Long expectedCount = expected.remove(key);
                if (expectedCount == null) {
                    expectedCount = 0L;
                }

                if (entry.getValue().getLast().longValue() != expectedCount) {
                    System.out.println("fail: key=" + key + " tagg=" + entry.getValue() + " expected=" + expected.get(key));
                    System.out.println("\t outputEvents: " + entry.getValue());
                    return false;
                }
            }

        }
        return true;
    }

    private static Number getMin(final String key) {
        return Integer.parseInt(key.split("-")[0]);
    }

    private static Number getMax(final String key) {
        return Integer.parseInt(key.split("-")[1]);
    }

    private static List<TopicPartition> getAllPartitions(final KafkaConsumer<?, ?> consumer, final String... topics) {
        final ArrayList<TopicPartition> partitions = new ArrayList<>();

        for (final String topic : topics) {
            for (final PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

}
