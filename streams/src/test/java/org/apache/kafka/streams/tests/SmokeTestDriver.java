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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;

public class SmokeTestDriver extends SmokeTestUtil {
    private static final String[] TOPICS = {
        "data",
        "echo",
        "max",
        "min", "min-suppressed", "min-raw",
        "dif",
        "sum",
        "sws-raw", "sws-suppressed",
        "cnt",
        "avg",
        "tagg"
    };

    private static final int MAX_RECORD_EMPTY_RETRIES = 30;

    private static class ValueList {
        public final String key;
        private final int[] values;
        private int index;

        ValueList(final int min, final int max) {
            key = min + "-" + max;

            values = new int[max - min + 1];
            for (int i = 0; i < values.length; i++) {
                values[i] = min + i;
            }
            // We want to randomize the order of data to test not completely predictable processing order
            // However, values are also use as a timestamp of the record. (TODO: separate data and timestamp)
            // We keep some correlation of time and order. Thus, the shuffling is done with a sliding window
            shuffle(values, 10);

            index = 0;
        }

        int next() {
            return (index < values.length) ? values[index++] : -1;
        }
    }

    public static String[] topics() {
        return Arrays.copyOf(TOPICS, TOPICS.length);
    }

    static void generatePerpetually(final String kafka,
                                    final int numKeys,
                                    final int maxRecordsPerKey) {
        final Properties producerProps = generatorProperties(kafka);

        int numRecordsProduced = 0;

        final ValueList[] data = new ValueList[numKeys];
        for (int i = 0; i < numKeys; i++) {
            data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
        }

        final Random rand = new Random();

        try (final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            while (true) {
                final int index = rand.nextInt(numKeys);
                final String key = data[index].key;
                final int value = data[index].next();

                final ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(
                        "data",
                        stringSerde.serializer().serialize("", key),
                        intSerde.serializer().serialize("", value)
                    );

                producer.send(record);

                numRecordsProduced++;
                if (numRecordsProduced % 100 == 0) {
                    System.out.println(Instant.now() + " " + numRecordsProduced + " records produced");
                }
                Utils.sleep(2);
            }
        }
    }

    public static Map<String, Set<Integer>> generate(final String kafka,
                                                     final int numKeys,
                                                     final int maxRecordsPerKey,
                                                     final Duration timeToSpend) {
        final Properties producerProps = generatorProperties(kafka);


        int numRecordsProduced = 0;

        final Map<String, Set<Integer>> allData = new HashMap<>();
        final ValueList[] data = new ValueList[numKeys];
        for (int i = 0; i < numKeys; i++) {
            data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
            allData.put(data[i].key, new HashSet<>());
        }
        final Random rand = new Random();

        int remaining = data.length;

        final long recordPauseTime = timeToSpend.toMillis() / numKeys / maxRecordsPerKey;

        List<ProducerRecord<byte[], byte[]>> needRetry = new ArrayList<>();

        try (final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            while (remaining > 0) {
                final int index = rand.nextInt(remaining);
                final String key = data[index].key;
                final int value = data[index].next();

                if (value < 0) {
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
                        System.out.println(Instant.now() + " " + numRecordsProduced + " records produced");
                    }
                    Utils.sleep(Math.max(recordPauseTime, 2));
                }
            }
            producer.flush();

            int remainingRetries = 5;
            while (!needRetry.isEmpty()) {
                final List<ProducerRecord<byte[], byte[]>> needRetry2 = new ArrayList<>();
                for (final ProducerRecord<byte[], byte[]> record : needRetry) {
                    System.out.println("retry producing " + stringSerde.deserializer().deserialize("", record.key()));
                    producer.send(record, new TestCallback(record, needRetry2));
                }
                producer.flush();
                needRetry = needRetry2;

                if (--remainingRetries == 0 && !needRetry.isEmpty()) {
                    System.err.println("Failed to produce all records after multiple retries");
                    Exit.exit(1);
                }
            }

            // now that we've sent everything, we'll send some final records with a timestamp high enough to flush out
            // all suppressed records.
            final List<PartitionInfo> partitions = producer.partitionsFor("data");
            for (final PartitionInfo partition : partitions) {
                producer.send(new ProducerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    System.currentTimeMillis() + Duration.ofDays(2).toMillis(),
                    stringSerde.serializer().serialize("", "flush"),
                    intSerde.serializer().serialize("", 0)
                ));
            }
        }
        return Collections.unmodifiableMap(allData);
    }

    private static Properties generatorProperties(final String kafka) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        return producerProps;
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

    public static class NumberDeserializer implements Deserializer<Number> {
        @Override
        public Number deserialize(final String topic, final byte[] data) {
            final Number value;
            switch (topic) {
                case "data":
                case "echo":
                case "min":
                case "min-raw":
                case "min-suppressed":
                case "sws-raw":
                case "sws-suppressed":
                case "max":
                case "dif":
                    value = intSerde.deserializer().deserialize(topic, data);
                    break;
                case "sum":
                case "cnt":
                case "tagg":
                    value = longSerde.deserializer().deserialize(topic, data);
                    break;
                case "avg":
                    value = doubleSerde.deserializer().deserialize(topic, data);
                    break;
                default:
                    throw new RuntimeException("unknown topic: " + topic);
            }
            return value;
        }
    }

    public static VerificationResult verify(final String kafka,
                                            final Map<String, Set<Integer>> inputs,
                                            final int maxRecordsPerKey) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NumberDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        final KafkaConsumer<String, Number> consumer = new KafkaConsumer<>(props);
        final List<TopicPartition> partitions = getAllPartitions(consumer, TOPICS);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        final int recordsGenerated = inputs.size() * maxRecordsPerKey;
        int recordsProcessed = 0;
        final Map<String, AtomicInteger> processed =
            Stream.of(TOPICS)
                  .collect(Collectors.toMap(t -> t, t -> new AtomicInteger(0)));

        final Map<String, Map<String, LinkedList<ConsumerRecord<String, Number>>>> events = new HashMap<>();

        VerificationResult verificationResult = new VerificationResult(false, "no results yet");
        int retry = 0;
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(6)) {
            final ConsumerRecords<String, Number> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty() && recordsProcessed >= recordsGenerated) {
                verificationResult = verifyAll(inputs, events);
                if (verificationResult.passed()) {
                    break;
                } else if (retry++ > MAX_RECORD_EMPTY_RETRIES) {
                    System.out.println(Instant.now() + " Didn't get any more results, verification hasn't passed, and out of retries.");
                    break;
                } else {
                    System.out.println(Instant.now() + " Didn't get any more results, but verification hasn't passed (yet). Retrying...");
                }
            } else {
                retry = 0;
                for (final ConsumerRecord<String, Number> record : records) {
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
                          .add(record);
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

        final Map<String, Set<Number>> received =
            events.get("echo")
                  .entrySet()
                  .stream()
                  .map(entry -> mkEntry(
                      entry.getKey(),
                      entry.getValue().stream().map(ConsumerRecord::value).collect(Collectors.toSet()))
                  )
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

        // give it one more try if it's not already passing.
        if (!verificationResult.passed()) {
            verificationResult = verifyAll(inputs, events);
        }
        success &= verificationResult.passed();

        System.out.println(verificationResult.result());

        System.out.println(success ? "SUCCESS" : "FAILURE");
        return verificationResult;
    }

    public static class VerificationResult {
        private final boolean passed;
        private final String result;

        VerificationResult(final boolean passed, final String result) {
            this.passed = passed;
            this.result = result;
        }

        public boolean passed() {
            return passed;
        }

        public String result() {
            return result;
        }
    }

    private static VerificationResult verifyAll(final Map<String, Set<Integer>> inputs,
                                                final Map<String, Map<String, LinkedList<ConsumerRecord<String, Number>>>> events) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        boolean pass;
        try (final PrintStream resultStream = new PrintStream(byteArrayOutputStream)) {
            pass = verifyTAgg(resultStream, inputs, events.get("tagg"));
            pass &= verifySuppressed(resultStream, "min-suppressed", events);
            pass &= verify(resultStream, "min-suppressed", inputs, events, windowedKey -> {
                final String unwindowedKey = windowedKey.substring(1, windowedKey.length() - 1).replaceAll("@.*", "");
                return getMin(unwindowedKey);
            });
            pass &= verifySuppressed(resultStream, "sws-suppressed", events);
            pass &= verify(resultStream, "min", inputs, events, SmokeTestDriver::getMin);
            pass &= verify(resultStream, "max", inputs, events, SmokeTestDriver::getMax);
            pass &= verify(resultStream, "dif", inputs, events, key -> getMax(key).intValue() - getMin(key).intValue());
            pass &= verify(resultStream, "sum", inputs, events, SmokeTestDriver::getSum);
            pass &= verify(resultStream, "cnt", inputs, events, key1 -> getMax(key1).intValue() - getMin(key1).intValue() + 1L);
            pass &= verify(resultStream, "avg", inputs, events, SmokeTestDriver::getAvg);
        }
        return new VerificationResult(pass, new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8));
    }

    private static boolean verify(final PrintStream resultStream,
                                  final String topic,
                                  final Map<String, Set<Integer>> inputData,
                                  final Map<String, Map<String, LinkedList<ConsumerRecord<String, Number>>>> events,
                                  final Function<String, Number> keyToExpectation) {
        final Map<String, LinkedList<ConsumerRecord<String, Number>>> observedInputEvents = events.get("data");
        final Map<String, LinkedList<ConsumerRecord<String, Number>>> outputEvents = events.getOrDefault(topic, emptyMap());
        if (outputEvents.isEmpty()) {
            resultStream.println(topic + " is empty");
            return false;
        } else {
            resultStream.printf("verifying %s with %d keys%n", topic, outputEvents.size());

            if (outputEvents.size() != inputData.size()) {
                resultStream.printf("fail: resultCount=%d expectedCount=%s%n\tresult=%s%n\texpected=%s%n",
                                    outputEvents.size(), inputData.size(), outputEvents.keySet(), inputData.keySet());
                return false;
            }
            for (final Map.Entry<String, LinkedList<ConsumerRecord<String, Number>>> entry : outputEvents.entrySet()) {
                final String key = entry.getKey();
                final Number expected = keyToExpectation.apply(key);
                final Number actual = entry.getValue().getLast().value();
                if (!expected.equals(actual)) {
                    resultStream.printf("%s fail: key=%s actual=%s expected=%s%n\t inputEvents=%n%s%n\toutputEvents=%n%s%n",
                                        topic,
                                        key,
                                        actual,
                                        expected,
                                        indent("\t\t", observedInputEvents.get(key)),
                                        indent("\t\t", entry.getValue()));
                    return false;
                }
            }
            return true;
        }
    }


    private static boolean verifySuppressed(final PrintStream resultStream,
                                            @SuppressWarnings("SameParameterValue") final String topic,
                                            final Map<String, Map<String, LinkedList<ConsumerRecord<String, Number>>>> events) {
        resultStream.println("verifying suppressed " + topic);
        final Map<String, LinkedList<ConsumerRecord<String, Number>>> topicEvents = events.getOrDefault(topic, emptyMap());
        for (final Map.Entry<String, LinkedList<ConsumerRecord<String, Number>>> entry : topicEvents.entrySet()) {
            if (entry.getValue().size() != 1) {
                final String unsuppressedTopic = topic.replace("-suppressed", "-raw");
                final String key = entry.getKey();
                final String unwindowedKey = key.substring(1, key.length() - 1).replaceAll("@.*", "");
                resultStream.printf("fail: key=%s%n\tnon-unique result:%n%s%n\traw results:%n%s%n\tinput data:%n%s%n",
                                    key,
                                    indent("\t\t", entry.getValue()),
                                    indent("\t\t", events.get(unsuppressedTopic).get(key)),
                                    indent("\t\t", events.get("data").get(unwindowedKey))
                );
                return false;
            }
        }
        return true;
    }

    private static String indent(@SuppressWarnings("SameParameterValue") final String prefix,
                                 final Iterable<ConsumerRecord<String, Number>> list) {
        final StringBuilder stringBuilder = new StringBuilder();
        for (final ConsumerRecord<String, Number> record : list) {
            stringBuilder.append(prefix).append(record).append('\n');
        }
        return stringBuilder.toString();
    }

    private static Long getSum(final String key) {
        final int min = getMin(key).intValue();
        final int max = getMax(key).intValue();
        return ((long) min + max) * (max - min + 1L) / 2L;
    }

    private static Double getAvg(final String key) {
        final int min = getMin(key).intValue();
        final int max = getMax(key).intValue();
        return ((long) min + max) / 2.0;
    }


    private static boolean verifyTAgg(final PrintStream resultStream,
                                      final Map<String, Set<Integer>> allData,
                                      final Map<String, LinkedList<ConsumerRecord<String, Number>>> taggEvents) {
        if (taggEvents == null) {
            resultStream.println("tagg is missing");
            return false;
        } else if (taggEvents.isEmpty()) {
            resultStream.println("tagg is empty");
            return false;
        } else {
            resultStream.println("verifying tagg");

            // generate expected answer
            final Map<String, Long> expected = new HashMap<>();
            for (final String key : allData.keySet()) {
                final int min = getMin(key).intValue();
                final int max = getMax(key).intValue();
                final String cnt = Long.toString(max - min + 1L);

                expected.put(cnt, expected.getOrDefault(cnt, 0L) + 1);
            }

            // check the result
            for (final Map.Entry<String, LinkedList<ConsumerRecord<String, Number>>> entry : taggEvents.entrySet()) {
                final String key = entry.getKey();
                Long expectedCount = expected.remove(key);
                if (expectedCount == null) {
                    expectedCount = 0L;
                }

                if (entry.getValue().getLast().value().longValue() != expectedCount) {
                    resultStream.println("fail: key=" + key + " tagg=" + entry.getValue() + " expected=" + expected.get(key));
                    resultStream.println("\t outputEvents: " + entry.getValue());
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
        final List<TopicPartition> partitions = new ArrayList<>();

        for (final String topic : topics) {
            for (final PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

}
