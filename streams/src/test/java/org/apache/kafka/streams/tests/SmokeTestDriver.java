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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SmokeTestDriver extends SmokeTestUtil {

    private static final int MAX_RECORD_EMPTY_RETRIES = 60;

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

    public static Map<String, Set<Integer>> generate(final String kafka,
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
                    new ProducerRecord<>("data", stringSerde.serializer().serialize("", key), intSerde.serializer().serialize("", value));

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

    private static void shuffle(final int[] data, final int windowSize) {
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

    public static boolean verify(final String kafka, final Map<String, Set<Integer>> allData, final int maxRecordsPerKey) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        final List<TopicPartition> partitions = getAllPartitions(consumer, "data", "echo", "max", "min", "dif", "sum", "cnt", "avg", "wcnt", "tagg");
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        final int recordsGenerated = allData.size() * maxRecordsPerKey;
        int recordsProcessed = 0;
        final Map<String, AtomicInteger> processed = Stream.of("data", "echo", "max", "min", "dif", "sum", "cnt", "avg", "wcnt", "tagg").collect(Collectors.toMap(t -> t, t -> new AtomicInteger(0)));

        final HashMap<String, LinkedList<Integer>> dataEvents = new HashMap<>();
        final HashMap<String, LinkedList<Integer>> echoEvents = new HashMap<>();
        final HashMap<String, LinkedList<Integer>> maxEvents = new HashMap<>();
        final HashMap<String, LinkedList<Integer>> minEvents = new HashMap<>();
        final HashMap<String, LinkedList<Integer>> difEvents = new HashMap<>();
        final HashMap<String, LinkedList<Long>> sumEvents = new HashMap<>();
        final HashMap<String, LinkedList<Long>> cntEvents = new HashMap<>();
        final HashMap<String, LinkedList<Double>> avgEvents = new HashMap<>();
        final HashMap<String, LinkedList<Long>> wcntEvents = new HashMap<>();
        final HashMap<String, LinkedList<Long>> taggEvents = new HashMap<>();

        final HashMap<String, Integer> max = new HashMap<>();
        final HashMap<String, Integer> min = new HashMap<>();
        final HashMap<String, Integer> dif = new HashMap<>();
        final HashMap<String, Long> sum = new HashMap<>();
        final HashMap<String, Long> cnt = new HashMap<>();
        final HashMap<String, Double> avg = new HashMap<>();
        final HashMap<String, Long> wcnt = new HashMap<>();
        final HashMap<String, Long> tagg = new HashMap<>();

        final HashSet<String> keys = new HashSet<>();
        final HashMap<String, Set<Integer>> received = new HashMap<>();
        for (final String key : allData.keySet()) {
            keys.add(key);
            received.put(key, new HashSet<>());
        }
        int retry = 0;
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(6)) {
            final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty() && recordsProcessed >= recordsGenerated) {
                if (verifyMin(min, allData, false, dataEvents, minEvents)
                    && verifyMax(max, allData, false, dataEvents, maxEvents)
                    && verifyDif(dif, allData, false, dataEvents, difEvents)
                    && verifySum(sum, allData, false, dataEvents, sumEvents)
                    && verifyCnt(cnt, allData, false, dataEvents, cntEvents)
                    && verifyAvg(avg, allData, false, dataEvents, avgEvents)
                    && verifyTAgg(tagg, allData, false, dataEvents, taggEvents)) {
                    break;
                }
                if (retry++ > MAX_RECORD_EMPTY_RETRIES) {
                    break;
                }
            } else {
                for (final ConsumerRecord<String, byte[]> record : records) {
                    final String key = record.key();
                    processed.get(record.topic()).incrementAndGet();
                    switch (record.topic()) {
                        case "data": {
                            addEvent(key, dataEvents, intSerde.deserializer().deserialize("", record.value()));
                            break;
                        }
                        case "echo": {
                            recordsProcessed++;
                            if (recordsProcessed % 100 == 0) {
                                System.out.println("Echo records processed = " + recordsProcessed);
                            }
                            final Integer deserialize = intSerde.deserializer().deserialize("", record.value());
                            received.get(key).add(deserialize);
                            addEvent(key, echoEvents, deserialize);
                            break;
                        }
                        case "min": {
                            final Integer deserialize = intSerde.deserializer().deserialize("", record.value());
                            min.put(key, deserialize);
                            addEvent(key, minEvents, deserialize);
                            break;
                        }
                        case "max": {
                            final Integer deserialize = intSerde.deserializer().deserialize("", record.value());
                            max.put(key, deserialize);
                            addEvent(key, maxEvents, deserialize);
                            break;
                        }
                        case "dif": {
                            final Integer deserialize = intSerde.deserializer().deserialize("", record.value());
                            dif.put(key, deserialize);
                            addEvent(key, difEvents, deserialize);
                            break;
                        }
                        case "sum": {
                            final Long deserialize = longSerde.deserializer().deserialize("", record.value());
                            sum.put(key, deserialize);
                            break;
                        }
                        case "cnt": {
                            final Long deserialize = longSerde.deserializer().deserialize("", record.value());
                            cnt.put(key, deserialize);
                            addEvent(key, cntEvents, deserialize);
                            break;
                        }
                        case "avg": {
                            final Double deserialize = doubleSerde.deserializer().deserialize("", record.value());
                            avg.put(key, deserialize);
                            addEvent(key, avgEvents, deserialize);
                            break;
                        }
                        case "wcnt": {
                            final Long deserialize = longSerde.deserializer().deserialize("", record.value());
                            wcnt.put(key, deserialize);
                            addEvent(key, wcntEvents, deserialize);
                            break;
                        }
                        case "tagg": {
                            final Long deserialize = longSerde.deserializer().deserialize("", record.value());
                            tagg.put(key, deserialize);
                            addEvent(key, taggEvents, deserialize);
                            break;
                        }
                        default:
                            System.out.println("unknown topic: " + record.topic());
                    }
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
        success = allData.equals(received);

        if (success) {
            System.out.println("ALL-RECORDS-DELIVERED");
        } else {
            int missedCount = 0;
            for (final Map.Entry<String, Set<Integer>> entry : allData.entrySet()) {
                missedCount += received.get(entry.getKey()).size();
            }
            System.out.println("missedRecords=" + missedCount);
        }

        success &= verifyMin(min, allData, true, dataEvents, minEvents);
        success &= verifyMax(max, allData, true, dataEvents, maxEvents);
        success &= verifyDif(dif, allData, true, dataEvents, difEvents);
        success &= verifySum(sum, allData, true, dataEvents, sumEvents);
        success &= verifyCnt(cnt, allData, true, dataEvents, cntEvents);
        success &= verifyAvg(avg, allData, true, dataEvents, avgEvents);
        success &= verifyTAgg(tagg, allData, true, dataEvents, taggEvents);

        System.out.println(success ? "SUCCESS" : "FAILURE");
        return success;
    }

    private static <V> void addEvent(final String key, final HashMap<String, LinkedList<V>> eventsMap, final V value) {
        if (!eventsMap.containsKey(key)) {
            eventsMap.put(key, new LinkedList<>());
        }
        eventsMap.get(key).add(value);
    }

    private static boolean verifyMin(final Map<String, Integer> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Integer>> minEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("min is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying min");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Integer> entry : map.entrySet()) {
                final String key = entry.getKey();
                final int expected = getMin(key);
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.printf("fail: key=%s min=%d expected=%d%n\tdata=%s%n\tmins=%s%n",
                                          key,
                                          entry.getValue(),
                                          expected,
                                          dataEvents.get(key),
                                          minEvents.get(key));
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyMax(final Map<String, Integer> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Integer>> maxEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("max is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying max");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Integer> entry : map.entrySet()) {
                final String key = entry.getKey();
                final int expected = getMax(key);
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.printf("fail: key=%s max=%d expected=%d%n\tdata=%s%n\tmaxs=%s%n",
                                          key,
                                          entry.getValue(),
                                          expected,
                                          dataEvents.get(key),
                                          maxEvents.get(key));
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyDif(final Map<String, Integer> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Integer>> difEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("dif is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying dif");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Integer> entry : map.entrySet()) {
                final String key = entry.getKey();
                final int min = getMin(key);
                final int max = getMax(key);
                final int expected = max - min;
                if (entry.getValue() == null || expected != entry.getValue()) {
                    if (print) {
                        System.out.printf("fail: key=%s dif=%d expected=%d%n\tdata=%s%n\tdifs=%s%n",
                                          key,
                                          entry.getValue(),
                                          expected,
                                          dataEvents.get(key),
                                          difEvents.get(key));
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyCnt(final Map<String, Long> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Long>> cntEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("cnt is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying cnt");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Long> entry : map.entrySet()) {
                final String key = entry.getKey();
                final int min = getMin(key);
                final int max = getMax(key);
                final long expected = (max - min) + 1L;
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.printf("fail: key=%s cnt=%d expected=%d%n\tdata=%s%n\tcnts=%s%n",
                                          key,
                                          entry.getValue(),
                                          expected,
                                          dataEvents.get(key),
                                          cntEvents.get(key));
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifySum(final Map<String, Long> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Long>> sumEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("sum is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying sum");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Long> entry : map.entrySet()) {
                final int min = getMin(entry.getKey());
                final int max = getMax(entry.getKey());
                final long expected = ((long) min + (long) max) * (max - min + 1L) / 2L;
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " sum=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyAvg(final Map<String, Double> map,
                                     final Map<String, Set<Integer>> allData,
                                     final boolean print,
                                     final HashMap<String, LinkedList<Integer>> dataEvents,
                                     final HashMap<String, LinkedList<Double>> avgEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("avg is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying avg");
            }

            if (map.size() != allData.size()) {
                if (print) {
                    System.out.println("fail: resultCount=" + map.size() + " expectedCount=" + allData.size());
                }
                return false;
            }
            for (final Map.Entry<String, Double> entry : map.entrySet()) {
                final int min = getMin(entry.getKey());
                final int max = getMax(entry.getKey());
                final double expected = ((long) min + (long) max) / 2.0;

                if (entry.getValue() == null || expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " avg=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }


    private static boolean verifyTAgg(final Map<String, Long> map,
                                      final Map<String, Set<Integer>> allData,
                                      final boolean print,
                                      final HashMap<String, LinkedList<Integer>> dataEvents,
                                      final HashMap<String, LinkedList<Long>> taggEvents) {
        if (map.isEmpty()) {
            if (print) {
                System.out.println("tagg is empty");
            }
            return false;
        } else {
            if (print) {
                System.out.println("verifying tagg");
            }

            // generate expected answer
            final Map<String, Long> expected = new HashMap<>();
            for (final String key : allData.keySet()) {
                final int min = getMin(key);
                final int max = getMax(key);
                final String cnt = Long.toString(max - min + 1L);

                if (expected.containsKey(cnt)) {
                    expected.put(cnt, expected.get(cnt) + 1L);
                } else {
                    expected.put(cnt, 1L);
                }
            }

            // check the result
            for (final Map.Entry<String, Long> entry : map.entrySet()) {
                final String key = entry.getKey();
                Long expectedCount = expected.remove(key);
                if (expectedCount == null) {
                    expectedCount = 0L;
                }

                if (entry.getValue().longValue() != expectedCount.longValue()) {
                    if (print) {
                        System.out.println("fail: key=" + key + " tagg=" + entry.getValue() + " expected=" + expected.get(key));
                    }
                    return false;
                }
            }

        }
        return true;
    }

    private static int getMin(final String key) {
        return Integer.parseInt(key.split("-")[0]);
    }

    private static int getMax(final String key) {
        return Integer.parseInt(key.split("-")[1]);
    }

    private static int getMinFromWKey(final String key) {
        return getMin(key.split("@")[0]);
    }

    private static int getMaxFromWKey(final String key) {
        return getMax(key.split("@")[0]);
    }

    private static long getStartFromWKey(final String key) {
        return Long.parseLong(key.split("@")[1]);
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
