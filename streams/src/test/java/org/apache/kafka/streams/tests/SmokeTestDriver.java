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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SmokeTestDriver extends SmokeTestUtil {

    public static final int MAX_RECORD_EMPTY_RETRIES = 60;

    private static class ValueList {
        public final String key;
        private final int[] values;
        private int index;

        ValueList(int min, int max) {
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

    // This main() is not used by the system test. It is intended to be used for local debugging.
    public static void main(String[] args) throws InterruptedException {
        final String kafka = "localhost:9092";
        final File stateDir = TestUtils.tempDirectory();

        final int numKeys = 20;
        final int maxRecordsPerKey = 1000;

        Thread driver = new Thread() {
            public void run() {
                try {
                    Map<String, Set<Integer>> allData = generate(kafka, numKeys, maxRecordsPerKey);
                    verify(kafka, allData, maxRecordsPerKey);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        SmokeTestClient streams1 = new SmokeTestClient(createDir(stateDir, "1"), kafka);
        SmokeTestClient streams2 = new SmokeTestClient(createDir(stateDir, "2"), kafka);
        SmokeTestClient streams3 = new SmokeTestClient(createDir(stateDir, "3"), kafka);
        SmokeTestClient streams4 = new SmokeTestClient(createDir(stateDir, "4"), kafka);

        System.out.println("starting the driver");
        driver.start();

        System.out.println("starting the first and second client");
        streams1.start();
        streams2.start();

        sleep(10000);

        System.out.println("starting the third client");
        streams3.start();

        System.out.println("closing the first client");
        streams1.close();
        System.out.println("closed the first client");

        sleep(10000);

        System.out.println("starting the forth client");
        streams4.start();

        driver.join();

        System.out.println("driver stopped");
        streams2.close();
        streams3.close();
        streams4.close();

        System.out.println("shutdown");
    }

    public static Map<String, Set<Integer>> generate(String kafka, final int numKeys, final int maxRecordsPerKey) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // the next 4 config values make sure that all records are produced with no loss and
        // no duplicates
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);

        int numRecordsProduced = 0;

        Map<String, Set<Integer>> allData = new HashMap<>();
        ValueList[] data = new ValueList[numKeys];
        for (int i = 0; i < numKeys; i++) {
            data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
            allData.put(data[i].key, new HashSet<Integer>());
        }
        Random rand = new Random();

        int remaining = data.length;

        while (remaining > 0) {
            int index = rand.nextInt(remaining);
            String key = data[index].key;
            int value = data[index].next();

            if (value < 0) {
                remaining--;
                data[index] = data[remaining];
            } else {

                ProducerRecord<byte[], byte[]> record =
                        new ProducerRecord<>("data", stringSerde.serializer().serialize("", key), intSerde.serializer().serialize("", value));

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
                allData.get(key).add(value);
                if (numRecordsProduced % 100 == 0)
                    System.out.println(numRecordsProduced + " records produced");
                Utils.sleep(2);

            }
        }
        producer.close();
        return Collections.unmodifiableMap(allData);
    }

    private static void shuffle(int[] data, int windowSize) {
        Random rand = new Random();
        for (int i = 0; i < data.length; i++) {
            // we shuffle data within windowSize
            int j = rand.nextInt(Math.min(data.length - i, windowSize)) + i;

            // swap
            int tmp = data[i];
            data[i] = data[j];
            data[j] = tmp;
        }
    }

    public static void verify(String kafka, Map<String, Set<Integer>> allData, int maxRecordsPerKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> partitions = getAllPartitions(consumer, "echo", "max", "min", "dif", "sum", "cnt", "avg", "wcnt", "tagg");
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        final int recordsGenerated = allData.size() * maxRecordsPerKey;
        int recordsProcessed = 0;

        HashMap<String, Integer> max = new HashMap<>();
        HashMap<String, Integer> min = new HashMap<>();
        HashMap<String, Integer> dif = new HashMap<>();
        HashMap<String, Long> sum = new HashMap<>();
        HashMap<String, Long> cnt = new HashMap<>();
        HashMap<String, Double> avg = new HashMap<>();
        HashMap<String, Long> wcnt = new HashMap<>();
        HashMap<String, Long> tagg = new HashMap<>();

        HashSet<String> keys = new HashSet<>();
        HashMap<String, Set<Integer>> received = new HashMap<>();
        for (String key : allData.keySet()) {
            keys.add(key);
            received.put(key, new HashSet<Integer>());
        }
        int retry = 0;
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(6)) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
            if (records.isEmpty() && recordsProcessed >= recordsGenerated) {
                if (verifyMin(min, allData, false)
                    && verifyMax(max, allData, false)
                    && verifyDif(dif, allData, false)
                    && verifySum(sum, allData, false)
                    && verifyCnt(cnt, allData, false)
                    && verifyAvg(avg, allData, false)
                    && verifyTAgg(tagg, allData, false)) {
                    break;
                }
                if (retry++ > MAX_RECORD_EMPTY_RETRIES) {
                    break;
                }
            } else {
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String key = stringSerde.deserializer().deserialize("", record.key());
                    switch (record.topic()) {
                        case "echo":
                            Integer value = intSerde.deserializer().deserialize("", record.value());
                            recordsProcessed++;
                            if (recordsProcessed % 100 == 0) {
                                System.out.println("Echo records processed = " + recordsProcessed);
                            }
                            received.get(key).add(value);
                            break;
                        case "min":
                            min.put(key, intSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "max":
                            max.put(key, intSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "dif":
                            dif.put(key, intSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "sum":
                            sum.put(key, longSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "cnt":
                            cnt.put(key, longSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "avg":
                            avg.put(key, doubleSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "wcnt":
                            wcnt.put(key, longSerde.deserializer().deserialize("", record.value()));
                            break;
                        case "tagg":
                            tagg.put(key, longSerde.deserializer().deserialize("", record.value()));
                            break;
                        default:
                            System.out.println("unknown topic: " + record.topic());
                    }
                }
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
            for (Map.Entry<String, Set<Integer>> entry : allData.entrySet()) {
                missedCount += received.get(entry.getKey()).size();
            }
            System.out.println("missedRecords=" + missedCount);
        }

        success &= verifyMin(min, allData, true);
        success &= verifyMax(max, allData, true);
        success &= verifyDif(dif, allData, true);
        success &= verifySum(sum, allData, true);
        success &= verifyCnt(cnt, allData, true);
        success &= verifyAvg(avg, allData, true);
        success &= verifyTAgg(tagg, allData, true);

        System.out.println(success ? "SUCCESS" : "FAILURE");
    }

    private static boolean verifyMin(Map<String, Integer> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                int expected = getMin(entry.getKey());
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " min=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyMax(Map<String, Integer> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                int expected = getMax(entry.getKey());
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " max=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyDif(Map<String, Integer> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                int min = getMin(entry.getKey());
                int max = getMax(entry.getKey());
                int expected = max - min;
                if (entry.getValue() == null || expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " dif=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifyCnt(Map<String, Long> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                int min = getMin(entry.getKey());
                int max = getMax(entry.getKey());
                long expected = (max - min) + 1L;
                if (expected != entry.getValue()) {
                    if (print) {
                        System.out.println("fail: key=" + entry.getKey() + " cnt=" + entry.getValue() + " expected=" + expected);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean verifySum(Map<String, Long> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                int min = getMin(entry.getKey());
                int max = getMax(entry.getKey());
                long expected = ((long) min + (long) max) * (max - min + 1L) / 2L;
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

    private static boolean verifyAvg(Map<String, Double> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            for (Map.Entry<String, Double> entry : map.entrySet()) {
                int min = getMin(entry.getKey());
                int max = getMax(entry.getKey());
                double expected = ((long) min + (long) max) / 2.0;

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


    private static boolean verifyTAgg(Map<String, Long> map, Map<String, Set<Integer>> allData, final boolean print) {
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
            Map<String, Long> expected = new HashMap<>();
            for (String key : allData.keySet()) {
                int min = getMin(key);
                int max = getMax(key);
                String cnt = Long.toString(max - min + 1L);

                if (expected.containsKey(cnt)) {
                    expected.put(cnt, expected.get(cnt) + 1L);
                } else {
                    expected.put(cnt, 1L);
                }
            }

            // check the result
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                String key = entry.getKey();
                Long expectedCount = expected.remove(key);
                if (expectedCount == null)
                    expectedCount = 0L;

                if (entry.getValue() != expectedCount) {
                    if (print) {
                        System.out.println("fail: key=" + key + " tagg=" + entry.getValue() + " expected=" + expected.get(key));
                    }
                    return false;
                }
            }

        }
        return true;
    }

    private static int getMin(String key) {
        return Integer.parseInt(key.split("-")[0]);
    }

    private static int getMax(String key) {
        return Integer.parseInt(key.split("-")[1]);
    }

    private static int getMinFromWKey(String key) {
        return getMin(key.split("@")[0]);
    }

    private static int getMaxFromWKey(String key) {
        return getMax(key.split("@")[0]);
    }

    private static long getStartFromWKey(String key) {
        return Long.parseLong(key.split("@")[1]);
    }

    private static List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, String... topics) {
        ArrayList<TopicPartition> partitions = new ArrayList<>();

        for (String topic : topics) {
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

}
