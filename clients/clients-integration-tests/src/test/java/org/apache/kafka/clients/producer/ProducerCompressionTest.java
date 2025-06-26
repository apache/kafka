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
package org.apache.kafka.clients.producer;


import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static kafka.utils.TestUtils.consumeRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;


@ClusterTestDefaults(types = {Type.KRAFT})
class ProducerCompressionTest {

    private final String topicName = "topic";
    private final int numRecords = 2000;

    /**
     * testCompression
     * <p>
     * Compressed messages should be able to sent and consumed correctly
     */
    @ClusterTest
    void testCompression(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        for (CompressionType compression : CompressionType.values()) {
            processCompressionTest(cluster, compression);
        }
    }


    void processCompressionTest(ClusterInstance cluster, CompressionType compression) throws InterruptedException,
        ExecutionException {
        String compressionTopic = topicName + "_" + compression.name;
        cluster.createTopic(compressionTopic, 1, (short) 1);
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression.name);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "66000");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        Consumer<byte[], byte[]> classicConsumer = cluster.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"));
        Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"));
        try (Producer<byte[], byte[]> producer = cluster.producer(producerProps)) {
            int partition = 0;
            // prepare the messages
            List<String> messages = IntStream.range(0, numRecords).mapToObj(this::messageValue).toList();
            Header[] headerArr = new Header[]{new RecordHeader("key", "value".getBytes())};
            RecordHeaders headers = new RecordHeaders(headerArr);

            // make sure the returned messages are correct
            long now = System.currentTimeMillis();
            List<Future<RecordMetadata>> responses = new ArrayList<>();
            messages.forEach(message -> {
                // 1. send message without key and header
                responses.add(producer.send(new ProducerRecord<>(compressionTopic, null, now, null,
                    message.getBytes())));
                // 2. send message with key, without header
                responses.add(producer.send(new ProducerRecord<>(compressionTopic, null, now,
                    String.valueOf(message.length()).getBytes(), message.getBytes())));
                // 3. send message with key and header
                responses.add(producer.send(new ProducerRecord<>(compressionTopic, null, now,
                    String.valueOf(message.length()).getBytes(), message.getBytes(), headers)));
            });
            for (int offset = 0; offset < responses.size(); offset++) {
                assertEquals(offset, responses.get(offset).get().offset(), compression.name);
            }
            verifyConsumerRecords(consumer, messages, now, headerArr, partition, compressionTopic, compression.name);
            verifyConsumerRecords(classicConsumer, messages, now, headerArr, partition, compressionTopic,
                compression.name);
        } finally {
            //  This consumer close very slowly, which may cause the entire test to time out, and we can't wait for 
            //  it to  auto close 
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(1)));
            classicConsumer.close(CloseOptions.timeout(Duration.ofSeconds(1)));
        }
    }

    private void verifyConsumerRecords(Consumer<byte[], byte[]> consumer, List<String> messages, long now,
                                       Header[] headerArr, int partition, String topic, String compression) {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(List.of(tp));
        consumer.seek(tp, 0);
        AtomicInteger num = new AtomicInteger(0);
        AtomicInteger flag = new AtomicInteger(0);
        consumeRecords(consumer, numRecords * 3, TestUtils.DEFAULT_MAX_WAIT_MS).foreach(record -> {
            String messageValue = messages.get(num.get());
            long offset = num.get() * 3L + flag.get();
            if (flag.get() == 0) {
                //  verify message without key and header
                assertNull(record.key(), errorMessage(compression));
                assertEquals(messageValue, new String(record.value()), errorMessage(compression));
                assertEquals(0, record.headers().toArray().length, errorMessage(compression));
                assertEquals(now, record.timestamp(), errorMessage(compression));
                assertEquals(offset, record.offset(), errorMessage(compression));
            } else if (flag.get() == 1) {
                //  verify message with key, without header
                assertEquals(String.valueOf(messageValue.length()), new String(record.key()), errorMessage(compression));
                assertEquals(messageValue, new String(record.value()), errorMessage(compression));
                assertEquals(0, record.headers().toArray().length, errorMessage(compression));
                assertEquals(now, record.timestamp(), errorMessage(compression));
                assertEquals(offset, record.offset(), errorMessage(compression));
            } else if (flag.get() == 2) {
                //  verify message with key and header
                assertEquals(String.valueOf(messageValue.length()), new String(record.key()), errorMessage(compression));
                assertEquals(messageValue, new String(record.value()), errorMessage(compression));
                assertEquals(1, record.headers().toArray().length, errorMessage(compression));
                assertEquals(headerArr[0], record.headers().toArray()[0], errorMessage(compression));
                assertEquals(now, record.timestamp(), errorMessage(compression));
                assertEquals(offset, record.offset(), errorMessage(compression));
            } else {
                fail();
            }
            flagLoop(num, flag);
            return null;
        });
    }

    private void flagLoop(AtomicInteger num, AtomicInteger flag) {
        if (flag.get() == 2) {
            num.incrementAndGet();
            flag.set(0);
        } else {
            flag.incrementAndGet();
        }
    }

    private String messageValue(int length) {
        Random random = new Random();
        return IntStream.range(0, length)
            .map(i -> random.nextInt(TestUtils.LETTERS_AND_DIGITS.length()))
            .mapToObj(TestUtils.LETTERS_AND_DIGITS::charAt)
            .map(String::valueOf)
            .collect(Collectors.joining());
    }

    private String errorMessage(String compression) {
        return String.format("Compression type: %s - Assertion failed", compression);
    }

}
