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


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static kafka.utils.TestUtils.consumeRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * testCompression
 * <p>
 * Compressed messages should be able to sent and consumed correctly
 */
@ClusterTestDefaults(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false")
        }
)
class ProducerCompressionTest {

    private final String topicName = "topic";
    private final int numRecords = 2000;


    @ClusterTest
    void testCompression(ClusterInstance cluster) {
        Set<String> compressionSet = Set.of("none"/*, "gzip", "snappy", "lz4", "zstd"*/);
        Set<String> protocolSet = Set.of("classic");
        compressionSet.forEach(compression -> {
            protocolSet.forEach(protocol -> {
                try {
                    processCompressionTest(cluster, compression, protocol);
                } catch (InterruptedException | ExecutionException e) {
                    fail(e);
                }
            });
        });
    }


    void processCompressionTest(ClusterInstance cluster, String compression, String protocol) throws InterruptedException,
            ExecutionException {
        String compressionTopic = topicName + "_" + compression;
        cluster.createTopic(compressionTopic, 1, (short) 1);
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "66000");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        Producer<byte[], byte[]> producer = cluster.producer(producerProps);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group_" + compression);
        consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        Consumer<byte[], byte[]> consumer = cluster.consumer(consumerProps);
        try (producer; consumer) {
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
                assertEquals(offset, responses.get(offset).get().offset());
            }
            verifyConsumerRecords(consumer, messages, now, headerArr, partition, compressionTopic);
            System.out.println("verify success");
        } finally {
            System.out.println("start close");
            producer.close();
            consumer.close();
            System.out.println("close success");
        }
    }

    private void verifyConsumerRecords(Consumer<byte[], byte[]> consumer, List<String> messages, long now,
                                       Header[] headerArr, int partition, String topic) {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 0);
        AtomicInteger num = new AtomicInteger(0);
        AtomicInteger flag = new AtomicInteger(0);
        consumeRecords(consumer, numRecords * 3, TestUtils.DEFAULT_MAX_WAIT_MS).foreach(record -> {
            String messageValue = messages.get(num.get());
            long offset = num.get() * 3L + flag.get();
            if (flag.get() == 0) {
                //  verify message without key and header
                assertNull(record.key());
                assertEquals(messageValue, new String(record.value()));
                assertEquals(0, record.headers().toArray().length);
                assertEquals(now, record.timestamp());
                assertEquals(offset, record.offset());
            } else if (flag.get() == 1) {
                //  verify message with key, without header
                assertEquals(String.valueOf(messageValue.length()), new String(record.key()));
                assertEquals(messageValue, new String(record.value()));
                assertEquals(0, record.headers().toArray().length);
                assertEquals(now, record.timestamp());
                assertEquals(offset, record.offset());
            } else if (flag.get() == 2) {
                //  verify message with key and header
                assertEquals(String.valueOf(messageValue.length()), new String(record.key()));
                assertEquals(messageValue, new String(record.value()));
                assertEquals(1, record.headers().toArray().length);
                assertEquals(headerArr[0], record.headers().toArray()[0]);
                assertEquals(now, record.timestamp());
                assertEquals(offset, record.offset());
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

}
