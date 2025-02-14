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

package org.apache.kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * This class records the average end to end latency for a single message to travel through Kafka.
 * Following are the required arguments
 * <p> broker_list = location of the bootstrap broker for both the producer and the consumer </p>
 * <p> topic = topic name used by both the producer and the consumer to send/receive messages </p>
 * <p> num_messages = # messages to send </p>
 * <p> producer_acks = See ProducerConfig.ACKS_DOC </p>
 * <p> message_size_bytes = size of each message in bytes </p>
 *
 * <p> e.g. [localhost:9092 test 10000 1 20] </p>
 */
public class EndToEndLatency {
    private static final long POLL_TIMEOUT_MS = 60000;
    private static final short DEFAULT_REPLICATION_FACTOR = 1;
    private static final int DEFAULT_NUM_PARTITIONS = 1;

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    // Visible for testing
    static void execute(String... args) throws Exception {
        if (args.length != 5 && args.length != 6) {
            throw new TerseException("USAGE: java " + EndToEndLatency.class.getName()
                    + " broker_list topic num_messages producer_acks message_size_bytes [optional] properties_file");
        }

        String brokers = args[0];
        String topic = args[1];
        int numMessages = Integer.parseInt(args[2]);
        String acks = args[3];
        int messageSizeBytes = Integer.parseInt(args[4]);
        Optional<String> propertiesFile = (args.length > 5 && !Utils.isBlank(args[5])) ? Optional.of(args[5]) : Optional.empty();

        if (!Arrays.asList("1", "all").contains(acks)) {
            throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all");
        }

        try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(propertiesFile, brokers);
             KafkaProducer<byte[], byte[]> producer = createKafkaProducer(propertiesFile, brokers, acks)) {

            if (!consumer.listTopics().containsKey(topic)) {
                createTopic(propertiesFile, brokers, topic);
            }
            setupConsumer(topic, consumer);
            double totalTime = 0.0;
            long[] latencies = new long[numMessages];
            Random random = new Random(0);

            for (int i = 0; i < numMessages; i++) {
                byte[] message = randomBytesOfLen(random, messageSizeBytes);
                long begin = System.nanoTime();
                //Send message (of random bytes) synchronously then immediately poll for it
                producer.send(new ProducerRecord<>(topic, message)).get();
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                long elapsed = System.nanoTime() - begin;

                validate(consumer, message, records);

                //Report progress
                if (i % 1000 == 0)
                    System.out.println(i + "\t" + elapsed / 1000.0 / 1000.0);
                totalTime += elapsed;
                latencies[i] = elapsed / 1000 / 1000;
            }

            printResults(numMessages, totalTime, latencies);
            consumer.commitSync();
        }
    }

    // Visible for testing
    static void validate(KafkaConsumer<byte[], byte[]> consumer, byte[] message, ConsumerRecords<byte[], byte[]> records) {
        if (records.isEmpty()) {
            consumer.commitSync();
            throw new RuntimeException("poll() timed out before finding a result (timeout:[" + POLL_TIMEOUT_MS + "])");
        }

        //Check result matches the original record
        String sent = new String(message, StandardCharsets.UTF_8);
        String read = new String(records.iterator().next().value(), StandardCharsets.UTF_8);

        if (!read.equals(sent)) {
            consumer.commitSync();
            throw new RuntimeException("The message read [" + read + "] did not match the message sent [" + sent + "]");
        }

        //Check we only got the one message
        if (records.count() != 1) {
            int count = records.count();
            consumer.commitSync();
            throw new RuntimeException("Only one result was expected during this test. We found [" + count + "]");
        }
    }

    private static void setupConsumer(String topic, KafkaConsumer<byte[], byte[]> consumer) {
        List<TopicPartition> topicPartitions = consumer
                .partitionsFor(topic)
                .stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .toList();
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        consumer.assignment().forEach(consumer::position);
    }

    private static void printResults(int numMessages, double totalTime, long[] latencies) {
        System.out.printf("Avg latency: %.4f ms%n", totalTime / numMessages / 1000.0 / 1000.0);
        Arrays.sort(latencies);
        int p50 = (int) latencies[(int) (latencies.length * 0.5)];
        int p99 = (int) latencies[(int) (latencies.length * 0.99)];
        int p999 = (int) latencies[(int) (latencies.length * 0.999)];
        System.out.printf("Percentiles: 50th = %d, 99th = %d, 99.9th = %d%n", p50, p99, p999);
    }

    private static byte[] randomBytesOfLen(Random random, int length) {
        byte[] randomBytes = new byte[length];
        Arrays.fill(randomBytes, Integer.valueOf(random.nextInt(26) + 65).byteValue());
        return randomBytes;
    }

    private static void createTopic(Optional<String> propertiesFile, String brokers, String topic) throws IOException {
        System.out.printf("Topic \"%s\" does not exist. "
                        + "Will create topic with %d partition(s) and replication factor = %d%n",
                topic, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR);

        Properties adminProps = loadPropsWithBootstrapServers(propertiesFile, brokers);
        Admin adminClient = Admin.create(adminProps);
        NewTopic newTopic = new NewTopic(topic, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.out.printf("Creation of topic %s failed%n", topic);
            throw new RuntimeException(e);
        } finally {
            Utils.closeQuietly(adminClient, "AdminClient");
        }
    }

    private static Properties loadPropsWithBootstrapServers(Optional<String> propertiesFile, String brokers) throws IOException {
        Properties properties = propertiesFile.isPresent() ? Utils.loadProps(propertiesFile.get()) : new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return properties;
    }

    private static KafkaConsumer<byte[], byte[]> createKafkaConsumer(Optional<String> propsFile, String brokers) throws IOException {
        Properties consumerProps = loadPropsWithBootstrapServers(propsFile, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); //ensure we have no temporal batching
        return new KafkaConsumer<>(consumerProps);
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(Optional<String> propsFile, String brokers, String acks) throws IOException {
        Properties producerProps = loadPropsWithBootstrapServers(propsFile, brokers);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0"); //ensure writes are synchronous
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }
}
