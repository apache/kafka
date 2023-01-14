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


import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * This class records the average end to end latency for a single message to travel through Kafka
 *
 * broker_list = location of the bootstrap broker for both the producer and the consumer
 * num_messages = # messages to send
 * producer_acks = See ProducerConfig.ACKS_DOC
 * message_size_bytes = size of each message in bytes
 *
 * e.g. [localhost:9092 test 10000 1 20]
 */
public class EndToEndLatency {

    private final static long POLL_TIMEOUT_MS = 60000;

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
        ArgumentParser parser = addArguments();
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        String brokers = res.getString("broker_list");
        String topic = res.getString("topic");
        int numMessages = res.getInt("num_messages");
        String acks = res.getString("producer_acks");
        int messageSizeBytes = res.getInt("message_size_bytes");
        String propertiesFile = res.getString("properties_file");

        if (!Arrays.asList("1", "all").contains(acks)) {
            throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all");
        }

        Properties props;
        try {
            props = loadPropsWithBootstrapServers(propertiesFile);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Properties file not found");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(props, brokers);
             KafkaProducer<byte[], byte[]> producer = createKafkaProducer(props, acks, brokers)) {

            if (!consumer.listTopics().containsKey(topic)) {
                createTopic(props, topic);
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

            //Results
            printResults(numMessages, totalTime, latencies);
            consumer.commitSync();
        }
    }

    static ArgumentParser addArguments() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("end-to-end-latency")
                .defaultHelp(true)
                .description("This tool records the average end to end latency for a single message to travel through Kafka");
        parser
                .addArgument("-b", "--brokers")
                .action(store())
                .type(String.class)
                .dest("broker_list")
                .help("The location of the bootstrap broker for both the producer and the consumer");
        parser
                .addArgument("-t", "--topic")
                .action(store())
                .type(String.class)
                .dest("topic")
                .help("The Kakfa topic to send/receive messages to/from");
        parser
                .addArgument("-n", "--num-records")
                .action(store())
                .type(Integer.class)
                .dest("num_messages")
                .help("The number of messages to send");
        parser
                .addArgument("-a", "--acks")
                .action(store())
                .type(String.class)
                .dest("producer_acks")
                .help("The number of messages to send");
        parser
                .addArgument("-s", "--message-bytes")
                .required(true)
                .action(store())
                .type(Integer.class)
                .dest("message_size_bytes")
                .help("Size of each message in bytes");
        parser
                .addArgument("-f", "--properties-file")
                .action(store())
                .type(String.class)
                .dest("properties_file");
        return parser;
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
        List<TopicPartition> topicPartitions = consumer.
                partitionsFor(topic).
                stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(Collectors.toList());
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
        System.out.printf("Percentiles: 50th = %d, 99th = %d, 99.9th = %d", p50, p99, p999);
    }

    private static byte[] randomBytesOfLen(Random random, int length) {
        byte[] randomBytes = new byte[length];
        Arrays.fill(randomBytes, Integer.valueOf(random.nextInt(26) + 65).byteValue());
        return randomBytes;
    }

    private static void createTopic(Properties props, String topic) {

        short defaultReplicationFactor = 1;
        int defaultNumPartitions = 1;

        System.out.printf("Topic \"%s\" does not exist. " +
                        "Will create topic with %d partition(s) and replication factor = %d%n",
                topic, defaultNumPartitions, defaultReplicationFactor);

        Admin adminClient = Admin.create(props);
        NewTopic newTopic = new NewTopic(topic, defaultNumPartitions, defaultReplicationFactor);
        adminClient.createTopics(Collections.singletonList(newTopic));
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.out.printf("Creation of topic %s failed", topic);
            throw new RuntimeException(e);
        } finally {
            Utils.closeQuietly(adminClient, "AdminClient");
        }
    }

    private static Properties loadPropsWithBootstrapServers(String propertiesFile) throws IOException {
        return propertiesFile != null ? Utils.loadProps(propertiesFile) : new Properties();
    }

    private static KafkaConsumer<byte[], byte[]> createKafkaConsumer(Properties properties, String brokers) {
        Properties consumerProps = new Properties(properties);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); //ensure we have no temporal batching
        System.out.println(consumerProps);
        return new KafkaConsumer<>(consumerProps);
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(Properties properties, String acks, String brokers) {
        Properties producerProps = new Properties(properties);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0"); //ensure writes are synchronous
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        System.out.println(producerProps);
        return new KafkaProducer<>(producerProps);
    }

}
