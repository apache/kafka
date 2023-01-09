package org.apache.kafka.tools;


import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
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

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }
    private final static long timeout = 60000;

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

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("end-to-end-latency")
                .defaultHelp(true)
                .description("This tool records the average end to end latency for a single message to travel through Kafka");

        parser
                .addArgument("--broker-list")
                .required(true)
                .action(store())
                .type(String.class)
                .dest("brokers")
                .help("The location of the bootstrap broker for both the producer and the consumer");

        parser
                .addArgument("--topic")
                .required(true)
                .action(store())
                .type(String.class)
                .dest("topic")
                .help("The Kakfa topic to send/receive messages to/from");

        parser
                .addArgument("--num-messages")
                .required(true)
                .action(store())
                .type(Integer.class)
                .dest("numMessages")
                .help("The number of messages to send");

        parser
                .addArgument("--producer-acks")
                .required(true)
                .action(store())
                .type(String.class)
                .dest("acks")
                .help("The number of messages to send");

        parser
                .addArgument("--message-size-bytes")
                .required(true)
                .action(store())
                .type(Integer.class)
                .dest("messageSize")
                .help("Size of each message in bytes");

        parser
                .addArgument("--properties-file")
                .required(false)
                .action(store())
                .type(String.class)
                .dest("propertiesFile");

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

        String brokers = res.getString("brokers");
        String topic = res.getString("topic");
        int numMessages = res.getInt("numMessages");
        String acks = res.getString("acks");
        int messageSizeBytes = res.getInt("messageSize");
        String propertiesFile = res.getString("propertiesFile");

        if (!("1".equals(acks) || "all".equals(acks))) {
            throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all");
        }

        Properties props;
        try {
            props = loadPropsWithBootstrapServers(propertiesFile, brokers);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Properties file not found");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(props);
             KafkaProducer<byte[], byte[]> producer = createKafkaProducer(props, acks)) {

            if (!consumer.listTopics().containsKey(topic)) {
                createTopic(props, topic);
            }

            List<TopicPartition> topicPartitions = consumer.
                    partitionsFor(topic).
                    stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toList());
            consumer.assign(topicPartitions);
            consumer.seekToEnd(topicPartitions);
            // TODO scala Function1 TopicPartition => Long.
            consumer.assignment().forEach(consumer::position);

            double totalTime = 0.0;
            long[] latencies = new long[numMessages];
            Random random = new Random(0);

            for (int i = 0; i < numMessages; i++) {
                byte[] message = randomBytesOfLen(random, messageSizeBytes);
                long begin = System.nanoTime();
                //Send message (of random bytes) synchronously then immediately poll for it
                producer.send(new ProducerRecord<>(topic, message)).get();
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeout));
                long elapsed = System.nanoTime() - begin;

                if (records.isEmpty()) {
                    consumer.commitSync();
                    throw new RuntimeException("poll() timed out before finding a result (timeout:[" + timeout + "])");
                }

                //Check result matches the original record
                String sent = new String(message, StandardCharsets.UTF_8);
                String read = new String(records.iterator().next().value(), StandardCharsets.UTF_8);

                if (!read.equals(sent)) {
                    consumer.commitSync();
                    throw new RuntimeException("The message read [" + read + "] did not match the message sent [" + sent + "]");
                }

                //Check we only got the one message
                if (records.iterator().hasNext()) {
                    int count = 1 + records.count();
                    throw new RuntimeException("Only one result was expected during this test. We found [" + count + "]");
                }

                //Report progress
                if (i % 1000 == 0)
                    System.out.println(i + "\t" + elapsed / 1000.0 / 1000.0);
                totalTime += elapsed;
                latencies[i] = elapsed / 1000 / 1000;
            }

            //Results
            System.out.printf("Avg latency: %.4f ms\n", totalTime / numMessages / 1000.0 / 1000.0);
            Arrays.sort(latencies);
            int p50 = (int) latencies[(int)(latencies.length * 0.5)];
            int p99 = (int) latencies[(int)(latencies.length * 0.99)];
            int p999 = (int) latencies[(int)(latencies.length * 0.999)];
            System.out.printf("Percentiles: 50th = %d, 99th = %d, 99.9th = %d", p50, p99, p999);
            consumer.commitSync();
        }

    }

    private static byte[] randomBytesOfLen(Random random, int length) {
        byte[] randomBytes = new byte[length];
        Arrays.fill(randomBytes, new Integer(random.nextInt(26) + 65).byteValue());
        return randomBytes;
    }

    private static void createTopic(Properties props, String topic) {

        short defaultReplicationFactor = 1;
        int defaultNumPartitions = 1;

        System.out.printf(("Topic \"%s\" does not exist. " +
                        "Will create topic with %d partition(s) and replication factor = %d%n"),
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

    private static Properties loadPropsWithBootstrapServers(String propertiesFile, String brokers) throws IOException {
        Properties properties = propertiesFile != null ? Utils.loadProps(propertiesFile) : new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return properties;
    }

    private static KafkaConsumer<byte[], byte[]> createKafkaConsumer(Properties properties) {
        Properties consumerProps = properties;
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); //ensure we have no temporal batching
        return new KafkaConsumer<>(consumerProps);
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(Properties properties, String acks) {
        Properties producerProps = properties;
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0"); //ensure writes are synchronous
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

}
