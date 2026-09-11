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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

/**
 * This class records the average end to end latency for a single message to travel through Kafka.
 * Following are the required arguments
 * <p> --bootstrap-server = location of the bootstrap broker for both the producer and the consumer
 * <p> --topic = topic name used by both the producer and the consumer to send/receive messages
 * <p> --num-records = # messages to send
 * <p> --producer-acks = See ProducerConfig.ACKS_DOC
 * <p> --record-size = size of each message value in bytes
 *
 * <p> e.g. [./bin/kafka-e2e-latency.sh --bootstrap-server localhost:9092 --topic test-topic --num-records 1000 --producer-acks 1 --record-size 512]
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
    static void execute(String[] args) throws Exception {
        String[] processedArgs = convertLegacyArgsIfNeeded(args);
        EndToEndLatencyCommandOptions opts = new EndToEndLatencyCommandOptions(processedArgs);

        // required
        String brokers = opts.options.valueOf(opts.bootstrapServerOpt);
        String topic = opts.options.valueOf(opts.topicOpt);
        int numRecords = opts.options.valueOf(opts.numRecordsOpt);
        String acks = opts.options.valueOf(opts.acksOpt);
        int recordValueSize = opts.options.valueOf(opts.recordSizeOpt);

        // optional
        Optional<String> propertiesFile = Optional.ofNullable(opts.options.valueOf(opts.commandConfigOpt));
        int recordKeySize = opts.options.valueOf(opts.recordKeySizeOpt);
        int numHeaders = opts.options.valueOf(opts.numHeadersOpt);
        int headerKeySize = opts.options.valueOf(opts.recordHeaderKeySizeOpt);
        int headerValueSize = opts.options.valueOf(opts.recordHeaderValueSizeOpt);

        try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(propertiesFile, brokers);
             KafkaProducer<byte[], byte[]> producer = createKafkaProducer(propertiesFile, brokers, acks)) {

            if (!consumer.listTopics().containsKey(topic)) {
                createTopic(propertiesFile, brokers, topic);
            }
            setupConsumer(topic, consumer);
            double totalTime = 0.0;
            long[] latencies = new long[numRecords];
            Random random = new Random(0);

            for (int i = 0; i < numRecords; i++) {
                byte[] recordKey = randomBytesOfLen(random, recordKeySize);
                byte[] recordValue = randomBytesOfLen(random, recordValueSize);
                List<Header> headers = generateHeadersWithSeparateSizes(random, numHeaders, headerKeySize, headerValueSize);

                long begin = System.nanoTime();
                //Send message (of random bytes) synchronously then immediately poll for it
                producer.send(new ProducerRecord<>(topic, null, recordKey, recordValue, headers)).get();
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                long elapsed = System.nanoTime() - begin;

                validate(consumer, recordValue, records, recordKey, headers);

                //Report progress
                if (i % 1000 == 0)
                    System.out.println(i + "\t" + elapsed / 1000.0 / 1000.0);
                totalTime += elapsed;
                latencies[i] = elapsed / 1000 / 1000;
            }

            printResults(numRecords, totalTime, latencies);
            consumer.commitSync();
        }
    }

    // Visible for testing
    static void validate(KafkaConsumer<byte[], byte[]> consumer, byte[] sentRecordValue, ConsumerRecords<byte[], byte[]> records, byte[] sentRecordKey, Iterable<Header> sentHeaders) {
        if (records.isEmpty()) {
            commitAndThrow(consumer, "poll() timed out before finding a result (timeout:[" + POLL_TIMEOUT_MS + "ms])");
        }

        ConsumerRecord<byte[], byte[]> record = records.iterator().next();
        String sent = new String(sentRecordValue, StandardCharsets.UTF_8);
        String read = new String(record.value(), StandardCharsets.UTF_8);

        if (!read.equals(sent)) {
            commitAndThrow(consumer, "The message value read [" + read + "] did not match the message value sent [" + sent + "]");
        }

        if (sentRecordKey != null) {
            if (record.key() == null) {
                commitAndThrow(consumer, "Expected message key but received null");
            }
            String sentKey = new String(sentRecordKey, StandardCharsets.UTF_8);
            String readKey = new String(record.key(), StandardCharsets.UTF_8);
            if (!readKey.equals(sentKey)) {
                commitAndThrow(consumer, "The message key read [" + readKey + "] did not match the message key sent [" + sentKey + "]");
            }
        } else if (record.key() != null) {
            commitAndThrow(consumer, "Expected null message key but received [" + new String(record.key(), StandardCharsets.UTF_8) + "]");
        }

        validateHeaders(consumer, sentHeaders, record);

        //Check we only got the one message
        if (records.count() != 1) {
            int count = records.count();
            commitAndThrow(consumer, "Only one result was expected during this test. We found [" + count + "]");
        }
    }

    private static void commitAndThrow(KafkaConsumer<byte[], byte[]> consumer, String message) {
        consumer.commitSync();
        throw new RuntimeException(message);
    }

    private static void validateHeaders(KafkaConsumer<byte[], byte[]> consumer, Iterable<Header> sentHeaders, ConsumerRecord<byte[], byte[]> record) {
        if (sentHeaders != null && sentHeaders.iterator().hasNext()) {
            if (!record.headers().iterator().hasNext()) {
                commitAndThrow(consumer, "Expected message headers but received none");
            }
            
            Iterator<Header> sentIterator = sentHeaders.iterator();
            Iterator<Header> receivedIterator = record.headers().iterator();
            
            while (sentIterator.hasNext() && receivedIterator.hasNext()) {
                Header sentHeader = sentIterator.next();
                Header receivedHeader = receivedIterator.next();
                if (!receivedHeader.key().equals(sentHeader.key()) || !Arrays.equals(receivedHeader.value(), sentHeader.value())) {
                    String receivedValueStr = receivedHeader.value() == null ? "null" : Arrays.toString(receivedHeader.value());
                    String sentValueStr = sentHeader.value() == null ? "null" : Arrays.toString(sentHeader.value());
                    commitAndThrow(consumer, "The message header read [" + receivedHeader.key() + ":" + receivedValueStr +
                            "] did not match the message header sent [" + sentHeader.key() + ":" + sentValueStr + "]");
                }
            }
            
            if (sentIterator.hasNext() || receivedIterator.hasNext()) {
                commitAndThrow(consumer, "Header count mismatch between sent and received messages");
            }
        }
    }

    private static List<Header> generateHeadersWithSeparateSizes(Random random, int numHeaders, int keySize, int valueSize) {
        List<Header> headers = new ArrayList<>();

        for (int i = 0; i < numHeaders; i++) {
            String headerKey = new String(randomBytesOfLen(random, keySize), StandardCharsets.UTF_8);
            byte[] headerValue = valueSize == -1 ? null : randomBytesOfLen(random, valueSize);
            headers.add(new Header() {
                @Override
                public String key() {
                    return headerKey;
                }

                @Override
                public byte[] value() {
                    return headerValue;
                }
            });
        }
        return headers;
    }

    private static void setupConsumer(String topic, KafkaConsumer<byte[], byte[]> consumer) {
        List<TopicPartition> topicPartitions = consumer
                .partitionsFor(topic)
                .stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        consumer.assignment().forEach(consumer::position);
    }

    private static void printResults(int numRecords, double totalTime, long[] latencies) {
        System.out.printf("Avg latency: %.4f ms%n", totalTime / numRecords / 1000.0 / 1000.0);
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
            adminClient.createTopics(Set.of(newTopic)).all().get();
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

    /**
     * Converts legacy positional arguments to named arguments for backward compatibility.
     *
     * @param args the command line arguments to convert
     * @return converted named arguments
     * @throws Exception if the legacy arguments are invalid
     * @deprecated Positional argument usage is deprecated and will be removed in Apache Kafka 5.0.
     *             Use named arguments instead: --bootstrap-server, --topic, --num-records, --producer-acks, --record-size, --command-config
     */
    @Deprecated(since = "4.2", forRemoval = true)
    static String[] convertLegacyArgsIfNeeded(String[] args) throws Exception {
        if (args.length == 0) {
            return args;
        }

        boolean hasRequiredNamedArgs = Arrays.stream(args).anyMatch(arg -> 
            arg.equals("--bootstrap-server") || 
            arg.equals("--topic") || 
            arg.equals("--num-records") || 
            arg.equals("--producer-acks") || 
            arg.equals("--record-size"));
        if (hasRequiredNamedArgs) {
            return args;
        }

        if (args.length != 5 && args.length != 6) {
            throw new TerseException("Invalid number of arguments. Expected 5 or 6 positional arguments, but got " + args.length + ". " +
                    "Usage: bootstrap-server topic num-records producer-acks record-size [optional] command-config");
        }

        return convertLegacyArgs(args);
    }

    private static String[] convertLegacyArgs(String[] legacyArgs) {
        List<String> newArgs = new ArrayList<>();

        // broker_list -> --bootstrap-server
        newArgs.add("--bootstrap-server");
        newArgs.add(legacyArgs[0]);

        // topic -> --topic
        newArgs.add("--topic");
        newArgs.add(legacyArgs[1]);

        // num_messages -> --num-records
        newArgs.add("--num-records");
        newArgs.add(legacyArgs[2]);

        // producer_acks -> --producer-acks
        newArgs.add("--producer-acks");
        newArgs.add(legacyArgs[3]);

        // message_size_bytes -> --record-size
        newArgs.add("--record-size");
        newArgs.add(legacyArgs[4]);

        // properties_file -> --command-config
        if (legacyArgs.length == 6) {
            newArgs.add("--command-config");
            newArgs.add(legacyArgs[5]);
        }
        System.out.println("WARNING: Positional argument usage is deprecated and will be removed in Apache Kafka 5.0. " +
                "Please use named arguments instead: --bootstrap-server, --topic, --num-records, --producer-acks, --record-size, --command-config");
        return newArgs.toArray(new String[0]);
    }

    public static final class EndToEndLatencyCommandOptions extends CommandDefaultOptions {
        final OptionSpec<String> bootstrapServerOpt;
        final OptionSpec<String> topicOpt;
        final OptionSpec<Integer> numRecordsOpt;
        final OptionSpec<String> acksOpt;
        final OptionSpec<Integer> recordSizeOpt;
        final OptionSpec<String> commandConfigOpt;
        final OptionSpec<Integer> recordKeySizeOpt;
        final OptionSpec<Integer> recordHeaderValueSizeOpt;
        final OptionSpec<Integer> recordHeaderKeySizeOpt;
        final OptionSpec<Integer> numHeadersOpt;

        public EndToEndLatencyCommandOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                    .withRequiredArg()
                    .describedAs("bootstrap-server")
                    .ofType(String.class);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic to use for the test.")
                    .withRequiredArg()
                    .describedAs("topic-name")
                    .ofType(String.class);
            numRecordsOpt = parser.accepts("num-records", "REQUIRED: The number of messages to send.")
                    .withRequiredArg()
                    .describedAs("count")
                    .ofType(Integer.class);
            acksOpt = parser.accepts("producer-acks", "REQUIRED: Producer acknowledgements. Must be '1' or 'all'.")
                    .withRequiredArg()
                    .describedAs("producer-acks")
                    .ofType(String.class);
            recordSizeOpt = parser.accepts("record-size", "REQUIRED: The size of each message payload in bytes.")
                    .withRequiredArg()
                    .describedAs("bytes")
                    .ofType(Integer.class);
            recordKeySizeOpt = parser.accepts("record-key-size", "Optional: The size of the message key in bytes. If not set, messages are sent without a key.")
                    .withOptionalArg()
                    .describedAs("bytes")
                    .ofType(Integer.class)
                    .defaultsTo(0);
            recordHeaderKeySizeOpt = parser.accepts("record-header-key-size", "Optional: The size of the message header key in bytes. Used together with record-header-size.")
                    .withOptionalArg()
                    .describedAs("bytes")
                    .ofType(Integer.class)
                    .defaultsTo(0);
            recordHeaderValueSizeOpt = parser.accepts("record-header-size", "Optional: The size of message header value in bytes. Use -1 for null header value.")
                    .withOptionalArg()
                    .describedAs("bytes")
                    .ofType(Integer.class)
                    .defaultsTo(0);
            numHeadersOpt = parser.accepts("num-headers", "Optional: The number of headers to include in each message.")
                    .withOptionalArg()
                    .describedAs("count")
                    .ofType(Integer.class)
                    .defaultsTo(0);
            commandConfigOpt = parser.accepts("command-config", "Optional: A property file for Kafka producer/consumer/admin client configuration.")
                    .withOptionalArg()
                    .describedAs("config-file")
                    .ofType(String.class);

            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }
            checkArgs();
        }

        void checkArgs() {
            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool measures end-to-end latency in Kafka by sending messages and timing their reception.");

            // check required arguments
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, topicOpt, numRecordsOpt, acksOpt, recordSizeOpt);

            // validate 'producer-acks'
            String acksValue = options.valueOf(acksOpt);
            if (!List.of("1", "all").contains(acksValue)) {
                CommandLineUtils.printUsageAndExit(parser, "Invalid value for --producer-acks. Latency testing requires synchronous acknowledgement. Please use '1' or 'all'.");
            }

            // validate for num-records and record-size
            if (options.valueOf(numRecordsOpt) <= 0) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --num-records must be a positive integer.");
            }
            if (options.valueOf(recordSizeOpt) < 0) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --record-size must be a non-negative integer.");
            }

            if (options.valueOf(recordKeySizeOpt) < 0) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --record-key-size must be a non-negative integer.");
            }
            if (options.valueOf(recordHeaderKeySizeOpt) < 0) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --record-header-key-size must be a non-negative integer.");
            }
            if (options.valueOf(recordHeaderValueSizeOpt) < -1) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --record-header-size must be a non-negative integer or -1 for null header value.");
            }
            if (options.valueOf(numHeadersOpt) < 0) {
                CommandLineUtils.printUsageAndExit(parser, "Value for --num-headers must be a non-negative integer.");
            }
        }
    }
}
