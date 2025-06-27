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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;


/**
 * This is a torture test that runs against an existing broker
 * <p>
 * Here is how it works:
 * <p>
 * It produces a series of specially formatted messages to one or more partitions. Each message it produces
 * it logs out to a text file. The messages have a limited set of keys, so there is duplication in the key space.
 * <p>
 * The broker will clean its log as the test runs.
 * <p>
 * When the specified number of messages have been produced we create a consumer and consume all the messages in the topic
 * and write that out to another text file.
 * <p>
 * Using a stable unix sort we sort both the producer log of what was sent and the consumer log of what was retrieved by the message key.
 * Then we compare the final message in both logs for each key. If this final message is not the same for all keys we
 * print an error and exit with exit code 1, otherwise we print the size reduction and exit with exit code 0.
 */
public class LogCompactionTester {
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser(false);
        LogCompactionTesterOptions options = new LogCompactionTesterOptions(parser);

        OptionSet optionSet = parser.parse(args);
        if (args.length == 0) {
            CommandLineUtils.printUsageAndExit(parser,
                    "A tool to test log compaction. Valid options are: ");
        }

        CommandLineUtils.checkRequiredArgs(parser, optionSet, options.brokerOpt, options.numMessagesOpt);

        long messages = optionSet.valueOf(options.numMessagesOpt);
        String compressionType = optionSet.valueOf(options.messageCompressionOpt);
        int percentDeletes = optionSet.valueOf(options.percentDeletesOpt);
        int dups = optionSet.valueOf(options.numDupsOpt);
        String brokerUrl = optionSet.valueOf(options.brokerOpt);
        int topicCount = optionSet.valueOf(options.topicsOpt);
        int sleepSecs = optionSet.valueOf(options.sleepSecsOpt);

        long testId = RANDOM.nextLong();
        String[] topics = IntStream.range(0, topicCount)
                .mapToObj(i -> "log-cleaner-test-" + testId + "-" + i)
                .toArray(String[]::new);
        createTopics(brokerUrl, topics);

        System.out.println("Producing " + messages + " messages..to topics " + String.join(",", topics));
        Path producedDataFilePath = produceMessages(
                brokerUrl, topics, messages,
                compressionType, dups, percentDeletes);
        System.out.println("Sleeping for " + sleepSecs + "seconds...");
        Thread.sleep(sleepSecs * 1000L);
        System.out.println("Consuming messages...");
        Path consumedDataFilePath = consumeMessages(brokerUrl, topics);

        long producedLines = lineCount(producedDataFilePath);
        long consumedLines = lineCount(consumedDataFilePath);
        double reduction = 100 * (1.0 - (double) consumedLines / producedLines);

        System.out.printf(
            "%d rows of data produced, %d rows of data consumed (%.1f%% reduction).%n",
            producedLines, consumedLines, reduction);

        System.out.println("De-duplicating and validating output files...");
        validateOutput(producedDataFilePath.toFile(), consumedDataFilePath.toFile());

        Files.deleteIfExists(producedDataFilePath);
        Files.deleteIfExists(consumedDataFilePath);
        // if you change this line, we need to update test_log_compaction_tool.py system test
        System.out.println("Data verification is completed");
    }


    private static void createTopics(String brokerUrl, String[] topics) throws Exception {
        Properties adminConfig = new Properties();
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

        try (Admin adminClient = Admin.create(adminConfig)) {
            Map<String, String> topicConfigs = Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
            );
            List<NewTopic> newTopics = Arrays.stream(topics)
                    .map(name -> new NewTopic(name, 1, (short) 1).configs(topicConfigs)).toList();
            adminClient.createTopics(newTopics).all().get();

            final List<String> pendingTopics = new ArrayList<>();
            waitUntilTrue(() -> {
                try {
                    Set<String> allTopics = adminClient.listTopics().names().get();
                    pendingTopics.clear();
                    pendingTopics.addAll(
                            Arrays.stream(topics)
                                    .filter(topicName -> !allTopics.contains(topicName))
                                    .toList()
                    );
                    return pendingTopics.isEmpty();
                } catch (InterruptedException | java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }, () -> "timed out waiting for topics: " + pendingTopics);
        }
    }

    private static void validateOutput(File producedDataFile, File consumedDataFile) {
        try (BufferedReader producedReader = externalSort(producedDataFile);
             BufferedReader consumedReader = externalSort(consumedDataFile)) {
            Iterator<TestRecord> produced = TestRecordUtils.valuesIterator(producedReader);
            Iterator<TestRecord> consumed = TestRecordUtils.valuesIterator(consumedReader);

            File producedDedupedFile = new File(producedDataFile.getAbsolutePath() + ".deduped");
            File consumedDedupedFile = new File(consumedDataFile.getAbsolutePath() + ".deduped");

            try (BufferedWriter producedDeduped = Files.newBufferedWriter(
                    producedDedupedFile.toPath(), StandardCharsets.UTF_8);
                 BufferedWriter consumedDeduped = Files.newBufferedWriter(
                         consumedDedupedFile.toPath(), StandardCharsets.UTF_8)) {
                int total = 0;
                int mismatched = 0;
                while (produced.hasNext() && consumed.hasNext()) {
                    TestRecord p = produced.next();
                    producedDeduped.write(p.toString());
                    producedDeduped.newLine();

                    TestRecord c = consumed.next();
                    consumedDeduped.write(c.toString());
                    consumedDeduped.newLine();

                    if (!p.equals(c)) {
                        mismatched++;
                    }
                    total++;
                }

                System.out.printf("Validated %d values, %d mismatches.%n", total, mismatched);
                require(!produced.hasNext(), "Additional values produced not found in consumer log.");
                require(!consumed.hasNext(), "Additional values consumed not found in producer log.");
                require(mismatched == 0, "Non-zero number of row mismatches.");
                // if all the checks worked out we can delete the deduped files
                Files.deleteIfExists(producedDedupedFile.toPath());
                Files.deleteIfExists(consumedDedupedFile.toPath());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static BufferedReader externalSort(File file) throws IOException {
        Path tempDir = Files.createTempDirectory("log_compaction_test");

        ProcessBuilder builder = new ProcessBuilder(
                "sort", "--key=1,2", "--stable", "--buffer-size=20%",
                "--temporary-directory=" + tempDir.toString(), file.getAbsolutePath());
        Process process = builder.start();

        // async read from the process's stderr to prevent blocking if the buffer fills up
        CompletableFuture.runAsync(() -> {
            try (BufferedReader errReader = new BufferedReader(
                    new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = errReader.readLine()) != null) {
                    System.err.println("[sort stderr] " + line);
                }
            } catch (IOException e) {
                System.err.println("Failed to read sort stderr: " + e.getMessage());
            }
        });

        // async wait for the process to complete and log a message if it exits abnormally
        CompletableFuture.runAsync(() -> {
            try {
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    System.err.println("Sort process exited abnormally with code " + exitCode + ".");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        return new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8),
                10 * 1024 * 1024
        );
    }

    private static long lineCount(Path filePath) throws IOException {
        try (Stream<String> lines = Files.lines(filePath)) {
            return lines.count();
        }
    }

    private static void require(boolean requirement, String message) {
        if (!requirement) {
            System.err.println("Data validation failed : " + message);
            Exit.exit(1);
        }
    }

    private static Path produceMessages(String brokerUrl, String[] topics, long messages,
                                        String compressionType, int dups, int percentDeletes) {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                producerProps, new ByteArraySerializer(), new ByteArraySerializer())) {
            int keyCount = (int) (messages / dups);
            Path producedFilePath = Files.createTempFile("kafka-log-cleaner-produced-", ".txt");
            System.out.println("Logging produce requests to " + producedFilePath);

            try (BufferedWriter producedWriter = Files.newBufferedWriter(
                    producedFilePath, StandardCharsets.UTF_8)) {
                for (long i = 0; i < messages * topics.length; i++) {
                    String topic = topics[(int) (i % topics.length)];
                    int key = RANDOM.nextInt(keyCount);
                    boolean delete = (i % 100) < percentDeletes;
                    ProducerRecord<byte[], byte[]> record;
                    if (delete) {
                        record = new ProducerRecord<>(topic,
                                String.valueOf(key).getBytes(StandardCharsets.UTF_8), null);
                    } else {
                        record = new ProducerRecord<>(topic,
                                String.valueOf(key).getBytes(StandardCharsets.UTF_8),
                                String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                    }
                    producer.send(record);
                    producedWriter.write(new TestRecord(topic, key, i, delete).toString());
                    producedWriter.newLine();
                }
            }
            return producedFilePath;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static Path consumeMessages(String brokerUrl, String[] topics) throws IOException {
        Consumer<String, String> consumer = createConsumer(brokerUrl);
        consumer.subscribe(Arrays.asList(topics));
        Path consumedFilePath = Files.createTempFile("kafka-log-cleaner-consumed-", ".txt");
        System.out.println("Logging consumed messages to " + consumedFilePath);

        try (BufferedWriter consumedWriter = Files.newBufferedWriter(
                consumedFilePath, StandardCharsets.UTF_8)) {
            boolean done = false;
            while (!done) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(20));
                if (!consumerRecords.isEmpty()) {
                    consumerRecords.forEach(
                        record -> {
                            try {
                                boolean delete = record.value() == null;
                                long value = delete ? -1L : Long.parseLong(record.value());
                                TestRecord testRecord = new TestRecord(
                                        record.topic(), Integer.parseInt(record.key()), value, delete);
                                consumedWriter.write(testRecord.toString());
                                consumedWriter.newLine();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    );
                } else {
                    done = true;
                }

            }
        } finally {
            consumer.close();
        }
        return consumedFilePath;
    }

    private static Consumer<String, String> createConsumer(String brokerUrl) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "log-cleaner-test-" + RANDOM.nextInt(Integer.MAX_VALUE));
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
    }

    /**
     * Wait for condition to be true for at most 15 seconds, checking every 100ms
     */
    private static void waitUntilTrue(Supplier<Boolean> condition, Supplier<String> timeoutMessage) throws InterruptedException {
        final long defaultMaxWaitMs = 15000; // 15 seconds
        final long defaultPollIntervalMs = 100; // 100ms
        long endTime = System.currentTimeMillis() + defaultMaxWaitMs;

        while (System.currentTimeMillis() < endTime) {
            try {
                if (condition.get()) {
                    return;
                }
            } catch (Exception e) {
                // Continue trying until timeout
            }
            Thread.sleep(Math.min(defaultPollIntervalMs, defaultMaxWaitMs));
        }

        throw new RuntimeException(timeoutMessage.get());
    }
}
