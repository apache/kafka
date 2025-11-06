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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.stream.Collectors.toCollection;


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
 * When the specified number of messages has been produced we create a consumer and consume all the messages in the topic
 * and write that out to another text file.
 * <p>
 * Using a stable unix sort we sort both the producer log of what was sent and the consumer log of what was retrieved by the message key.
 * Then we compare the final message in both logs for each key. If this final message is not the same for all keys we
 * print an error and exit with exit code 1, otherwise we print the size reduction and exit with exit code 0.
 */
public class LogCompactionTester {

    public static class Options {
        public final OptionSpec<Long> numMessagesOpt;
        public final OptionSpec<String>  messageCompressionOpt;
        public final OptionSpec<Integer> compressionLevelOpt;
        public final OptionSpec<Integer> numDupsOpt;
        public final OptionSpec<String>  brokerOpt;
        public final OptionSpec<Integer> topicsOpt;
        public final OptionSpec<Integer> percentDeletesOpt;
        public final OptionSpec<Integer> sleepSecsOpt;
        public final OptionSpec<Void>    helpOpt;

        public Options(OptionParser parser) {
            numMessagesOpt = parser
                    .accepts("messages", "The number of messages to send or consume.")
                    .withRequiredArg()
                    .describedAs("count")
                    .ofType(Long.class)
                    .defaultsTo(Long.MAX_VALUE);

            messageCompressionOpt = parser
                    .accepts("compression-type", "message compression type")
                    .withOptionalArg()
                    .describedAs("compressionType")
                    .ofType(String.class)
                    .defaultsTo("none");

            compressionLevelOpt = parser
                    .accepts("compression-level", "The compression level to use with the specified compression type.")
                    .withOptionalArg()
                    .describedAs("level")
                    .ofType(Integer.class);

            numDupsOpt = parser
                    .accepts("duplicates", "The number of duplicates for each key.")
                    .withRequiredArg()
                    .describedAs("count")
                    .ofType(Integer.class)
                    .defaultsTo(5);

            brokerOpt = parser
                    .accepts("bootstrap-server", "The server(s) to connect to.")
                    .withRequiredArg()
                    .describedAs("url")
                    .ofType(String.class);

            topicsOpt = parser
                    .accepts("topics", "The number of topics to test.")
                    .withRequiredArg()
                    .describedAs("count")
                    .ofType(Integer.class)
                    .defaultsTo(1);

            percentDeletesOpt = parser
                    .accepts("percent-deletes", "The percentage of updates that are deletes.")
                    .withRequiredArg()
                    .describedAs("percent")
                    .ofType(Integer.class)
                    .defaultsTo(0);

            sleepSecsOpt = parser
                    .accepts("sleep", "Time in milliseconds to sleep between production and consumption.")
                    .withRequiredArg()
                    .describedAs("ms")
                    .ofType(Integer.class)
                    .defaultsTo(0);

            helpOpt = parser
                    .acceptsAll(List.of("h", "help"), "Display help information");
        }
    }

    public record TestRecord(String topic, int key, long value, boolean delete) {
        @Override
        public String toString() {
            return topic + "\t" + key + "\t" + value + "\t" + (delete ? "d" : "u");
        }

        public String getTopicAndKey() {
            return topic + key;
        }

        public static TestRecord parse(String line) {
            String[] components = line.split("\t");
            if (components.length != 4) {
                throw new IllegalArgumentException("Invalid TestRecord format: " + line);
            }

            return new TestRecord(
                    components[0],
                    Integer.parseInt(components[1]),
                    Long.parseLong(components[2]),
                    "d".equals(components[3])
            );
        }
    }

    public static class TestRecordUtils {
        // maximum line size while reading produced/consumed record text file
        private static final int READ_AHEAD_LIMIT = 4906;

        public static TestRecord readNext(BufferedReader reader) throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            TestRecord curr = TestRecord.parse(line);
            while (true) {
                String peekedLine = peekLine(reader);
                if (peekedLine == null) {
                    return curr;
                }
                TestRecord next = TestRecord.parse(peekedLine);
                if (!next.getTopicAndKey().equals(curr.getTopicAndKey())) {
                    return curr;
                }
                curr = next;
                reader.readLine();
            }
        }

        public static Iterator<TestRecord> valuesIterator(BufferedReader reader) {
            return Spliterators.iterator(new Spliterators.AbstractSpliterator<>(
                    Long.MAX_VALUE, Spliterator.ORDERED) {
                @Override
                public boolean tryAdvance(java.util.function.Consumer<? super TestRecord> action) {
                    try {
                        TestRecord rec;
                        do {
                            rec = readNext(reader);
                        } while (rec != null && rec.delete);
                        if (rec == null) return false;
                        action.accept(rec);
                        return true;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }

        public static String peekLine(BufferedReader reader) throws IOException {
            reader.mark(READ_AHEAD_LIMIT);
            String line = reader.readLine();
            reader.reset();
            return line;
        }
    }

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser(false);
        Options options = new Options(parser);

        OptionSet optionSet = parser.parse(args);
        if (args.length == 0) {
            CommandLineUtils.printUsageAndExit(parser,
                    "A tool to test log compaction. Valid options are: ");
        }

        CommandLineUtils.checkRequiredArgs(parser, optionSet, options.brokerOpt, options.numMessagesOpt);

        long messages = optionSet.valueOf(options.numMessagesOpt);
        CompressionType compressionType = CompressionType.forName(optionSet.valueOf(options.messageCompressionOpt));
        Integer compressionLevel = optionSet.valueOf(options.compressionLevelOpt);
        int percentDeletes = optionSet.valueOf(options.percentDeletesOpt);
        int dups = optionSet.valueOf(options.numDupsOpt);
        String brokerUrl = optionSet.valueOf(options.brokerOpt);
        int topicCount = optionSet.valueOf(options.topicsOpt);
        int sleepSecs = optionSet.valueOf(options.sleepSecsOpt);

        long testId = RANDOM.nextLong();
        Set<String> topics = IntStream.range(0, topicCount)
                .mapToObj(i -> "log-cleaner-test-" + testId + "-" + i)
                .collect(toCollection(LinkedHashSet::new));
        createTopics(brokerUrl, topics);

        System.out.println("Producing " + messages + " messages..to topics " + String.join(",", topics));
        Path producedDataFilePath = produceMessages(
                brokerUrl, topics, messages,
                compressionType, compressionLevel,
                dups, percentDeletes);
        System.out.println("Sleeping for " + sleepSecs + "seconds...");
        TimeUnit.MILLISECONDS.sleep(sleepSecs * 1000L);
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


    private static void createTopics(String brokerUrl, Set<String> topics) throws Exception {
        Properties adminConfig = new Properties();
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

        try (Admin adminClient = Admin.create(adminConfig)) {
            Map<String, String> topicConfigs = Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
            );
            List<NewTopic> newTopics = topics.stream()
                    .map(name -> new NewTopic(name, 1, (short) 1).configs(topicConfigs)).toList();
            adminClient.createTopics(newTopics).all().get();

            final List<String> pendingTopics = new ArrayList<>();
            waitUntilTrue(() -> {
                try {
                    Set<String> allTopics = adminClient.listTopics().names().get();
                    pendingTopics.clear();
                    pendingTopics.addAll(
                            topics.stream()
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
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            // clean up temp directory if process fails to start
            try {
                Files.deleteIfExists(tempDir);
            } catch (IOException cleanupException) {
                e.addSuppressed(cleanupException);
            }
            throw new IOException("Failed to start sort process. Ensure 'sort' command is available.", e);
        }

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

    private static Path produceMessages(String brokerUrl, Set<String> topics, long messages,
                                        CompressionType compressionType, Integer compressionLevel,
                                        int dups, int percentDeletes) throws IOException {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType.name);
        
        if (compressionLevel != null) {
            switch (compressionType) {
                case GZIP -> producerProps.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, compressionLevel);
                case LZ4 -> producerProps.put(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, compressionLevel);
                case ZSTD -> producerProps.put(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, compressionLevel);
                default -> System.out.println("Warning: Compression level " + compressionLevel + " is ignored for compression type "
                    + compressionType.name + ". Only gzip, lz4, and zstd support compression levels.");
            }
        }

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                producerProps, new ByteArraySerializer(), new ByteArraySerializer())) {
            int keyCount = (int) (messages / dups);
            Path producedFilePath = Files.createTempFile("kafka-log-cleaner-produced-", ".txt");
            System.out.println("Logging produce requests to " + producedFilePath);

            try (BufferedWriter producedWriter = Files.newBufferedWriter(
                    producedFilePath, StandardCharsets.UTF_8)) {
                List<String> topicsList = List.copyOf(topics);
                int size = topicsList.size();
                for (long i = 0; i < messages * size; i++) {
                    String topic = topicsList.get((int) (i % size));
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
        }
    }

    private static Path consumeMessages(String brokerUrl, Set<String> topics) throws IOException {

        Path consumedFilePath = Files.createTempFile("kafka-log-cleaner-consumed-", ".txt");
        System.out.println("Logging consumed messages to " + consumedFilePath);

        try (Consumer<String, String> consumer = createConsumer(brokerUrl);
             BufferedWriter consumedWriter = Files.newBufferedWriter(consumedFilePath, StandardCharsets.UTF_8)) {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(20));
                if (consumerRecords.isEmpty()) return consumedFilePath;
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
            }
        }
    }

    private static Consumer<String, String> createConsumer(String brokerUrl) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "log-cleaner-test-" + RANDOM.nextInt(Integer.MAX_VALUE),
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
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
            TimeUnit.MILLISECONDS.sleep(defaultPollIntervalMs);
        }

        throw new RuntimeException(timeoutMessage.get());
    }
}
