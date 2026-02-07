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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

import static joptsimple.util.RegexMatcher.regex;
import static org.apache.kafka.server.util.CommandLineUtils.parseKeyValueArgs;

public class ConsumerPerformance {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerPerformance.class);
    private static final Random RND = new Random();

    public static void main(String[] args) {
        run(args, KafkaConsumer::new);
    }

    static void run(String[] args, Function<Properties, Consumer<byte[], byte[]>> consumerCreator) {
        try {
            LOG.info("Starting consumer...");
            ConsumerPerfOptions options = new ConsumerPerfOptions(args);
            AtomicLong totalRecordsRead = new AtomicLong(0);
            AtomicLong totalBytesRead = new AtomicLong(0);
            AtomicLong joinTimeMs = new AtomicLong(0);
            AtomicLong joinTimeMsInSingleRound = new AtomicLong(0);

            if (!options.hideHeader())
                printHeader(options.showDetailedStats());

            try (Consumer<byte[], byte[]> consumer = consumerCreator.apply(options.props())) {
                long bytesRead = 0L;
                long recordsRead = 0L;
                long lastBytesRead = 0L;
                long lastRecordsRead = 0L;
                long currentTimeMs = System.currentTimeMillis();
                long joinStartMs = currentTimeMs;
                long startMs = currentTimeMs;
                consume(consumer, options, totalRecordsRead, totalBytesRead, joinTimeMs,
                    bytesRead, recordsRead, lastBytesRead, lastRecordsRead,
                    joinStartMs, joinTimeMsInSingleRound);
                long endMs = System.currentTimeMillis();

                // print final stats
                double elapsedSec = (endMs - startMs) / 1_000.0;
                long fetchTimeInMs = (endMs - startMs) - joinTimeMs.get();
                if (!options.showDetailedStats()) {
                    double totalMbRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
                    System.out.printf("%s, %s, %.4f, %.4f, %d, %.4f, %d, %d, %.4f, %.4f%n",
                        options.dateFormat().format(startMs),
                        options.dateFormat().format(endMs),
                        totalMbRead,
                        totalMbRead / elapsedSec,
                        totalRecordsRead.get(),
                        totalRecordsRead.get() / elapsedSec,
                        joinTimeMs.get(),
                        fetchTimeInMs,
                        totalMbRead / (fetchTimeInMs / 1000.0),
                        totalRecordsRead.get() / (fetchTimeInMs / 1000.0)
                    );
                }

                if (options.printMetrics()) {
                    ToolsUtils.printMetrics(consumer.metrics());
                }
            }
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    protected static void printHeader(boolean showDetailedStats) {
        String newFieldsInHeader = ", rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec";
        if (!showDetailedStats)
            System.out.printf("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec%s%n", newFieldsInHeader);
        else
            System.out.printf("time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec%s%n", newFieldsInHeader);
    }

    private static void consume(Consumer<byte[], byte[]> consumer,
                                ConsumerPerfOptions options,
                                AtomicLong totalRecordsRead,
                                AtomicLong totalBytesRead,
                                AtomicLong joinTimeMs,
                                long bytesRead,
                                long recordsRead,
                                long lastBytesRead,
                                long lastRecordsRead,
                                long joinStartMs,
                                AtomicLong joinTimeMsInSingleRound) {
        long numRecords = options.numRecords();
        long recordFetchTimeoutMs = options.recordFetchTimeoutMs();
        long reportingIntervalMs = options.reportingIntervalMs();
        boolean showDetailedStats = options.showDetailedStats();
        SimpleDateFormat dateFormat = options.dateFormat();

        ConsumerPerfRebListener listener = new ConsumerPerfRebListener(joinTimeMs, joinStartMs, joinTimeMsInSingleRound);
        if (options.topic().isPresent()) {
            consumer.subscribe(options.topic().get(), listener);
        } else {
            consumer.subscribe(options.include().get(), listener);
        }

        // now start the benchmark
        long currentTimeMs = System.currentTimeMillis();
        long lastReportTimeMs = currentTimeMs;
        long lastConsumedTimeMs = currentTimeMs;

        while (recordsRead < numRecords && currentTimeMs - lastConsumedTimeMs <= recordFetchTimeoutMs) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            currentTimeMs = System.currentTimeMillis();
            if (!records.isEmpty())
                lastConsumedTimeMs = currentTimeMs;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                recordsRead += 1;
                if (record.key() != null)
                    bytesRead += record.key().length;
                if (record.value() != null)
                    bytesRead += record.value().length;
                if (currentTimeMs - lastReportTimeMs >= reportingIntervalMs) {
                    if (showDetailedStats)
                        printConsumerProgress(0, bytesRead, lastBytesRead, recordsRead, lastRecordsRead,
                            lastReportTimeMs, currentTimeMs, dateFormat, joinTimeMsInSingleRound.get());
                    joinTimeMsInSingleRound.set(0);
                    lastReportTimeMs = currentTimeMs;
                    lastRecordsRead = recordsRead;
                    lastBytesRead = bytesRead;
                }
            }
        }

        if (recordsRead < numRecords)
            System.out.printf("WARNING: Exiting before consuming the expected number of records: timeout (%d ms) exceeded. " +
                "You can use the --timeout option to increase the timeout.%n", recordFetchTimeoutMs);
        totalRecordsRead.set(recordsRead);
        totalBytesRead.set(bytesRead);
    }

    protected static void printConsumerProgress(int id,
                                                long bytesRead,
                                                long lastBytesRead,
                                                long recordsRead,
                                                long lastRecordsRead,
                                                long startMs,
                                                long endMs,
                                                SimpleDateFormat dateFormat,
                                                long joinTimeMsInSingleRound) {
        printBasicProgress(id, bytesRead, lastBytesRead, recordsRead, lastRecordsRead, startMs, endMs, dateFormat);
        printExtendedProgress(bytesRead, lastBytesRead, recordsRead, lastRecordsRead, startMs, endMs, joinTimeMsInSingleRound);
        System.out.println();
    }

    private static void printBasicProgress(int id,
                                           long bytesRead,
                                           long lastBytesRead,
                                           long recordsRead,
                                           long lastRecordsRead,
                                           long startMs,
                                           long endMs,
                                           SimpleDateFormat dateFormat) {
        double elapsedMs = endMs - startMs;
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        double intervalMbPerSec = 1000.0 * intervalMbRead / elapsedMs;
        double intervalRecordsPerSec = ((recordsRead - lastRecordsRead) / elapsedMs) * 1000.0;
        System.out.printf("%s, %d, %.4f, %.4f, %d, %.4f", dateFormat.format(endMs), id,
            totalMbRead, intervalMbPerSec, recordsRead, intervalRecordsPerSec);
    }

    private static void printExtendedProgress(long bytesRead,
                                              long lastBytesRead,
                                              long recordsRead,
                                              long lastRecordsRead,
                                              long startMs,
                                              long endMs,
                                              long joinTimeMsInSingleRound) {
        long fetchTimeMs = endMs - startMs - joinTimeMsInSingleRound;
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        long intervalRecordsRead = recordsRead - lastRecordsRead;
        double intervalMbPerSec = (fetchTimeMs <= 0) ? 0.0 : 1000.0 * intervalMbRead / fetchTimeMs;
        double intervalRecordsPerSec = (fetchTimeMs <= 0) ? 0.0 : 1000.0 * intervalRecordsRead / fetchTimeMs;
        System.out.printf(", %d, %d, %.4f, %.4f", joinTimeMsInSingleRound,
            fetchTimeMs, intervalMbPerSec, intervalRecordsPerSec);
    }

    public static class ConsumerPerfRebListener implements ConsumerRebalanceListener {
        private final AtomicLong joinTimeMs;
        private final AtomicLong joinTimeMsInSingleRound;
        private final Collection<TopicPartition> assignedPartitions;
        private long joinStartMs;

        public ConsumerPerfRebListener(AtomicLong joinTimeMs, long joinStartMs, AtomicLong joinTimeMsInSingleRound) {
            this.joinTimeMs = joinTimeMs;
            this.joinStartMs = joinStartMs;
            this.joinTimeMsInSingleRound = joinTimeMsInSingleRound;
            this.assignedPartitions = new HashSet<>();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            assignedPartitions.removeAll(partitions);
            if (assignedPartitions.isEmpty()) {
                joinStartMs = System.currentTimeMillis();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (assignedPartitions.isEmpty()) {
                long elapsedMs = System.currentTimeMillis() - joinStartMs;
                joinTimeMs.addAndGet(elapsedMs);
                joinTimeMsInSingleRound.addAndGet(elapsedMs);
            }
            assignedPartitions.addAll(partitions);
        }
    }

    protected static class ConsumerPerfOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> includeOpt;
        private final OptionSpec<String> groupIdOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<String> commandPropertiesOpt;
        private final OptionSpec<Void> resetBeginningOffsetOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        @Deprecated(since = "4.2", forRemoval = true)
        private final OptionSpec<String> consumerConfigOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpec<Void> printMetricsOpt;
        private final OptionSpec<Void> showDetailedStatsOpt;
        private final OptionSpec<Long> recordFetchTimeoutOpt;
        @Deprecated(since = "4.2", forRemoval = true)
        private final OptionSpec<Long> numMessagesOpt;
        private final OptionSpec<Long> numRecordsOpt;
        private final OptionSpec<Long> reportingIntervalOpt;
        private final OptionSpec<String> dateFormatOpt;
        private final OptionSpec<Void> hideHeaderOpt;

        public ConsumerPerfOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            topicOpt = parser.accepts("topic", "The topic to consume from.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
            includeOpt = parser.accepts("include", "Regular expression specifying list of topics to include for consumption.")
                .withRequiredArg()
                .describedAs("Java regex (String)")
                .ofType(String.class);
            groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("perf-consumer-" + RND.nextInt(100_000))
                .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The maximum amount of data to fetch from a single partition per request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
            commandPropertiesOpt = parser.accepts("command-property", "Kafka consumer related configuration properties like client.id. " +
                    "These configs take precedence over those passed via --command-config or --consumer.config.")
                .withRequiredArg()
                .describedAs("prop1=val1")
                .ofType(String.class);
            resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
                "offset to consume from, start with the latest record present in the log rather than the earliest record.");
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(2 * 1024 * 1024);
            consumerConfigOpt = parser.accepts("consumer.config", "(DEPRECATED) Consumer config properties file. " +
                            "This option will be removed in a future version. Use --command-config instead.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Config properties file.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics.");
            showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
                "interval as configured by reporting-interval.");
            recordFetchTimeoutOpt = parser.accepts("timeout", "The maximum allowed time in milliseconds between returned records.")
                .withOptionalArg()
                .describedAs("milliseconds")
                .ofType(Long.class)
                .defaultsTo(10_000L);
            numMessagesOpt = parser.accepts("messages", "(DEPRECATED) The number of records to consume. " +
                            "This option will be removed in a future version. Use --num-records instead.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Long.class);
            numRecordsOpt = parser.accepts("num-records", "REQUIRED: The number of records to consume.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Long.class);
            reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in milliseconds at which to print progress info.")
                .withRequiredArg()
                .withValuesConvertedBy(regex("^\\d+$"))
                .describedAs("interval_ms")
                .ofType(Long.class)
                .defaultsTo(5_000L);
            dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
                    "See java.text.SimpleDateFormat for options.")
                .withRequiredArg()
                .describedAs("date format")
                .ofType(String.class)
                .defaultsTo("yyyy-MM-dd HH:mm:ss:SSS");
            hideHeaderOpt = parser.accepts("hide-header", "If set, skips printing the header for the stats.");
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
                return;
            }
            if (options != null) {
                CommandLineUtils.maybePrintHelpOrVersion(this, "This tool is used to verify the consumer performance.");
                CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
                CommandLineUtils.checkOneOfArgs(parser, options, topicOpt, includeOpt);

                CommandLineUtils.checkOneOfArgs(parser, options, numMessagesOpt, numRecordsOpt);
                CommandLineUtils.checkInvalidArgs(parser, options, consumerConfigOpt, commandConfigOpt);

                if (options.has(numMessagesOpt)) {
                    System.out.println("Warning: --messages is deprecated. Use --num-records instead.");
                }

                if (options.has(consumerConfigOpt)) {
                    System.out.println("Warning: --consumer.config is deprecated. Use --command-config instead.");
                }
            }
        }

        public boolean printMetrics() {
            return options.has(printMetricsOpt);
        }

        public String brokerHostsAndPorts() {
            return options.valueOf(bootstrapServerOpt);
        }

        private Properties readProps(List<String> commandProperties, String commandConfigFile) throws IOException {
            Properties props = commandConfigFile != null
                    ? Utils.loadProps(commandConfigFile)
                    : new Properties();
            props.putAll(parseKeyValueArgs(commandProperties));
            return props;
        }

        public Properties props() throws IOException {
            List<String> commandProperties = options.valuesOf(commandPropertiesOpt);
            String commandConfigFile;
            if (options.has(consumerConfigOpt)) {
                commandConfigFile = options.valueOf(consumerConfigOpt);
            } else {
                commandConfigFile = options.valueOf(commandConfigOpt);
            }
            Properties props = readProps(commandProperties, commandConfigFile);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsAndPorts());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt));
            props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString());
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                options.has(resetBeginningOffsetOpt) ? "latest" : "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
            if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, "perf-consumer-client");
            return props;
        }

        public Optional<Collection<String>> topic() {
            return options.has(topicOpt)
                    ? Optional.of(List.of(options.valueOf(topicOpt)))
                    : Optional.empty();
        }

        public Optional<Pattern> include() {
            return options.has(includeOpt)
                    ? Optional.of(Pattern.compile(options.valueOf(includeOpt)))
                    : Optional.empty();
        }

        public long numRecords() {
            return options.has(numMessagesOpt)
                    ? options.valueOf(numMessagesOpt)
                    : options.valueOf(numRecordsOpt);
        }

        public long reportingIntervalMs() {
            long value = options.valueOf(reportingIntervalOpt);
            if (value <= 0)
                throw new IllegalArgumentException("Reporting interval must be greater than 0.");
            return value;
        }

        public boolean showDetailedStats() {
            return options.has(showDetailedStatsOpt);
        }

        public SimpleDateFormat dateFormat() {
            return new SimpleDateFormat(options.valueOf(dateFormatOpt));
        }

        public boolean hideHeader() {
            return options.has(hideHeaderOpt);
        }

        public long recordFetchTimeoutMs() {
            return options.valueOf(recordFetchTimeoutOpt);
        }
    }
}
