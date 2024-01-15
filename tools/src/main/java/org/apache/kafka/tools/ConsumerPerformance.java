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

import joptsimple.OptionException;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static joptsimple.util.RegexMatcher.regex;

public class ConsumerPerformance {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerPerformance.class);
    private static final Random RND = new Random();

    public static void main(String[] args) {
        try {
            LOG.info("Starting consumer...");
            ConsumerPerfOptions options = new ConsumerPerfOptions(args);
            AtomicLong totalMessagesRead = new AtomicLong(0);
            AtomicLong totalBytesRead = new AtomicLong(0);
            AtomicLong joinTimeMs = new AtomicLong(0);
            AtomicLong joinTimeMsInSingleRound = new AtomicLong(0);

            if (!options.hideHeader())
                printHeader(options.showDetailedStats());

            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(options.props());
            long bytesRead = 0L;
            long messagesRead = 0L;
            long lastBytesRead = 0L;
            long lastMessagesRead = 0L;
            long currentTimeMs = System.currentTimeMillis();
            long joinStartMs = currentTimeMs;
            long startMs = currentTimeMs;
            consume(consumer, options, totalMessagesRead, totalBytesRead, joinTimeMs,
                bytesRead, messagesRead, lastBytesRead, lastMessagesRead,
                joinStartMs, joinTimeMsInSingleRound);
            long endMs = System.currentTimeMillis();

            Map<MetricName, ? extends Metric> metrics = null;
            if (options.printMetrics())
                metrics = consumer.metrics();
            consumer.close();

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
                    totalMessagesRead.get(),
                    totalMessagesRead.get() / elapsedSec,
                    joinTimeMs.get(),
                    fetchTimeInMs,
                    totalMbRead / (fetchTimeInMs / 1000.0),
                    totalMessagesRead.get() / (fetchTimeInMs / 1000.0)
                );
            }

            if (metrics != null)
                ToolsUtils.printMetrics(metrics);
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

    private static void consume(KafkaConsumer<byte[], byte[]> consumer,
                                ConsumerPerfOptions options,
                                AtomicLong totalMessagesRead,
                                AtomicLong totalBytesRead,
                                AtomicLong joinTimeMs,
                                long bytesRead,
                                long messagesRead,
                                long lastBytesRead,
                                long lastMessagesRead,
                                long joinStartMs,
                                AtomicLong joinTimeMsInSingleRound) {
        long numMessages = options.numMessages();
        long recordFetchTimeoutMs = options.recordFetchTimeoutMs();
        long reportingIntervalMs = options.reportingIntervalMs();
        boolean showDetailedStats = options.showDetailedStats();
        SimpleDateFormat dateFormat = options.dateFormat();
        consumer.subscribe(options.topic(),
            new ConsumerPerfRebListener(joinTimeMs, joinStartMs, joinTimeMsInSingleRound));

        // now start the benchmark
        long currentTimeMs = System.currentTimeMillis();
        long lastReportTimeMs = currentTimeMs;
        long lastConsumedTimeMs = currentTimeMs;

        while (messagesRead < numMessages && currentTimeMs - lastConsumedTimeMs <= recordFetchTimeoutMs) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            currentTimeMs = System.currentTimeMillis();
            if (!records.isEmpty())
                lastConsumedTimeMs = currentTimeMs;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                messagesRead += 1;
                if (record.key() != null)
                    bytesRead += record.key().length;
                if (record.value() != null)
                    bytesRead += record.value().length;
                if (currentTimeMs - lastReportTimeMs >= reportingIntervalMs) {
                    if (showDetailedStats)
                        printConsumerProgress(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead,
                            lastReportTimeMs, currentTimeMs, dateFormat, joinTimeMsInSingleRound.get());
                    joinTimeMsInSingleRound = new AtomicLong(0);
                    lastReportTimeMs = currentTimeMs;
                    lastMessagesRead = messagesRead;
                    lastBytesRead = bytesRead;
                }
            }
        }

        if (messagesRead < numMessages)
            System.out.printf("WARNING: Exiting before consuming the expected number of messages: timeout (%d ms) exceeded. " +
                "You can use the --timeout option to increase the timeout.%n", recordFetchTimeoutMs);
        totalMessagesRead.set(messagesRead);
        totalBytesRead.set(bytesRead);
    }

    protected static void printConsumerProgress(int id,
                                                long bytesRead,
                                                long lastBytesRead,
                                                long messagesRead,
                                                long lastMessagesRead,
                                                long startMs,
                                                long endMs,
                                                SimpleDateFormat dateFormat,
                                                long joinTimeMsInSingleRound) {
        printBasicProgress(id, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, endMs, dateFormat);
        printExtendedProgress(bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, endMs, joinTimeMsInSingleRound);
        System.out.println();
    }

    private static void printBasicProgress(int id,
                                           long bytesRead,
                                           long lastBytesRead,
                                           long messagesRead,
                                           long lastMessagesRead,
                                           long startMs,
                                           long endMs,
                                           SimpleDateFormat dateFormat) {
        double elapsedMs = endMs - startMs;
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        double intervalMbPerSec = 1000.0 * intervalMbRead / elapsedMs;
        double intervalMessagesPerSec = ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0;
        System.out.printf("%s, %d, %.4f, %.4f, %d, %.4f", dateFormat.format(endMs), id,
            totalMbRead, intervalMbPerSec, messagesRead, intervalMessagesPerSec);
    }

    private static void printExtendedProgress(long bytesRead,
                                              long lastBytesRead,
                                              long messagesRead,
                                              long lastMessagesRead,
                                              long startMs,
                                              long endMs,
                                              long joinTimeMsInSingleRound) {
        long fetchTimeMs = endMs - startMs - joinTimeMsInSingleRound;
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        long intervalMessagesRead = messagesRead - lastMessagesRead;
        double intervalMbPerSec = (fetchTimeMs <= 0) ? 0.0 : 1000.0 * intervalMbRead / fetchTimeMs;
        double intervalMessagesPerSec = (fetchTimeMs <= 0) ? 0.0 : 1000.0 * intervalMessagesRead / fetchTimeMs;
        System.out.printf(", %d, %d, %.4f, %.4f", joinTimeMsInSingleRound,
            fetchTimeMs, intervalMbPerSec, intervalMessagesPerSec);
    }

    public static class ConsumerPerfRebListener implements ConsumerRebalanceListener {
        private AtomicLong joinTimeMs;
        private AtomicLong joinTimeMsInSingleRound;
        private long joinStartMs;

        public ConsumerPerfRebListener(AtomicLong joinTimeMs, long joinStartMs, AtomicLong joinTimeMsInSingleRound) {
            this.joinTimeMs = joinTimeMs;
            this.joinStartMs = joinStartMs;
            this.joinTimeMsInSingleRound = joinTimeMsInSingleRound;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            joinStartMs = System.currentTimeMillis();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            long elapsedMs = System.currentTimeMillis() - joinStartMs;
            joinTimeMs.addAndGet(elapsedMs);
            joinTimeMsInSingleRound.addAndGet(elapsedMs);
        }
    }

    protected static class ConsumerPerfOptions extends CommandDefaultOptions {
        private final OptionSpec<String> brokerListOpt;
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> groupIdOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<Void> resetBeginningOffsetOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        private final OptionSpec<Integer> numThreadsOpt;
        private final OptionSpec<Integer> numFetchersOpt;
        private final OptionSpec<String> consumerConfigOpt;
        private final OptionSpec<Void> printMetricsOpt;
        private final OptionSpec<Void> showDetailedStatsOpt;
        private final OptionSpec<Long> recordFetchTimeoutOpt;
        private final OptionSpec<Long> numMessagesOpt;
        private final OptionSpec<Long> reportingIntervalOpt;
        private final OptionSpec<String> dateFormatOpt;
        private final OptionSpec<Void> hideHeaderOpt;

        public ConsumerPerfOptions(String[] args) {
            super(args);
            brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified. The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                .withRequiredArg()
                .describedAs("broker-list")
                .ofType(String.class);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to.")
                .requiredUnless("broker-list")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
            groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("perf-consumer-" + RND.nextInt(100_000))
                .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
            resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
                "offset to consume from, start with the latest message present in the log rather than the earliest message.");
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(2 * 1024 * 1024);
            numThreadsOpt = parser.accepts("threads", "DEPRECATED AND IGNORED: Number of processing threads.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Integer.class)
                .defaultsTo(10);
            numFetchersOpt = parser.accepts("num-fetch-threads", "DEPRECATED AND IGNORED: Number of fetcher threads.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Integer.class)
                .defaultsTo(1);
            consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics.");
            showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
                "interval as configured by reporting-interval");
            recordFetchTimeoutOpt = parser.accepts("timeout", "The maximum allowed time in milliseconds between returned records.")
                .withOptionalArg()
                .describedAs("milliseconds")
                .ofType(Long.class)
                .defaultsTo(10_000L);
            numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send or consume")
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
            hideHeaderOpt = parser.accepts("hide-header", "If set, skips printing the header for the stats");
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
                return;
            }
            if (options != null) {
                if (options.has(numThreadsOpt) || options.has(numFetchersOpt))
                    System.out.println("WARNING: option [threads] and [num-fetch-threads] have been deprecated and will be ignored by the test");
                CommandLineUtils.maybePrintHelpOrVersion(this, "This tool is used to verify the consumer performance.");
                CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt);
            }
        }

        public boolean printMetrics() {
            return options.has(printMetricsOpt);
        }

        public String brokerHostsAndPorts() {
            return options.valueOf(options.has(bootstrapServerOpt) ? bootstrapServerOpt : brokerListOpt);
        }

        public Properties props() throws IOException {
            Properties props = (options.has(consumerConfigOpt))
                ? Utils.loadProps(options.valueOf(consumerConfigOpt))
                : new Properties();
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

        public Set<String> topic() {
            return Collections.singleton(options.valueOf(topicOpt));
        }

        public long numMessages() {
            return options.valueOf(numMessagesOpt);
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
