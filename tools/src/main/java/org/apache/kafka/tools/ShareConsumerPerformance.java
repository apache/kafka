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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

import static joptsimple.util.RegexMatcher.regex;
import static org.apache.kafka.server.util.CommandLineUtils.parseKeyValueArgs;

public class ShareConsumerPerformance {
    private static final Logger LOG = LoggerFactory.getLogger(ShareConsumerPerformance.class);

    public static void main(String[] args) {
        run(args, KafkaShareConsumer::new);
    }

    static void run(String[] args, Function<Properties, ShareConsumer<byte[], byte[]>> shareConsumerCreator) {
        try {
            LOG.info("Starting share consumer/consumers...");
            ShareConsumerPerfOptions options = new ShareConsumerPerfOptions(args);
            AtomicLong totalRecordsRead = new AtomicLong(0);
            AtomicLong totalBytesRead = new AtomicLong(0);

            if (!options.hideHeader())
                printHeader();

            List<ShareConsumer<byte[], byte[]>> shareConsumers = new ArrayList<>();
            List<String> clientIds = new ArrayList<>();
            for (int i = 0; i < options.threads(); i++) {
                if (options.threads() == 1) {
                    clientIds.add(options.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
                    shareConsumers.add(shareConsumerCreator.apply(options.props()));
                    break;
                }
                Properties shareConsumerProps = options.props();
                String shareConsumerClientId = options.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + "-" + (i + 1);
                shareConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, shareConsumerClientId);
                clientIds.add(shareConsumerClientId);
                shareConsumers.add(shareConsumerCreator.apply(shareConsumerProps));
            }
            long startMs = System.currentTimeMillis();
            consume(shareConsumers, options, totalRecordsRead, totalBytesRead, startMs, clientIds);
            long endMs = System.currentTimeMillis();

            List<Map<MetricName, ? extends Metric>> shareConsumersMetrics = new ArrayList<>();
            if (options.printMetrics()) {
                shareConsumers.forEach(shareConsumer -> shareConsumersMetrics.add(shareConsumer.metrics()));
            }
            shareConsumers.forEach(shareConsumer -> {
                @SuppressWarnings("UnusedLocalVariable")
                Map<TopicIdPartition, Optional<KafkaException>> ignored = shareConsumer.commitSync();
            });

            // Print final stats for share group.
            double elapsedSec = (endMs - startMs) / 1_000.0;
            long fetchTimeInMs = endMs - startMs;
            printStatsForShareGroup(totalBytesRead.get(), totalRecordsRead.get(), elapsedSec, fetchTimeInMs, startMs,
                endMs, options.dateFormat());

            shareConsumersMetrics.forEach(ToolsUtils::printMetrics);

            shareConsumers.forEach(shareConsumer -> shareConsumer.close(Duration.ofMillis(500)));
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    protected static void printHeader() {
        String newFieldsInHeader = ", fetch.time.ms";
        System.out.printf("start.time, end.time, data.consumed.in.MB, MB.sec, nMsg.sec, data.consumed.in.nMsg%s%n", newFieldsInHeader);
    }

    private static void consume(List<ShareConsumer<byte[], byte[]>> shareConsumers,
                                ShareConsumerPerfOptions options,
                                AtomicLong totalRecordsRead,
                                AtomicLong totalBytesRead,
                                long startMs,
                                List<String> clientIds) throws ExecutionException, InterruptedException {
        long numRecords = options.numRecords();
        long recordFetchTimeoutMs = options.recordFetchTimeoutMs();
        shareConsumers.forEach(shareConsumer -> shareConsumer.subscribe(options.topic()));

        // Now start the benchmark.
        AtomicLong recordsRead = new AtomicLong(0);
        AtomicLong bytesRead = new AtomicLong(0);
        List<ShareConsumerConsumption> shareConsumersConsumptionDetails = new ArrayList<>();


        ExecutorService executorService = Executors.newFixedThreadPool(shareConsumers.size());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < shareConsumers.size(); i++) {
            final int index = i;
            ShareConsumerConsumption shareConsumerConsumption = new ShareConsumerConsumption(0, 0);
            futures.add(executorService.submit(() -> {
                try {
                    consumeRecordsForSingleShareConsumer(shareConsumers.get(index), recordsRead, bytesRead, options,
                        shareConsumerConsumption, index + 1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
            shareConsumersConsumptionDetails.add(shareConsumerConsumption);
        }
        LOG.debug("Shutting down of thread pool is started");
        executorService.shutdown();

        try {
            // Wait a while for existing tasks to terminate.
            // Adding 100 ms to the timeout so all the threads can finish before we reach this part of code.
            if (!executorService.awaitTermination(recordFetchTimeoutMs + 100, TimeUnit.MILLISECONDS)) {
                LOG.debug("Shutting down of thread pool could not be completed. It will retry cancelling the tasks using shutdownNow.");
                executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(recordFetchTimeoutMs + 100, TimeUnit.MILLISECONDS))
                    LOG.debug("Shutting down of thread pool could not be completed even after retrying cancellation of the tasks using shutdownNow.");
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            LOG.warn("Encountered InterruptedException while shutting down thread pool. It will retry cancelling the tasks using shutdownNow.");
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        for (Future<?> future : futures) {
            future.get();
        }

        if (options.showShareConsumerStats()) {
            long endMs = System.currentTimeMillis();
            for (int index = 0; index < shareConsumersConsumptionDetails.size(); index++) {
                // Print stats for share consumer.
                double elapsedSec = (endMs - startMs) / 1_000.0;
                long fetchTimeInMs = endMs - startMs;
                long recordsReadByConsumer = shareConsumersConsumptionDetails.get(index).recordsConsumed();
                long bytesReadByConsumer = shareConsumersConsumptionDetails.get(index).bytesConsumed();
                printStatsForShareConsumer(clientIds.get(index), bytesReadByConsumer, recordsReadByConsumer, elapsedSec, fetchTimeInMs, startMs, endMs, options.dateFormat(), index + 1);
            }
        }

        if (recordsRead.get() < numRecords) {
            System.out.printf("WARNING: Exiting before consuming the expected number of records: timeout (%d ms) exceeded. " +
                    "You can use the --timeout option to increase the timeout.%n", recordFetchTimeoutMs);
        }
        totalRecordsRead.set(recordsRead.get());
        totalBytesRead.set(bytesRead.get());
    }

    private static void consumeRecordsForSingleShareConsumer(ShareConsumer<byte[], byte[]> shareConsumer,
                                                              AtomicLong totalRecordsRead,
                                                              AtomicLong totalBytesRead,
                                                              ShareConsumerPerfOptions options,
                                                              ShareConsumerConsumption shareConsumerConsumption,
                                                              int index) throws InterruptedException {
        SimpleDateFormat dateFormat = options.dateFormat();
        long currentTimeMs = System.currentTimeMillis();
        long lastConsumedTimeMs = currentTimeMs;
        long lastReportTimeMs = currentTimeMs;

        long lastBytesRead = 0L;
        long lastRecordsRead = 0L;
        long recordsReadByConsumer = 0L;
        long bytesReadByConsumer = 0L;
        while (totalRecordsRead.get() < options.numRecords() && currentTimeMs - lastConsumedTimeMs <= options.recordFetchTimeoutMs()) {
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(100));
            currentTimeMs = System.currentTimeMillis();
            if (!records.isEmpty())
                lastConsumedTimeMs = currentTimeMs;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                recordsReadByConsumer += 1;
                totalRecordsRead.addAndGet(1);
                if (record.key() != null) {
                    bytesReadByConsumer += record.key().length;
                    totalBytesRead.addAndGet(record.key().length);
                }
                if (record.value() != null) {
                    bytesReadByConsumer += record.value().length;
                    totalBytesRead.addAndGet(record.value().length);
                }
                if (currentTimeMs - lastReportTimeMs >= options.reportingIntervalMs()) {
                    if (options.showDetailedStats())
                        printShareConsumerProgress(bytesReadByConsumer, lastBytesRead, recordsReadByConsumer, lastRecordsRead,
                                lastReportTimeMs, currentTimeMs, dateFormat, index);
                    lastReportTimeMs = currentTimeMs;
                    lastRecordsRead = recordsReadByConsumer;
                    lastBytesRead = bytesReadByConsumer;
                }
                shareConsumerConsumption.updateRecordsConsumed(recordsReadByConsumer);
                shareConsumerConsumption.updateBytesConsumed(bytesReadByConsumer);
            }
        }
    }

    protected static void printShareConsumerProgress(long bytesRead,
                                                long lastBytesRead,
                                                long recordsRead,
                                                long lastRecordsRead,
                                                long startMs,
                                                long endMs,
                                                SimpleDateFormat dateFormat,
                                                int index) {
        double elapsedMs = endMs - startMs;
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        double intervalMbPerSec = 1000.0 * intervalMbRead / elapsedMs;
        double intervalRecordsPerSec = ((recordsRead - lastRecordsRead) / elapsedMs) * 1000.0;
        long fetchTimeMs = endMs - startMs;

        System.out.printf("%s, %s, %.4f, %.4f, %.4f, %d, %d for share consumer %d", dateFormat.format(startMs), dateFormat.format(endMs),
            totalMbRead, intervalMbPerSec, intervalRecordsPerSec, recordsRead, fetchTimeMs, index);
        System.out.println();
    }

    private static void printStatsForShareConsumer(
        String clientId,
        long bytesRead,
        long recordsRead,
        double elapsedSec,
        long fetchTimeInMs,
        long startMs,
        long endMs,
        SimpleDateFormat dateFormat,
        int index) {
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        System.out.printf("Share consumer %s having client id %s consumption metrics- %s, %s, %.4f, %.4f, %.4f, %d, %d%n",
                index,
                clientId,
                dateFormat.format(startMs),
                dateFormat.format(endMs),
                totalMbRead,
                totalMbRead / elapsedSec,
                recordsRead / elapsedSec,
                recordsRead,
                fetchTimeInMs
        );
    }

    private static void printStatsForShareGroup(
        long bytesRead,
        long recordsRead,
        double elapsedSec,
        long fetchTimeInMs,
        long startMs,
        long endMs,
        SimpleDateFormat dateFormat) {
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        System.out.printf("%s, %s, %.4f, %.4f, %.4f, %d, %d%n",
            dateFormat.format(startMs),
            dateFormat.format(endMs),
            totalMbRead,
            totalMbRead / elapsedSec,
            recordsRead / elapsedSec,
            recordsRead,
            fetchTimeInMs
        );
    }

    protected static class ShareConsumerPerfOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> groupIdOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<String> commandPropertiesOpt;
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
        private final OptionSpec<Integer> numThreadsOpt;
        private final OptionSpec<Void> showShareConsumerStatsOpt;

        public ShareConsumerPerfOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to.")
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
                    .defaultsTo("perf-share-consumer")
                    .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(1024 * 1024);
            commandPropertiesOpt = parser.accepts("command-property", "Kafka share consumer related configuration properties like client.id. " +
                            "These configs take precedence over those passed via --command-config or --consumer.config.")
                    .withRequiredArg()
                    .describedAs("prop1=val1")
                    .ofType(String.class);
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(2 * 1024 * 1024);
            consumerConfigOpt = parser.accepts("consumer.config", "(DEPRECATED) Share consumer config properties file. " +
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
            numThreadsOpt = parser.accepts("threads", "The number of share consumers to use for sharing the load.")
                    .withRequiredArg()
                    .describedAs("count")
                    .ofType(Integer.class)
                    .defaultsTo(1);
            showShareConsumerStatsOpt = parser.accepts("show-consumer-stats", "If set, stats are reported for each share " +
                    "consumer depending on the no. of threads.");
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
                return;
            }
            if (options != null) {
                CommandLineUtils.maybePrintHelpOrVersion(this, "This tool is used to verify the share consumer performance.");
                CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, bootstrapServerOpt);

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
            props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
            if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, "perf-share-consumer-client");
            return props;
        }

        public Set<String> topic() {
            return Set.of(options.valueOf(topicOpt));
        }

        public long numRecords() {
            return options.has(numMessagesOpt)
                    ? options.valueOf(numMessagesOpt)
                    : options.valueOf(numRecordsOpt);
        }

        public int threads() {
            return options.valueOf(numThreadsOpt);
        }

        public boolean showShareConsumerStats() {
            return options.has(showShareConsumerStatsOpt);
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

    // Helper class to know the final records and bytes consumed by share consumer at the end of consumption.
    private static class ShareConsumerConsumption {
        private long recordsConsumed;
        private long bytesConsumed;

        public ShareConsumerConsumption(long recordsConsumed, long bytesConsumed) {
            this.recordsConsumed = recordsConsumed;
            this.bytesConsumed = bytesConsumed;
        }

        public long recordsConsumed() {
            return recordsConsumed;
        }

        public long bytesConsumed() {
            return bytesConsumed;
        }

        public void updateRecordsConsumed(long recordsConsumed) {
            this.recordsConsumed = recordsConsumed;
        }

        public void updateBytesConsumed(long bytesConsumed) {
            this.bytesConsumed = bytesConsumed;
        }
    }
}
