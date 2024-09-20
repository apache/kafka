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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.ThroughputThrottler;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.SplittableRandom;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ProducerPerformance {

    public static final String DEFAULT_TRANSACTION_ID_PREFIX = "performance-producer-";
    public static final long DEFAULT_TRANSACTION_DURATION_MS = 3000L;

    public static void main(String[] args) throws Exception {
        ProducerPerformance perf = new ProducerPerformance();
        perf.start(args);
    }

    void start(String[] args) throws IOException {
        ArgumentParser parser = argParser();

        try {
            ConfigPostProcessor config = new ConfigPostProcessor(parser, args);
            KafkaProducer<byte[], byte[]> producer = createKafkaProducer(config.producerProps);

            if (config.transactionsEnabled)
                producer.initTransactions();

            /* setup perf test */
            byte[] payload = null;
            if (config.recordSize != null) {
                payload = new byte[config.recordSize];
            }
            // not thread-safe, do not share with other threads
            SplittableRandom random = new SplittableRandom(0);
            ProducerRecord<byte[], byte[]> record;
            if (config.warmupRecords > 0) {
                // TODO: Keep this message? Maybe unnecessary
                System.out.println("Warmup first " + config.warmupRecords + " records. Steady-state results will print after the complete-test summary.");
                this.warmupStats = new Stats(config.warmupRecords, 5000);
            } else {
                stats = new Stats(config.numRecords, 5000);
            }
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(config.throughput, startMs);

            int currentTransactionSize = 0;
            long transactionStartTime = 0;
            for (long i = 0; i < config.numRecords; i++) {

                payload = generateRandomPayload(config.recordSize, config.payloadByteList, payload, random, config.payloadMonotonic, i);

                if (config.transactionsEnabled && currentTransactionSize == 0) {
                    producer.beginTransaction();
                    transactionStartTime = System.currentTimeMillis();
                }

                record = new ProducerRecord<>(config.topicName, payload);

                long sendStartMs = System.currentTimeMillis();
                if (warmupStats != null) {
                    if (i < config.warmupRecords) {
                        cb = new PerfCallback(sendStartMs, payload.length, warmupStats);
                    } else if (i == config.warmupRecords) {
                        stats = new Stats(config.numRecords - config.warmupRecords, 5000, config.warmupRecords);
                        cb = new PerfCallback(sendStartMs, payload.length, stats);
                    }
                    producer.send(record, cb);
                } else {
                    cb = new PerfCallback(sendStartMs, payload.length, stats);
                    producer.send(record, cb);
                }

                currentTransactionSize++;
                if (config.transactionsEnabled && config.transactionDurationMs <= (sendStartMs - transactionStartTime)) {
                    producer.commitTransaction();
                    currentTransactionSize = 0;
                }

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            if (config.transactionsEnabled && currentTransactionSize != 0)
                producer.commitTransaction();

            if (!config.shouldPrintMetrics) {
                producer.close();

                /* print warmup stats if relevant */
                if (warmupStats != null) {
                    overallStats = new Stats(warmupStats, stats);
                    overallStats.printTotal();
                }
                /* print final results */
                stats.printTotal();
            } else {
                // Make sure all messages are sent before printing out the stats and the metrics
                // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
                // expects this class to work with older versions of the client jar that don't support flush().
                producer.flush();

                /* print warmup stats if relevant */
                if (warmupStats != null) {
                    overallStats = new Stats(warmupStats, stats);
                    overallStats.printTotal();
                }
                /* print final results */
                stats.printTotal();

                /* print out metrics */
                ToolsUtils.printMetrics(producer.metrics());
                producer.close();
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

    }

    KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    Callback cb;

    Stats stats;
    Stats overallStats;
    Stats warmupStats;

    static byte[] generateRandomPayload(Integer recordSize, List<byte[]> payloadByteList, byte[] payload,
            SplittableRandom random, boolean payloadMonotonic, long recordValue) {
        if (!payloadByteList.isEmpty()) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        } else if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else if (payloadMonotonic) {
            payload = Long.toString(recordValue).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("no payload File Path or record Size or payload-monotonic option provided");
        }
        return payload;
    }
    
    static Properties readProps(List<String> producerProps, String producerConfig) throws IOException {
        Properties props = new Properties();
        if (producerConfig != null) {
            props.putAll(Utils.loadProps(producerConfig));
        }
        if (producerProps != null)
            for (String prop : producerProps) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                props.put(pieces[0], pieces[1]);
            }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "perf-producer-client");
        }
        return props;
    }

    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            System.out.println("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            System.out.println("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        return payloadByteList;
    }

    /** Get the command-line argument parser. */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance. To enable transactions, " +
                        "you can specify a transaction id or set a transaction duration using --transaction-duration-ms. " +
                        "There are three ways to specify the transaction id: set transaction.id=<id> via --producer-props, " +
                        "set transaction.id=<id> in the config file via --producer.config, or use --transaction-id <id>.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        payloadOptions.addArgument("--record-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file " +
                        "or --payload-monotonic.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file or --payload-monotonic.");

        payloadOptions.addArgument("--payload-monotonic")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PAYLOAD-MONOTONIC")
                .dest("payloadMonotonic")
                .help("payload is monotonically increasing integer. Note that you must provide exactly one of --record-size " +
                        "or --payload-file or --payload-monotonic.");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Double.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--producer-props")
                 .nargs("+")
                 .required(false)
                 .metavar("PROP-NAME=PROP-VALUE")
                 .type(String.class)
                 .dest("producerConfig")
                 .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
                         "These configs take precedence over those passed via --producer.config.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("producer config properties file.");

        parser.addArgument("--print-metrics")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PRINT-METRICS")
                .dest("printMetrics")
                .help("print out metrics at the end of the test.");

        parser.addArgument("--transactional-id")
               .action(store())
               .required(false)
               .type(String.class)
               .metavar("TRANSACTIONAL-ID")
               .dest("transactionalId")
               .help("The transactional id to use. This config takes precedence over the transactional.id " +
                       "specified via --producer.config or --producer-props. Note that if the transactional id " +
                       "is not specified while --transaction-duration-ms is provided, the default value for the " +
                       "transactional id will be performance-producer- followed by a random uuid.");

        parser.addArgument("--transaction-duration-ms")
               .action(store())
               .required(false)
               .type(Long.class)
               .metavar("TRANSACTION-DURATION")
               .dest("transactionDurationMs")
               .help("The max age of each transaction. The commitTransaction will be called after this time has elapsed. " +
                       "The value should be greater than 0. If the transactional id is specified via --producer-props, " +
                       "--producer.config, or --transactional-id but --transaction-duration-ms is not specified, " +
                       "the default value will be 3000.");

        parser.addArgument("--warmup-records")
               .action(store())
               .required(false)
               .type(Long.class)
               .metavar("WARMUP-RECORDS")
               .dest("warmupRecords")
               .setDefault(0L)
               .help("The number of records to treat as warmup; these initial records will not be included in steady-state statistics. " +
                       "An additional summary line will be printed describing the steady-state statistics. (default: 0).");

        return parser;
    }

    // Visible for testing
    static class Stats {
        private final long start;
        private final int[] latencies;
        private final long sampling;
        private final long reportingInterval;
        private long iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long windowStart;
        private long warmupRecords;

        public Stats(long numRecords, int reportingInterval) {
            this(numRecords, reportingInterval, 0);
        }

        public Stats(long numRecords, int reportingInterval, long warmupRecords) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = numRecords / Math.min(numRecords, 500000);
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            this.warmupRecords = warmupRecords;
        }

        public Stats(Stats first, Stats second) {
            // create a Stats object that's the combination of two disjoint Stats objects
            this.start = Math.min(first.start, second.start);
            this.iteration = first.iteration + second.iteration;
            this.sampling = first.sampling;
            this.latencies = Arrays.copyOf(first.latencies, first.index + second.index);
            System.arraycopy(second.latencies, 0, this.latencies, first.index(), second.index());
            this.maxLatency = Math.max(first.maxLatency, second.maxLatency);
            this.windowCount = first.windowCount + second.windowCount;
            this.totalLatency = first.totalLatency + second.totalLatency;
            this.reportingInterval = first.reportingInterval;
            this.warmupRecords = 0;
            this.count = first.count + second.count;
            // unused vars, populating to prevent compiler errors:
            //this.windowMaxLatency = 0;
            //this.windowTotalLatency = 0;
            //this.windowBytes = 0;
        }

        public void record(int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (this.iteration % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public long totalCount() {
            return this.count;
        }

        public long currentWindowCount() {
            return this.windowCount;
        }

        public long iteration() {
            return this.iteration;
        }

        public long bytes() {
            return this.bytes;
        }

        public int index() {
            return this.index;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            String state = "";
            if (this.warmupRecords > 0) {
                state = " steady state";
            }
            System.out.printf("%d%s records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              count,
                              state,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        /*
        public void combineStats(Stats stats) {
            this.count += stats.totalCount();
            this.bytes += stats.bytes;
            this.totalLatency += stats.totalLatency;
            this.latencies = Arrays.copyOf(this.latencies, index + stats.index);
            System.arraycopy(stats.latencies, 0, this.latencies, this.index(), stats.index());
            this.index += stats.index;
        }

        public void printTotal(Stats warmupStats) {
            long overallElapsed = System.currentTimeMillis() - warmupStats.start;
            long overallCount = count + warmupStats.totalCount();
            long overallBytes = bytes + warmupStats.bytes();
            double overallRecsPerSec = 1000.0 * overallCount / (double) overallElapsed;
            double overallMbPerSec = 1000.0 * overallBytes / (double) overallElapsed / (1024.0 * 1024.0);
            int overallMax = Math.max(maxLatency, warmupStats.maxLatency);
            long overallTotalLatency = totalLatency + warmupStats.totalLatency;

            int totalElements = index + warmupStats.index();
            int[] overallLatencyArray = Arrays.copyOf(warmupStats.latencies, totalElements);
            System.arraycopy(this.latencies, 0, overallLatencyArray, warmupStats.index(), this.index());

            int[] percs = percentiles(overallLatencyArray, totalElements, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              overallCount,
                              overallRecsPerSec,
                              overallMbPerSec,
                              overallTotalLatency / (double) overallCount,
                              (double) overallMax,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }
        */

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    static final class PerfCallback implements Callback {
        private final long start;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            // It will only be counted when the sending is successful, otherwise the number of sent records may be
            // magically printed when the sending fails.
            if (exception == null) {
                this.stats.record(latency, bytes, now);
                this.stats.iteration++;
            }
            if (exception != null)
                exception.printStackTrace();
        }
    }

    static final class ConfigPostProcessor {
        final String topicName;
        final long numRecords;
        final long warmupRecords;
        final Integer recordSize;
        final double throughput;
        final boolean payloadMonotonic;
        final Properties producerProps;
        final boolean shouldPrintMetrics;
        final Long transactionDurationMs;
        final boolean transactionsEnabled;
        final List<byte[]> payloadByteList;

        public ConfigPostProcessor(ArgumentParser parser, String[] args) throws IOException, ArgumentParserException {
            Namespace namespace = parser.parseArgs(args);
            this.topicName = namespace.getString("topic");
            this.numRecords = namespace.getLong("numRecords");
            this.warmupRecords = Math.max(namespace.getLong("warmupRecords"), 0);
            this.recordSize = namespace.getInt("recordSize");
            this.throughput = namespace.getDouble("throughput");
            this.payloadMonotonic = namespace.getBoolean("payloadMonotonic");
            this.shouldPrintMetrics = namespace.getBoolean("printMetrics");

            List<String> producerConfigs = namespace.getList("producerConfig");
            String producerConfigFile = namespace.getString("producerConfigFile");
            String payloadFilePath = namespace.getString("payloadFile");
            Long transactionDurationMsArg = namespace.getLong("transactionDurationMs");
            String transactionIdArg = namespace.getString("transactionalId");
            if (warmupRecords >= numRecords) {
                throw new ArgumentParserException("The value for --warmup-records must be strictly fewer than the number of records in the test, --num-records.", parser);
            }
            if (producerConfigs == null && producerConfigFile == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }
            if (transactionDurationMsArg != null && transactionDurationMsArg <= 0) {
                throw new ArgumentParserException("--transaction-duration-ms should > 0", parser);
            }

            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = namespace.getString("payloadDelimiter").equals("\\n")
                    ? "\n" : namespace.getString("payloadDelimiter");
            this.payloadByteList = readPayloadFile(payloadFilePath, payloadDelimiter);
            this.producerProps = readProps(producerConfigs, producerConfigFile);
            // setup transaction related configs
            this.transactionsEnabled = transactionDurationMsArg != null
                    || transactionIdArg != null
                    || producerProps.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            if (transactionsEnabled) {
                Optional<String> txIdInProps =
                        Optional.ofNullable(producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                                .map(Object::toString);
                String transactionId = Optional.ofNullable(transactionIdArg).orElse(txIdInProps.orElse(DEFAULT_TRANSACTION_ID_PREFIX + Uuid.randomUuid().toString()));
                producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

                if (transactionDurationMsArg == null) {
                    transactionDurationMsArg = DEFAULT_TRANSACTION_DURATION_MS;
                }
            }
            this.transactionDurationMs = transactionDurationMsArg;
        }
    }
}
