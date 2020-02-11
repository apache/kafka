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

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

public class KafkaClientPerformance {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();
        ClientTestHandler<byte[], byte[]> clientTestHandler = null;
        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            long numRecords = res.getLong("numRecords");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String transactionalId = res.getString("transactionalId");
            String consumerGroupId = res.getString("consumerGroupId");
            boolean shouldPrintMetrics = res.getBoolean("printMetrics");
            long transactionDurationMs = res.getLong("transactionDurationMs");
            long flushDurationMs = res.getLong("flushDurationMs");
            long discountStartRecords = res.getLong("discountStartRecords");
            boolean flushEnabled = 0 < flushDurationMs;
            boolean transactionsEnabled =  0 < transactionDurationMs;

            if (producerProps == null && producerConfig == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }

            Properties props = generateProps(producerProps, producerConfig);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

            if (transactionsEnabled)
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

            /* initialize test suite */
            String testSuiteName = res.getString("testSuiteName");
            String outputTopic = res.getString("outputTopic");
            switch (testSuiteName) {
                case "ProducerPerformance":
                    clientTestHandler = new RandomPayloadTestHandler(res, producer, outputTopic);
                    break;
                case "EosPerformance":
                    clientTestHandler = new ConsumerTestHandler<>(res, consumerGroupId, producer, outputTopic);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown test suite name: " + testSuiteName);
            }

            if (transactionsEnabled)
                producer.initTransactions();

            ProducerRecord<byte[], byte[]> record;
            Stats stats = new Stats(numRecords, 5000, discountStartRecords);
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            int currentTransactionSize = 0;
            long transactionStartTime = 0;
            long lastFlushTime = 0;
            // First iterations will be discarded for correctness.
            for (long i = 0; i < numRecords + discountStartRecords; i++) {
                if (transactionsEnabled && currentTransactionSize == 0) {
                    producer.beginTransaction();
                    transactionStartTime = System.currentTimeMillis();
                }

                record = clientTestHandler.getRecord();
                if (record == null) {
                    System.out.println("Skipping null record");
                    // Reset this cycle as we are skipping over null record.
                    i -= 1;
                    continue;
                }

                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, record.value().length, stats);
                producer.send(record, cb);

                currentTransactionSize++;
                if (transactionsEnabled && transactionDurationMs <= (sendStartMs - transactionStartTime)) {
                    clientTestHandler.commitTransaction();
                    currentTransactionSize = 0;
                }

                if (flushEnabled && flushDurationMs <= (sendStartMs - lastFlushTime)) {
                    clientTestHandler.flush();
                    lastFlushTime = System.currentTimeMillis();
                }

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            if (transactionsEnabled && currentTransactionSize != 0)
                producer.commitTransaction();

            if (flushEnabled) {
                producer.flush();
            }

            if (!shouldPrintMetrics) {
                producer.close();

                /* print final results */
                stats.printTotal();
            } else {
                // Make sure all messages are sent before printing out the stats and the metrics
                // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
                // expects this class to work with older versions of the client jar that don't support flush().
                producer.flush();

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
        } finally {
            if (clientTestHandler != null) {
                clientTestHandler.close();
            }
        }
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-client-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the Kafka client performance.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        parser.addArgument("--test-suite-name")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("TEST-SUITE-NAME")
                .dest("testSuiteName")
                .help("the name of this test suite");

        parser.addArgument("--input-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("INPUT-TOPIC")
                .dest("inputTopic")
                .help("consume messages from this topic");

        parser.addArgument("--output-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("OUTPUT-TOPIC")
                .dest("outputTopic")
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
                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file.");

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
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--poll-timeout")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("POLL-TIMEOUT")
                .dest("pollTimeout")
                .setDefault(100L)
                .help("consumer poll timeout");

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

        parser.addArgument("--consumer-config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("consumerConfigFile")
                .help("consumer config properties file.");

        parser.addArgument("--consumer-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("consumerConfig")
                .help("kafka consumer related configuration properties like bootstrap.servers,client.id etc. " +
                      "These configs take precedence over those passed via --consumer-config.");

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
                .setDefault("performance-producer-default-transactional-id")
                .help("The transactionalId to use if transaction-duration-ms is > 0. Useful when testing the performance of concurrent transactions.");

        parser.addArgument("--transaction-duration-ms")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("TRANSACTION-DURATION")
                .dest("transactionDurationMs")
                .setDefault(0L)
                .help("The max age of each transaction. The commitTransaction will be called after this time has elapsed. Transactions are only enabled if this value is positive.");

        parser.addArgument("--consumer-group-id")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONSUMER-GROUP-ID")
                .dest("consumerGroupId")
                .setDefault("performance-consumer-default-group-id")
                .help("The consumerGroupId to use if we use consumer as the data source.");

        parser.addArgument("--flush-duration-ms")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("FLUSH-DURATION")
                .dest("flushDurationMs")
                .setDefault(0L)
                .help("When configured, the amount of time to elapse before making a producer.flush() call. Explicit flush is only enabled if this value is positive.");

        parser.addArgument("--discount-start-records")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("DISCOUNT-RECORDS")
                .dest("discountStartRecords")
                .setDefault(100L)
                .help("Drop the first few records to avoid start-up jitters affecting the throughput result");


        return parser;
    }

    static Properties generateProps(List<String> overrideProps, String configFile) throws IOException {
        Properties props = new Properties();
        if (configFile != null) {
            props.putAll(Utils.loadProps(configFile));
        }

        if (overrideProps != null) {
            for (String prop : overrideProps) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                props.put(pieces[0], pieces[1]);
            }
        }
        return props;
    }


    static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        // Records discounted for final result
        private long discount;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval, long discount) {
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            if (discount < 0L) {
                throw new IllegalArgumentException("Record discount must be greater or equal to 0");
            }
            this.discount = discount;
        }

        public void record(int iter, int latency, int bytes, long time) {
            // Discount records will not be included in the final result
            if (discount > 0) {
                discount--;
                return;
            } else if (discount == 0) {
                // When all the discount records are disgarded, offically starts the window recording
                this.start = System.currentTimeMillis();
                this.windowStart = System.currentTimeMillis();
                this.iteration = 0;
                discount--;
            }

            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
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
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

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

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

}
