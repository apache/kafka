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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

public class ProducerPerformance {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            Integer recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String payloadFilePath = res.getString("payloadFile");
            String transactionalId = res.getString("transactionalId");
            boolean shouldPrintMetrics = res.getBoolean("printMetrics");
            final boolean withRdma = res.getBoolean("withRdma");
            long transactionDurationMs = res.getLong("transactionDurationMs");
            boolean transactionsEnabled =  0 < transactionDurationMs;

            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");

            if (producerProps == null && producerConfig == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }

            List<byte[]> payloadByteList = new ArrayList<>();
            if (payloadFilePath != null) {
                Path path = Paths.get(payloadFilePath);
                System.out.println("Reading payloads from: " + path.toAbsolutePath());
                if (Files.notExists(path) || Files.size(path) == 0)  {
                    throw new  IllegalArgumentException("File does not exist or empty file provided.");
                }

                String[] payloadList = new String(Files.readAllBytes(path), "UTF-8").split(payloadDelimiter);

                System.out.println("Number of messages read: " + payloadList.length);

                for (String payload : payloadList) {
                    payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
                }
            }

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
            if (transactionsEnabled)
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

            if (transactionsEnabled)
                producer.initTransactions();

            /* setup perf test */
            byte[] payload = null;
            Random random = new Random(0);
            if (recordSize != null) {
                payload = new byte[recordSize];
                for (int i = 0; i < payload.length; ++i)
                    payload[i] = (byte) (random.nextInt(26) + 65);
            }
            ProducerRecord<byte[], byte[]> record;
            Stats stats = new Stats(numRecords, 5000);
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            int currentTransactionSize = 0;
            long transactionStartTime = 0;
            for (long i = 0; i < numRecords; i++) {
                if (transactionsEnabled && currentTransactionSize == 0) {
                    producer.beginTransaction();
                    transactionStartTime = System.currentTimeMillis();
                }


                if (payloadFilePath != null) {
                    payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
                }
                record = new ProducerRecord<>(topicName, payload);

                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, System.nanoTime(), payload.length, stats);

                if (withRdma)
                    producer.RDMAsend(record, cb); // RDMAsend
                else
                    producer.send(record, cb);

                currentTransactionSize++;
                if (transactionsEnabled && transactionDurationMs <= (sendStartMs - transactionStartTime)) {
                    producer.commitTransaction();
                    currentTransactionSize = 0;
                }

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            if (transactionsEnabled && currentTransactionSize != 0)
                producer.commitTransaction();

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
        }

    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

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
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

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

        parser.addArgument("--with-rdma")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("WITH-RDMA")
                .dest("withRdma")
                .help("use rdma for producing");


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


        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
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
        }

        public void record(int iter, int latency, int latencyNano, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latencyNano;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, long startNano, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, startNano, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
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
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %.2f us 50th, %.2f us 95th, %.2f us 99th, %.2f us 99.9th.%n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0] / 1000.0,
                              percs[1] / 1000.0,
                              percs[2] / 1000.0,
                              percs[3] / 1000.0);
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
        private final long startNano;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, long startNano, int bytes, Stats stats) {
            this.start = start;
            this.startNano = startNano;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long nowNano = System.nanoTime();
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            int latencyNano = (int) (nowNano - startNano);
            this.stats.record(iteration, latency, latencyNano, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

}
