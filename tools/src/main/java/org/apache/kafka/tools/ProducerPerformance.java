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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
    private static final AtomicInteger ID = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {

        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            Integer recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            int numThreads = res.getInt("numThreads");
            if (numRecords < numThreads)
                numThreads = (int) numRecords;
            int valueBound = res.getInt("valueBound");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String payloadFilePath = res.getString("payloadFile");
            boolean shouldPrintMetrics = res.getBoolean("printMetrics");

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
            if (producerProps != null) {
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.setProperty(pieces[0], pieces[1]);
                }
            }

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null) {
                props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "Producer-Performance-" + ID.getAndIncrement());
            }
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

            /* setup perf test */
            Stats stats = new Stats(numRecords, 5000, numThreads);
            ProducerPerformanceThread[] producerPerformanceThreads = new ProducerPerformanceThread[numThreads];
            long numRecordsPerThread = numRecords / numThreads;
            int throughputPerThread = throughput <= 0 ? throughput : Math.max(throughput / numThreads, 1);
            for (int i = 0; i < numThreads; i++) {
                // If number of records cannot be exactly divided by num threads, some threads need to send one more record.
                int numRecordsAdjustment = i < numRecords % numThreads ? 1 : 0;
                producerPerformanceThreads[i] = new ProducerPerformanceThread(producer, topicName,
                        numRecordsPerThread + numRecordsAdjustment, recordSize, throughputPerThread, valueBound,
                        payloadByteList, stats.threadStats(i));
            }
            for (ProducerPerformanceThread t : producerPerformanceThreads)
                t.start();
            for (ProducerPerformanceThread t : producerPerformanceThreads)
                t.join();
            producer.flush();
            /* print final results */
            stats.printTotal();
            if (shouldPrintMetrics)
                ToolsUtils.printMetrics(producer.metrics());
            producer.close();
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

        parser.addArgument("--num-threads")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("NUM-THREADS")
                .dest("numThreads")
                .setDefault(1)
                .help("The number of producing threads.");

        parser.addArgument("--value-bound")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("VALUE-BOUND")
                .setDefault(-1)
                .dest("valueBound")
                .help("The value bound of the random integers in message payload to simulate different compression ratio. " +
                        "A non-positive value means the producer will send random bytes only containing A-Z.");

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

        return parser;
    }

    private static class Stats {
        private static final long MAX_LATENCY_SAMPLES = 500000;
        private final ThreadStats[] threadStatsArr;
        private final long start;

        Stats(long numRecords, int reportingInterval, int numThreads) {
            int numRecordsPerThread = (int) numRecords / numThreads;
            int sampling = (int) (numRecordsPerThread / Math.min(numRecordsPerThread, MAX_LATENCY_SAMPLES / numThreads));
            threadStatsArr = new ThreadStats[numThreads];
            for (int i = 0; i < numThreads; i++) {
                // If number of records cannot be exactly divided by num threads, some threads need to send one more
                // record.
                int numRecordsAdjustment = i < numRecords % numThreads ? 1 : 0;
                threadStatsArr[i] = new ThreadStats(i, numRecordsPerThread + numRecordsAdjustment, reportingInterval,
                                                    sampling, numThreads != 1);
            }
            this.start = System.currentTimeMillis();
        }

        ThreadStats threadStats(int i) {
            return threadStatsArr[i];
        }

        void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            long totalCount = 0L;
            long totalBytes = 0L;
            long maxLatency = 0L;
            long totalLatency = 0L;
            int totalIndex = 0;
            for (ThreadStats threadStats : threadStatsArr) {
                totalCount += threadStats.count;
                totalBytes += threadStats.bytes;
                maxLatency = Math.max(maxLatency, threadStats.maxLatency);
                totalLatency += threadStats.totalLatency;
                totalIndex += threadStats.index;
            }
            int[] totalLatencies = new int[totalIndex];
            int index = 0;
            for (ThreadStats threadStats : threadStatsArr) {
                for (int i = 0; i < threadStats.index; i++)
                    totalLatencies[index++] = threadStats.latencies[i];
            }
            double recsPerSec = 1000.0 * totalCount / (double) elapsed;
            double mbPerSec = 1000.0 * totalBytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(totalLatencies, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %2f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              totalCount,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) totalCount,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, double... percentiles) {
            Arrays.sort(latencies, 0, latencies.length);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * latencies.length);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static class ThreadStats {
        private final boolean logThreadId;
        private final int threadId;
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

        ThreadStats(int threadId, long numRecords, int reportingInterval, int sampling, boolean logThreadId) {
            this.threadId = threadId;
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = sampling;
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            this.logThreadId = logThreadId;
        }

        public void record(int iter, int latency, int bytes, long time) {
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

        Callback nextCompletion(long start, int bytes, ThreadStats threadstats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, threadstats);
            this.iteration++;
            return cb;
        }

        void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%s%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.%n",
                    logThreadId ? String.format("[Thread-%d] ", threadId) : "",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }
    }

    private static final class ProducerPerformanceThread extends Thread {
        private final Producer<byte[], byte[]> producer;
        private final String topicName;
        private final long numRecords;
        private final int recordSize;
        private final int throughput;
        private final int valueBound;
        private final List<byte[]> payloadByteList;
        private final ThreadStats threadStats;
        private final Random random = new Random();

        ProducerPerformanceThread(Producer<byte[], byte[]> producer,
                                  String topicName,
                                  long numRecords,
                                  int recordSize,
                                  int throughput,
                                  int valueBound,
                                  List<byte[]> payloadByteList,
                                  ThreadStats threadStats) {
            this.producer = producer;
            this.topicName = topicName;
            this.numRecords = numRecords;
            this.recordSize = recordSize;
            this.throughput = throughput;
            this.valueBound = valueBound;
            this.payloadByteList = payloadByteList;
            this.threadStats = threadStats;
        }

        @Override
        public void run() {

            long startMs = System.currentTimeMillis();
            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            for (int i = 0; i < numRecords; i++) {
                byte[] payload = payloadByteList == null || payloadByteList.isEmpty() ?
                    newPayload() : payloadByteList.get(random.nextInt(payloadByteList.size()));
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, payload);
                long sendStartMs = System.currentTimeMillis();
                Callback cb = threadStats.nextCompletion(sendStartMs, payload.length, threadStats);
                producer.send(record, cb);

                if (throttler.shouldThrottle(i, sendStartMs))
                    throttler.throttle();
            }
        }

        private byte[] newPayload() {
            byte[] payload = new byte[recordSize];
            if (valueBound > 0) {
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                while (buffer.position() < buffer.limit() - 4) {
                    buffer.putInt(random.nextInt(valueBound));
                }
            } else {
                for (int i = 0; i < payload.length; ++i)
                    payload[i] = (byte) (random.nextInt(26) + 65);
            }
            return payload;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final ThreadStats stats;

        PerfCallback(int iter, long start, int bytes, ThreadStats stats) {
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
