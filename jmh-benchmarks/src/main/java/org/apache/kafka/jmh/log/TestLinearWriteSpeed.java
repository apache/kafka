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
package org.apache.kafka.jmh.log;

import kafka.log.UnifiedLog;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.compress.GzipCompression;
import org.apache.kafka.common.compress.Lz4Compression;
import org.apache.kafka.common.compress.ZstdCompression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import scala.Option;

public class TestLinearWriteSpeed {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<String> dirOpt = parser.accepts("dir", "The directory to write to.")
            .withRequiredArg()
            .describedAs("path")
            .ofType(String.class)
            .defaultsTo(System.getProperty("java.io.tmpdir"));

        OptionSpec<Long> bytesOpt = parser.accepts("bytes", "REQUIRED: The total number of bytes to write.")
            .withRequiredArg()
            .describedAs("num_bytes")
            .ofType(Long.class);

        OptionSpec<Integer> sizeOpt = parser.accepts("size", "REQUIRED: The size of each write.")
            .withRequiredArg()
            .describedAs("num_bytes")
            .ofType(Integer.class);

        OptionSpec<Integer> messageSizeOpt = parser.accepts("message-size", "REQUIRED: The size of each message in the message set.")
            .withRequiredArg()
            .describedAs("num_bytes")
            .ofType(Integer.class)
            .defaultsTo(1024);

        OptionSpec<Integer> filesOpt = parser.accepts("files", "REQUIRED: The number of logs or files.")
            .withRequiredArg()
            .describedAs("num_files")
            .ofType(Integer.class)
            .defaultsTo(1);

        OptionSpec<Long> reportingIntervalOpt = parser.accepts("reporting-interval", "The number of ms between updates.")
            .withRequiredArg()
            .describedAs("ms")
            .ofType(Long.class)
            .defaultsTo(1000L);

        OptionSpec<Integer> maxThroughputOpt = parser.accepts("max-throughput-mb", "The maximum throughput.")
            .withRequiredArg()
            .describedAs("mb")
            .ofType(Integer.class)
            .defaultsTo(Integer.MAX_VALUE);

        OptionSpec<Long> flushIntervalOpt = parser.accepts("flush-interval", "The number of messages between flushes")
            .withRequiredArg()
            .describedAs("message_count")
            .ofType(Long.class)
            .defaultsTo(Long.MAX_VALUE);

        OptionSpec<String> compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
            .withRequiredArg()
            .describedAs("codec")
            .ofType(String.class)
            .defaultsTo(CompressionType.NONE.name);

        OptionSpec<Integer> compressionLevelOpt = parser.accepts("level", "The compression level to use")
            .withRequiredArg()
            .describedAs("level")
            .ofType(Integer.class)
            .defaultsTo(0);

        OptionSpec<Void> mmapOpt = parser.accepts("mmap", "Do writes to memory-mapped files.");
        OptionSpec<Void> channelOpt = parser.accepts("channel", "Do writes to file channels.");
        OptionSpec<Void> logOpt = parser.accepts("log", "Do writes to kafka logs.");
        OptionSet options = parser.parse(args);
        CommandLineUtils.checkRequiredArgs(parser, options, bytesOpt, sizeOpt, filesOpt);

        long bytesToWrite = options.valueOf(bytesOpt);
        int bufferSize = options.valueOf(sizeOpt);
        int numFiles = options.valueOf(filesOpt);
        long reportingInterval = options.valueOf(reportingIntervalOpt);
        String dir = options.valueOf(dirOpt);
        long maxThroughputBytes = options.valueOf(maxThroughputOpt) * 1024L * 1024L;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int messageSize = options.valueOf(messageSizeOpt);
        long flushInterval = options.valueOf(flushIntervalOpt);
        CompressionType compressionType = CompressionType.forName(options.valueOf(compressionCodecOpt));
        Compression.Builder<? extends Compression> compressionBuilder = Compression.of(compressionType);
        int compressionLevel = options.valueOf(compressionLevelOpt);

        setupCompression(compressionType, compressionBuilder, compressionLevel);

        ThreadLocalRandom.current().nextBytes(buffer.array());
        int numMessages = bufferSize / (messageSize + Records.LOG_OVERHEAD);
        long createTime = System.currentTimeMillis();

        List<SimpleRecord> recordsList = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            recordsList.add(new SimpleRecord(createTime, null, new byte[messageSize]));
        }

        MemoryRecords messageSet = MemoryRecords.withRecords(Compression.NONE, recordsList.toArray(new SimpleRecord[0]));
        Writable[] writables = new Writable[numFiles];
        KafkaScheduler scheduler = new KafkaScheduler(1);
        scheduler.startup();

        for (int i = 0; i < numFiles; i++) {
            if (options.has(mmapOpt)) {
                writables[i] = new MmapWritable(new File(dir, "kafka-test-" + i + ".dat"), bytesToWrite / numFiles, buffer);
            } else if (options.has(channelOpt)) {
                writables[i] = new ChannelWritable(new File(dir, "kafka-test-" + i + ".dat"), buffer);
            } else if (options.has(logOpt)) {
                int segmentSize = ThreadLocalRandom.current().nextInt(512) * 1024 * 1024 + 64 * 1024 * 1024;
                Properties logProperties = new Properties();
                logProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(segmentSize));
                logProperties.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, Long.toString(flushInterval));
                LogConfig logConfig = new LogConfig(logProperties);
                writables[i] = new LogWritable(new File(dir, "kafka-test-" + i), logConfig, scheduler, messageSet);
            } else {
                System.err.println("Must specify what to write to with one of --log, --channel, or --mmap");
                Exit.exit(1);
            }
        }
        bytesToWrite = (bytesToWrite / numFiles) * numFiles;

        System.out.printf("%10s\t%10s\t%10s%n", "mb_sec", "avg_latency", "max_latency");

        long beginTest = System.nanoTime();
        long maxLatency = 0L;
        long totalLatency = 0L;
        long count = 0L;
        long written = 0L;
        long totalWritten = 0L;
        long lastReport = beginTest;

        while (totalWritten + bufferSize < bytesToWrite) {
            long start = System.nanoTime();
            int writeSize = writables[(int) (count % numFiles)].write();
            long elapsed = System.nanoTime() - start;
            maxLatency = Math.max(elapsed, maxLatency);
            totalLatency += elapsed;
            written += writeSize;
            count += 1;
            totalWritten += writeSize;
            if ((start - lastReport) / (1000.0 * 1000.0) > reportingInterval) {
                double elapsedSecs = (start - lastReport) / (1000.0 * 1000.0 * 1000.0);
                double mb = written / (1024.0 * 1024.0);
                System.out.printf("%10.3f\t%10.3f\t%10.3f%n", mb / elapsedSecs, (totalLatency / (double) count) / (1000.0 * 1000.0), maxLatency / (1000.0 * 1000.0));
                lastReport = start;
                written = 0;
                maxLatency = 0L;
                totalLatency = 0L;
            } else if (written > maxThroughputBytes * (reportingInterval / 1000.0)) {
                long lastReportMs = lastReport / (1000 * 1000);
                long now = System.nanoTime() / (1000 * 1000);
                long sleepMs = lastReportMs + reportingInterval - now;
                if (sleepMs > 0)
                    Thread.sleep(sleepMs);
            }
        }
        double elapsedSecs = (System.nanoTime() - beginTest) / (1000.0 * 1000.0 * 1000.0);
        System.out.println((bytesToWrite / (1024.0 * 1024.0 * elapsedSecs)) + " MB per sec");
        scheduler.shutdown();
    }

    private static void setupCompression(CompressionType compressionType,
                                         Compression.Builder<? extends Compression> compressionBuilder,
                                         int compressionLevel) {
        switch (compressionType) {
            case GZIP:
                ((GzipCompression.Builder) compressionBuilder).level(compressionLevel);
                break;
            case LZ4:
                ((Lz4Compression.Builder) compressionBuilder).level(compressionLevel);
                break;
            case ZSTD:
                ((ZstdCompression.Builder) compressionBuilder).level(compressionLevel);
                break;
            default:
                break;
        }
    }

    interface Writable {
        int write() throws IOException;

        void close() throws IOException;
    }

    static class MmapWritable implements Writable {
        File file;
        ByteBuffer content;
        RandomAccessFile raf;
        MappedByteBuffer buffer;

        public MmapWritable(File file, long size, ByteBuffer content) throws IOException {
            this.file = file;
            this.content = content;
            file.deleteOnExit();
            raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
        }

        public int write() {
            buffer.put(content);
            content.rewind();
            return content.limit();
        }

        public void close() throws IOException {
            raf.close();
            Utils.delete(file);
        }
    }

    static class ChannelWritable implements Writable {
        File file;
        ByteBuffer content;
        FileChannel channel;

        public ChannelWritable(File file, ByteBuffer content) throws IOException {
            this.file = file;
            this.content = content;
            file.deleteOnExit();
            channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        public int write() throws IOException {
            channel.write(content);
            content.rewind();
            return content.limit();
        }

        public void close() throws IOException {
            channel.close();
            Utils.delete(file);
        }
    }

    static class LogWritable implements Writable {
        MemoryRecords messages;
        UnifiedLog log;

        public LogWritable(File dir, LogConfig config, Scheduler scheduler, MemoryRecords messages) throws IOException {
            this.messages = messages;
            Utils.delete(dir);
            this.log = UnifiedLog.apply(
                dir,
                config,
                0L,
                0L,
                scheduler,
                new BrokerTopicStats(),
                Time.SYSTEM,
                5 * 60 * 1000,
                new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                new LogDirFailureChannel(10),
                true,
                Option.empty(),
                true,
                new CopyOnWriteMap<>(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER
            );
        }

        public int write() {
            log.appendAsLeader(
                messages,
                0,
                AppendOrigin.CLIENT,
                MetadataVersion.latestProduction(),
                RequestLocal.noCaching(),
                VerificationGuard.SENTINEL
            );
            return messages.sizeInBytes();
        }

        public void close() throws IOException {
            log.close();
            Utils.delete(log.dir());
        }
    }
}
