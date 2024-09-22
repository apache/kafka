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
import kafka.utils.TestUtils;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;


@State(Scope.Benchmark)
public class StressTestLog {
    private WriterThread writer;
    private ReaderThread reader;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private File dir;

    @Setup(Level.Trial)
    public void setup() throws InterruptedException {
        dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir());
        MockTime time = new MockTime();
        Properties logProperties = new Properties();
        logProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, 64 * 1024 * 1024);
        logProperties.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.MAX_VALUE);
        logProperties.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1024 * 1024);

        int fiveMinutesInMillis = (int) Duration.ofMinutes(5).toMillis();
        UnifiedLog log = UnifiedLog.apply(
                dir,
                new LogConfig(logProperties),
                0L,
                0L,
                time.scheduler,
                new BrokerTopicStats(),
                time,
                fiveMinutesInMillis,
                new ProducerStateManagerConfig(600000, false),
                fiveMinutesInMillis,
                new LogDirFailureChannel(10),
                true,
                Option.empty(),
                true,
                new ConcurrentHashMap<>(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER
        );

        writer = new WriterThread(log);
        reader = new ReaderThread(log);

        Exit.addShutdownHook("strees-test-shudtodwn-hook", () -> {
            try {
                running.set(false);
                writer.join();
                reader.join();
                Utils.delete(dir);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void stressTest() throws InterruptedException {
        writer.start();
        reader.start();
        while (running.get()) {
            Thread.sleep(1000);
            System.out.printf("Reader offset = %d, writer offset = %d%n", reader.getCurrentOffset(), writer.getCurrentOffset());
            writer.checkProgress();
            reader.checkProgress();
        }
    }

    abstract class WorkerThread extends Thread {
        protected final UnifiedLog log;
        protected int currentOffset = 0;
        protected int lastOffsetCheckpointed = currentOffset;
        protected long lastProgressCheckTime = System.currentTimeMillis();

        public WorkerThread(UnifiedLog log) {
            this.log = log;
        }

        public abstract void work() throws InterruptedException;

        @Override
        public void run() {
            try {
                while (running.get()) {
                    work();
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            } finally {
                running.set(false);
            }
        }

        public boolean isMakingProgress() {
            if (currentOffset > lastOffsetCheckpointed) {
                lastOffsetCheckpointed = currentOffset;
                return true;
            }
            return false;
        }

        public void checkProgress() {
            long curTime = System.currentTimeMillis();
            if (curTime - lastProgressCheckTime > 500) {
                assert isMakingProgress() : "Thread not making progress";
                lastProgressCheckTime = curTime;
            }
        }

        public int getCurrentOffset() {
            return currentOffset;
        }
    }

    class WriterThread extends WorkerThread {
        WriterThread(UnifiedLog log) {
            super(log);
        }

        @Override
        public void work() throws InterruptedException {
            MemoryRecords records = TestUtils.singletonRecords(Integer.toString(currentOffset).getBytes(StandardCharsets.UTF_8),
                    null,
                    Compression.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE);
            LogAppendInfo logAppendInfo = log.appendAsLeader(records,
                    0,
                    AppendOrigin.CLIENT,
                    MetadataVersion.LATEST_PRODUCTION,
                    RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL);
            assert (logAppendInfo.firstOffset() == -1 || logAppendInfo.firstOffset() == currentOffset) && logAppendInfo.lastOffset() == currentOffset;
            currentOffset++;
            if (currentOffset % 1000 == 0) {
                Thread.sleep(50);
            }
        }
    }

    class ReaderThread extends WorkerThread {

        ReaderThread(UnifiedLog log) {
            super(log);
        }

        @Override
        public void work() {
            Records read = log.read(currentOffset, 1, FetchIsolation.LOG_END, true).records;
            if (read instanceof FileRecords && read.sizeInBytes() > 0) {
                FileLogInputStream.FileChannelRecordBatch first = ((FileRecords) read).batches().iterator().next();
                assert first.lastOffset() == currentOffset : "We should either read nothing or the message we asked for.";
                assert first.sizeInBytes() == read.sizeInBytes() : String.format("Expected %d but got %d.", first.sizeInBytes(), read.sizeInBytes());
                currentOffset++;
            }
        }
    }
}
