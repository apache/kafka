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

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
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
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;


public class StressTestLog {
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {
        File dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir());
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

        WriterThread writer = new WriterThread(log);
        writer.start();
        ReaderThread reader = new ReaderThread(log);
        reader.start();

        Exit.addShutdownHook("strees-test-shudtodwn-hook", () -> {
            try {
                RUNNING.set(false);
                writer.join();
                reader.join();
                Utils.delete(dir);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        while (RUNNING.get()) {
            Thread.sleep(1000);
            System.out.printf("Reader offset = %d, writer offset = %d%n", reader.currentOffset, writer.currentOffset);
            writer.checkProgress();
            reader.checkProgress();
        }
    }

    abstract static class WorkerThread extends Thread {
        protected long currentOffset = 0;
        private long lastOffsetCheckpointed = currentOffset;
        private long lastProgressCheckTime = System.currentTimeMillis();

        @Override
        public void run() {
            try {
                while (StressTestLog.RUNNING.get()) {
                    work();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                StressTestLog.RUNNING.set(false);
            }
        }

        protected abstract void work() throws Exception;

        public boolean isMakingProgress() {
            if (currentOffset > lastOffsetCheckpointed) {
                lastOffsetCheckpointed = currentOffset;
                return true;
            }
            return false;
        }

        public void checkProgress() {
            long curTime = System.currentTimeMillis();
            if ((curTime - lastProgressCheckTime) > 500) {
                if (!isMakingProgress()) {
                    throw new RuntimeException("Thread not making progress");
                }
                lastProgressCheckTime = curTime;
            }
        }
    }

    static class WriterThread extends WorkerThread {
        private final UnifiedLog log;

        public WriterThread(UnifiedLog log) {
            this.log = log;
        }

        @Override
        protected void work() throws Exception {
            byte[] value = Long.toString(currentOffset).getBytes(StandardCharsets.UTF_8);
            MemoryRecords records = TestUtils.singletonRecords(value,
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

            if ((logAppendInfo.firstOffset() != -1 && logAppendInfo.firstOffset() != currentOffset)
                || logAppendInfo.lastOffset() != currentOffset) {
                throw new RuntimeException("Offsets do not match");
            }

            currentOffset++;
            if (currentOffset % 1000 == 0) {
                Thread.sleep(50);
            }
        }
    }

    static class ReaderThread extends WorkerThread {
        private final UnifiedLog log;

        public ReaderThread(UnifiedLog log) {
            this.log = log;
        }

        @Override
        protected void work() throws Exception {
            try {
                FetchDataInfo fetchDataInfo = log.read(
                    currentOffset,
                    1,
                    FetchIsolation.LOG_END,
                    true
                );

                Records records = fetchDataInfo.records;

                if (records instanceof FileRecords && records.sizeInBytes() > 0) {
                    FileLogInputStream.FileChannelRecordBatch first = ((FileRecords) records).batches().iterator().next();
                    if (first.lastOffset() != currentOffset) {
                        throw new RuntimeException("Expected to read the message at offset " + currentOffset);
                    }
                    if (first.sizeInBytes() != records.sizeInBytes()) {
                        throw new RuntimeException(String.format("Expected %d but got %d.", first.sizeInBytes(), records.sizeInBytes()));
                    }
                    currentOffset++;
                }
            } catch (OffsetOutOfRangeException e) {
                // this is ok
            }
        }
    }
}
