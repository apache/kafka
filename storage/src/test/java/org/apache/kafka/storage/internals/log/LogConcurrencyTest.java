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

package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class LogConcurrencyTest {
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private final KafkaScheduler scheduler = new KafkaScheduler(1);
    private final File tmpDir = TestUtils.tempDirectory();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private UnifiedLog log;

    @BeforeEach
    public void setup() {
        scheduler.startup();
    }

    @AfterEach
    public void teardown() throws Exception {
        scheduler.shutdown();
        log.close();
        Utils.delete(tmpDir);
    }

    @Test
    public void testUncommittedDataNotConsumed() throws Exception {
        testUncommittedDataNotConsumed(createLog());
    }

    @Test
    public void testUncommittedDataNotConsumedFrequentSegmentRolls() throws Exception {
        final Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 237);
        final LogConfig logConfig = new LogConfig(logProps);
        testUncommittedDataNotConsumed(createLog(logConfig));
    }

    public void testUncommittedDataNotConsumed(UnifiedLog log) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final int maxOffset = 5000;
            final ConsumerTask consumer = new ConsumerTask(log, maxOffset);
            final LogAppendTask appendTask = new LogAppendTask(log, maxOffset);

            final Future<?> consumerFuture = executor.submit(consumer);
            final Future<?> fetcherTaskFuture = executor.submit(appendTask);

            fetcherTaskFuture.get();
            consumerFuture.get();

            validateConsumedData(log, consumer.getConsumedBatches());
        } finally {
            executor.shutdownNow();
        }
    }

    private UnifiedLog createLog() throws IOException {
        return createLog(new LogConfig(new Properties()));
    }

    private UnifiedLog createLog(LogConfig config) throws IOException {
        log = UnifiedLog.create(
            logDir,
            config,
            0L,
            0L,
            scheduler,
            brokerTopicStats,
            Time.SYSTEM,
            5 * 60 * 1000,
            new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
            new LogDirFailureChannel(10),
            true,
            Optional.empty()
        );

        return log;
    }

    private void validateConsumedData(UnifiedLog log, List<FetchedBatch> consumedBatches) {
        final Iterator<FetchedBatch> iter = consumedBatches.iterator();
        log.logSegments().forEach(segment ->
            segment.log().batches().forEach(batch -> {
                if (iter.hasNext()) {
                    final FetchedBatch consumedBatch = iter.next();
                    try {
                        assertEquals(batch.partitionLeaderEpoch(),
                            consumedBatch.epoch(), "Consumed batch with unexpected leader epoch");
                        assertEquals(batch.baseOffset(),
                            consumedBatch.baseOffset(), "Consumed batch with unexpected base offset");
                    } catch (Throwable t) {
                        fail("Consumed batch " + consumedBatch +
                            " does not match next expected batch in log " + batch, t);
                    }
                }
            })
        );
    }

    /**
     * Simple consumption task which reads the log in ascending order and collects
     * consumed batches for validation
     */
    private static class ConsumerTask implements Callable<Void> {
        private final UnifiedLog log;
        private final int lastOffset;
        private final List<FetchedBatch> consumedBatches = new ArrayList<>();

        public ConsumerTask(UnifiedLog log, int lastOffset) {
            this.log = log;
            this.lastOffset = lastOffset;
        }

        public List<FetchedBatch> getConsumedBatches() {
            return consumedBatches;
        }

        @Override
        public Void call() throws Exception {
            long fetchOffset = 0L;
            while (log.highWatermark() < lastOffset) {
                final FetchDataInfo readInfo = log.read(fetchOffset, 1, FetchIsolation.HIGH_WATERMARK, true);
                for (RecordBatch batch : readInfo.records.batches()) {
                    consumedBatches.add(new FetchedBatch(batch.baseOffset(), batch.partitionLeaderEpoch()));
                    fetchOffset = batch.lastOffset() + 1;
                }
            }
            return null;
        }
    }

    /**
     * This class simulates basic leader/follower behavior.
     */
    private record LogAppendTask(UnifiedLog log, long lastOffset) implements Callable<Void> {

        @Override
        public Void call() throws Exception {
            int leaderEpoch = 1;
            boolean isLeader = true;
            while (log.highWatermark() < lastOffset) {
                switch (TestUtils.RANDOM.nextInt(2)) {
                    case 0 -> {
                        final LogOffsetMetadata logEndOffsetMetadata = log.logEndOffsetMetadata();
                        final long logEndOffset = logEndOffsetMetadata.messageOffset;
                        final int batchSize = TestUtils.RANDOM.nextInt(9) + 1;
                        final SimpleRecord[] records = IntStream.rangeClosed(0, batchSize)
                            .mapToObj(i -> new SimpleRecord(String.valueOf(i).getBytes()))
                            .toArray(SimpleRecord[]::new);

                        if (isLeader) {
                            log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records), leaderEpoch);
                            log.maybeIncrementHighWatermark(logEndOffsetMetadata);
                        } else {
                            log.appendAsFollower(
                                MemoryRecords.withRecords(
                                    logEndOffset,
                                    Compression.NONE,
                                    leaderEpoch,
                                    records
                                ),
                                Integer.MAX_VALUE
                            );
                            log.updateHighWatermark(logEndOffset);
                        }
                    }
                    case 1 -> {
                        isLeader = !isLeader;
                        leaderEpoch += 1;

                        if (!isLeader) {
                            log.truncateTo(log.highWatermark());
                        }
                    }
                }
            }
            return null;
        }
    }

    private record FetchedBatch(long baseOffset, int epoch) {
        @Override
        public String toString() {
            return "FetchedBatch(" +
                "baseOffset=" + baseOffset +
                ", epoch=" + epoch +
                ")";
        }
    }
}
