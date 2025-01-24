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
package kafka.log.remote;

import kafka.utils.TestUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import scala.Option;

import static org.apache.kafka.common.record.FileRecords.TimestampAndOffset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteLogOffsetReaderTest {

    private final MockTime time = new MockTime();
    private final TopicPartition topicPartition = new TopicPartition("test", 0);
    private Path logDir;
    private LeaderEpochFileCache cache;
    private MockRemoteLogManager rlm;

    @BeforeEach
    void setUp() throws IOException {
        logDir = Files.createTempDirectory("kafka-test");
        LeaderEpochCheckpointFile checkpoint = new LeaderEpochCheckpointFile(TestUtils.tempFile(), new LogDirFailureChannel(1));
        cache = new LeaderEpochFileCache(topicPartition, checkpoint, time.scheduler);
        rlm = new MockRemoteLogManager(2, 1, logDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        rlm.close();
        Utils.delete(logDir.toFile());
    }

    @Test
    public void testReadRemoteLog() throws Exception {
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> asyncOffsetReadFutureHolder =
                rlm.asyncOffsetRead(topicPartition, time.milliseconds(), 0L, cache, Option::empty);
        asyncOffsetReadFutureHolder.taskFuture().get(1, TimeUnit.SECONDS);
        assertTrue(asyncOffsetReadFutureHolder.taskFuture().isDone());

        OffsetResultHolder.FileRecordsOrError result = asyncOffsetReadFutureHolder.taskFuture().get();
        assertFalse(result.hasException());
        assertTrue(result.hasTimestampAndOffset());
        assertEquals(new TimestampAndOffset(100L, 90L, Optional.of(3)),
                result.timestampAndOffset().get());
    }

    @Test
    public void testTaskQueueFullAndCancelTask() throws Exception {
        rlm.pause();

        List<AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError>> holderList = new ArrayList<>();
        // Task queue size is 1 and number of threads is 2, so it can accept at-most 3 items
        for (int i = 0; i < 3; i++) {
            holderList.add(rlm.asyncOffsetRead(topicPartition, time.milliseconds(), 0L, cache, Option::empty));
        }
        assertThrows(TimeoutException.class, () -> holderList.get(0).taskFuture().get(10, TimeUnit.MILLISECONDS));
        assertEquals(0, holderList.stream().filter(h -> h.taskFuture().isDone()).count());

        assertThrows(RejectedExecutionException.class, () ->
                holderList.add(rlm.asyncOffsetRead(topicPartition, time.milliseconds(), 0L, cache, Option::empty)));

        holderList.get(2).jobFuture().cancel(false);

        rlm.resume();
        for (AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> holder : holderList) {
            if (!holder.jobFuture().isCancelled()) {
                holder.taskFuture().get(1, TimeUnit.SECONDS);
            }
        }
        assertEquals(3, holderList.size());
        assertEquals(2, holderList.stream().filter(h -> h.taskFuture().isDone()).count());
        assertEquals(1, holderList.stream().filter(h -> !h.taskFuture().isDone()).count());
    }

    @Test
    public void testThrowErrorOnFindOffsetByTimestamp() throws Exception {
        RemoteStorageException exception = new RemoteStorageException("Error");
        try (RemoteLogManager rlm = new MockRemoteLogManager(2, 1, logDir.toString()) {
            @Override
            public Optional<TimestampAndOffset> findOffsetByTimestamp(TopicPartition tp,
                                                                      long timestamp,
                                                                      long startingOffset,
                                                                      LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException {
                throw exception;
            }
        }) {
            AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> futureHolder
                    = rlm.asyncOffsetRead(topicPartition, time.milliseconds(), 0L, cache, Option::empty);
            futureHolder.taskFuture().get(1, TimeUnit.SECONDS);

            assertTrue(futureHolder.taskFuture().isDone());
            assertTrue(futureHolder.taskFuture().get().hasException());
            assertEquals(exception, futureHolder.taskFuture().get().exception().get());
        }
    }

    private static class MockRemoteLogManager extends RemoteLogManager {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        public MockRemoteLogManager(int threads,
                                    int taskQueueSize,
                                    String logDir) throws IOException {
            super(rlmConfig(threads, taskQueueSize),
                    1,
                    logDir,
                    "mock-cluster-id",
                    new MockTime(),
                    tp -> Optional.empty(),
                    (tp, logStartOffset) -> { },
                    new BrokerTopicStats(true),
                    new Metrics()
            );
        }

        @Override
        public Optional<TimestampAndOffset> findOffsetByTimestamp(TopicPartition tp,
                                                                  long timestamp,
                                                                  long startingOffset,
                                                                  LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException {
            lock.readLock().lock();
            try {
                return Optional.of(new TimestampAndOffset(100, 90, Optional.of(3)));
            } finally {
                lock.readLock().unlock();
            }
        }

        void pause() {
            lock.writeLock().lock();
        }

        void resume() {
            lock.writeLock().unlock();
        }
    }

    private static RemoteLogManagerConfig rlmConfig(int threads, int taskQueueSize) {
        Properties props = new Properties();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, threads);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP, taskQueueSize);
        AbstractConfig config = new AbstractConfig(RemoteLogManagerConfig.configDef(), props, false);
        return new RemoteLogManagerConfig(config);
    }
}