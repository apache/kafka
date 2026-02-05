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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
@Tag("integration")
public class LogCleanerLagIntegrationTest extends AbstractLogCleanerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(LogCleanerLagIntegrationTest.class);

    private static final int MS_PER_HOUR = 60 * 60 * 1000;
    private static final int MIN_COMPACTION_LAG = MS_PER_HOUR; // 1 hour

    static {
        // compactionLag must be divisible by 2 for this test
        assertTrue(MIN_COMPACTION_LAG % 2 == 0, "compactionLag must be divisible by 2 for this test");
    }

    private final MockTime time = new MockTime(1400000000000L, 1000L);  // Tue May 13 16:53:20 UTC 2014
    private static final long CLEANER_BACKOFF_MS = 200L;
    private static final int SEGMENT_SIZE = 512;

    private static final List<TopicPartition> TOPIC_PARTITIONS = Arrays.asList(
        new TopicPartition("log", 0),
        new TopicPartition("log", 1),
        new TopicPartition("log", 2)
    );

    @Override
    protected MockTime time() {
        return time;
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void cleanerTest(CompressionType compressionType) throws IOException, InterruptedException {
        Compression codec = Compression.of(compressionType).build();
        cleaner = makeCleaner(TOPIC_PARTITIONS,
            CLEANER_BACKOFF_MS,
            MIN_COMPACTION_LAG,
            SEGMENT_SIZE);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        // t = T0
        long t0 = time.milliseconds();
        Map<Integer, Integer> appends0 = writeDupsWithTimestamp(100, 3, theLog, codec, t0);
        long startSizeBlock0 = theLog.size();
        log.debug("total log size at T0: {}", startSizeBlock0);

        LogSegment activeSegAtT0 = theLog.activeSegment();
        log.debug("active segment at T0 has base offset: {}", activeSegAtT0.baseOffset());
        long sizeUpToActiveSegmentAtT0 = calculateSizeUpToOffset(theLog, activeSegAtT0.baseOffset());
        log.debug("log size up to base offset of active segment at T0: {}", sizeUpToActiveSegmentAtT0);

        cleaner.startup();

        // T0 < t < T1
        // advance to a time still less than one compaction lag from start
        time.sleep(MIN_COMPACTION_LAG / 2);
        Thread.sleep(5 * CLEANER_BACKOFF_MS); // give cleaning thread a chance to _not_ clean
        assertEquals(startSizeBlock0, theLog.size(), "There should be no cleaning until the compaction lag has passed");

        // t = T1 > T0 + compactionLag
        // advance to time a bit more than one compaction lag from start
        time.sleep(MIN_COMPACTION_LAG / 2 + 1);
        long t1 = time.milliseconds();

        // write another block of data
        Map<Integer, Integer> appends1 = new HashMap<>(appends0);
        appends1.putAll(writeDupsWithTimestamp(100, 3, theLog, codec, t1));
        long firstBlock1SegmentBaseOffset = activeSegAtT0.baseOffset();

        // the first block should get cleaned
        cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT0.baseOffset(), 60000L);

        // check the data is the same
        Map<Integer, Integer> read1 = readFromLog(theLog);
        assertEquals(appends1, read1, "Contents of the map shouldn't change.");

        long compactedSize = calculateSizeUpToOffset(theLog, activeSegAtT0.baseOffset());
        log.debug("after cleaning the compacted size up to active segment at T0: {}", compactedSize);
        Long lastCleaned = cleaner.cleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        assertTrue(lastCleaned >= firstBlock1SegmentBaseOffset,
            String.format("log cleaner should have processed up to offset %d, but lastCleaned=%d",
                firstBlock1SegmentBaseOffset, lastCleaned));
        assertTrue(sizeUpToActiveSegmentAtT0 > compactedSize,
            String.format("log should have been compacted: size up to offset of active segment at T0=%d compacted size=%d",
                sizeUpToActiveSegmentAtT0, compactedSize));
    }

    private long calculateSizeUpToOffset(UnifiedLog log, long offset) {
        long size = 0;
        for (LogSegment segment : log.logSegments(0L, offset)) {
            size += segment.size();
        }
        return size;
    }

    private Map<Integer, Integer> readFromLog(UnifiedLog log) {
        Map<Integer, Integer> result = new HashMap<>();
        for (LogSegment segment : log.logSegments()) {
            for (Record record : segment.log().records()) {
                int key = Integer.parseInt(LogTestUtils.readString(record.key()));
                int value = Integer.parseInt(LogTestUtils.readString(record.value()));
                result.put(key, value);
            }
        }
        return result;
    }

    private Map<Integer, Integer> writeDupsWithTimestamp(int numKeys, int numDups, UnifiedLog log,
                                                          Compression codec, long timestamp) throws IOException {
        Map<Integer, Integer> result = new HashMap<>();
        for (int i = 0; i < numDups; i++) {
            for (int key = 0; key < numKeys; key++) {
                int count = counter();
                log.appendAsLeader(
                    LogTestUtils.singletonRecords(
                        String.valueOf(count).getBytes(),
                        codec,
                        String.valueOf(key).getBytes(),
                        timestamp),
                    0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                incCounter();
                result.put(key, count);
            }
        }
        return result;
    }
}
