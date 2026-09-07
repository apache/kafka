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
package org.apache.kafka.storage.internals.epoch;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for the LeaderEpochFileCache.
 */
public class LeaderEpochFileCacheTest {
    private final TopicPartition tp = new TopicPartition("TestTopic", 5);

    private LeaderEpochCheckpointFile checkpoint;
    private LeaderEpochFileCache cache;

    @BeforeEach
    public void setup() throws IOException {
        MockTime mockTime = new MockTime();
        checkpoint = new LeaderEpochCheckpointFile(TestUtils.tempFile(), new LogDirFailureChannel(1));
        cache = new LeaderEpochFileCache(tp, checkpoint, mockTime.scheduler);
    }

    @Test
    public void testPreviousEpoch() {
        assertEquals(OptionalInt.empty(), cache.previousEpoch());

        cache.assign(2, 10);
        assertEquals(OptionalInt.empty(), cache.previousEpoch());

        cache.assign(4, 15);
        assertEquals(OptionalInt.of(2), cache.previousEpoch());

        cache.assign(10, 20);
        assertEquals(OptionalInt.of(4), cache.previousEpoch());

        cache.truncateFromEndAsyncFlush(18);
        assertEquals(OptionalInt.of(2), cache.previousEpoch());
    }

    @Test
    public void shouldAddEpochAndMessageOffsetToCache() {
        cache.assign(2, 10);
        long logEndOffset = 11;

        assertEquals(Optional.of(2), cache.latestEpoch());
        assertEquals(new EpochEntry(2, 10), cache.epochEntries().get(0));
        assertEquals(Map.entry(2, logEndOffset), cache.endOffsetFor(2, logEndOffset));
    }

    @Test
    public void shouldReturnLogEndOffsetIfLatestEpochRequested() {
        cache.assign(2, 11);
        cache.assign(2, 12);
        long logEndOffset = 14;

        assertEquals(Map.entry(2, logEndOffset), cache.endOffsetFor(2, logEndOffset));
    }

    @Test
    public void shouldReturnUndefinedOffsetIfUndefinedEpochRequested() {
        Map.Entry<Integer, Long> expectedEpochEndOffset = Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);

        cache.assign(2, 11);
        cache.assign(3, 12);

        Map.Entry<Integer, Long> epochAndOffsetFor = cache.endOffsetFor(UNDEFINED_EPOCH, 0L);

        assertEquals(
                expectedEpochEndOffset,
                epochAndOffsetFor,
                "Expected undefined epoch and offset if undefined epoch requested. Cache not empty.");
    }

    @Test
    public void shouldEnforceMonotonicallyIncreasingStartOffsets() {
        cache.assign(2, 9);

        cache.assign(3, 9);

        assertEquals(List.of(new EpochEntry(3, 9)), cache.epochEntries());
    }

    @Test
    public void shouldNotOverwriteOffsetForALeaderEpochOnceItHasBeenAssigned() {
        cache.assign(2, 6);

        cache.assign(2, 10);

        assertEquals(List.of(new EpochEntry(2, 6)), cache.epochEntries());
    }

    @Test
    public void shouldReturnUnsupportedIfNoEpochRecorded() {
        assertEquals(Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(0, 0L));
    }

    @Test
    public void shouldReturnUnsupportedIfNoEpochRecordedAndUndefinedEpochRequested() {
        Map.Entry<Integer, Long> offsetFor = cache.endOffsetFor(UNDEFINED_EPOCH, 73);

        assertEquals(
                Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
                offsetFor,
                "Expected undefined epoch and offset if undefined epoch requested. Empty cache.");
    }

    @Test
    public void shouldReturnFirstEpochIfRequestedEpochLessThanFirstEpoch() {
        cache.assign(5, 11);
        cache.assign(6, 12);
        cache.assign(7, 13);

        Map.Entry<Integer, Long> epochAndOffset = cache.endOffsetFor(4, 0L);

        assertEquals(Map.entry(4, 11L), epochAndOffset);
    }

    @Test
    public void shouldTruncateIfMatchingEpochButEarlierStartingOffset() {
        cache.assign(5, 11);
        cache.assign(6, 12);
        cache.assign(7, 13);

        cache.assign(7, 12);

        assertEquals(Map.entry(5, 12L), cache.endOffsetFor(5, 0L));
        assertEquals(Map.entry(5, 12L), cache.endOffsetFor(6, 0L));
    }

    @Test
    public void shouldGetFirstOffsetOfSubsequentEpochWhenOffsetRequestedForPreviousEpoch() {
        cache.assign(1, 11);
        cache.assign(1, 12);
        cache.assign(2, 13);
        cache.assign(2, 14);
        cache.assign(3, 15);
        cache.assign(3, 16);

        assertEquals(Map.entry(2, 15L), cache.endOffsetFor(2, 17));
    }

    @Test
    public void shouldReturnNextAvailableEpochIfThereIsNoExactEpochForTheOneRequested() {
        cache.assign(0, 10);
        cache.assign(2, 13);
        cache.assign(4, 17);

        assertEquals(Map.entry(0, 13L), cache.endOffsetFor(1, 0L));
        assertEquals(Map.entry(2, 17L), cache.endOffsetFor(2, 0L));
        assertEquals(Map.entry(2, 17L), cache.endOffsetFor(3, 0L));
    }

    @Test
    public void shouldNotUpdateEpochAndStartOffsetIfItDidNotChange() {
        cache.assign(2, 6);
        cache.assign(2, 7);

        assertEquals(1, cache.epochEntries().size());
        assertEquals(new EpochEntry(2, 6), cache.epochEntries().get(0));
    }

    @Test
    public void shouldReturnInvalidOffsetIfEpochIsRequestedWhichIsNotCurrentlyTracked() {
        cache.assign(2, 100);

        assertEquals(Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(3, 100));
    }

    @Test
    public void shouldSupportEpochsThatDoNotStartFromZero() {
        cache.assign(2, 6);
        long logEndOffset = 7;

        assertEquals(Map.entry(2, logEndOffset), cache.endOffsetFor(2, logEndOffset));
        assertEquals(1, cache.epochEntries().size());
        assertEquals(new EpochEntry(2, 6), cache.epochEntries().get(0));
    }

    @Test
    public void shouldPersistEpochsBetweenInstances() throws IOException {
        String checkpointPath = TestUtils.tempFile().getAbsolutePath();
        LeaderEpochCheckpointFile checkpoint = new LeaderEpochCheckpointFile(
                new File(checkpointPath),
                new LogDirFailureChannel(1));

        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, new MockTime().scheduler);
        cache.assign(2, 6);

        LeaderEpochCheckpointFile checkpoint2 = new LeaderEpochCheckpointFile(
                new File(checkpointPath),
                new LogDirFailureChannel(1));
        LeaderEpochFileCache cache2 = new LeaderEpochFileCache(tp, checkpoint2, new MockTime().scheduler);

        assertEquals(1, cache2.epochEntries().size());
        assertEquals(new EpochEntry(2, 6), cache2.epochEntries().get(0));
    }

    @Test
    public void shouldEnforceMonotonicallyIncreasingEpochs() {
        cache.assign(1, 5);
        cache.assign(2, 6);

        // When we update an epoch in the past with a different offset, the log has already reached
        // an inconsistent state. Our options are either to raise an error, ignore the new append,
        // or truncate the cached epochs to the point of conflict. We take this latter approach in
        // order to guarantee that epochs and offsets in the cache increase monotonically, which makes
        // the search logic simpler to reason about.
        cache.assign(1, 7);

        long logEndOffset = 8;

        assertEquals(Optional.of(1), cache.latestEpoch());
        assertEquals(Map.entry(1, logEndOffset), cache.endOffsetFor(1, logEndOffset));
        assertEquals(Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(2, logEndOffset));
        assertEquals(new EpochEntry(1, 7), cache.epochEntries().get(0));
    }

    @Test
    public void shouldEnforceOffsetsIncreaseMonotonically() {
        cache.assign(2, 6);
        cache.assign(3, 5);

        assertEquals(new EpochEntry(3, 5), cache.epochEntries().get(0));
    }

    @Test
    public void shouldIncreaseAndTrackEpochsAsLeadersChangeManyTimes() {
        long logEndOffset = 0L;

        cache.assign(0, 0);

        cache.assign(1, 0);

        assertEquals(Optional.of(1), cache.latestEpoch());
        assertEquals(Map.entry(1, 0L), cache.endOffsetFor(1, logEndOffset));
        assertEquals(Map.entry(0, 0L), cache.endOffsetFor(0, logEndOffset));

        logEndOffset = 5L;

        assertEquals(Map.entry(1, 5L), cache.endOffsetFor(1, logEndOffset));
        assertEquals(Map.entry(0, 0L), cache.endOffsetFor(0, logEndOffset));

        cache.assign(2, 5);

        logEndOffset = 10;

        assertEquals(Map.entry(2, 10L), cache.endOffsetFor(2, logEndOffset));
        assertEquals(Map.entry(1, 5L), cache.endOffsetFor(1, logEndOffset));
        assertEquals(Map.entry(0, 0L), cache.endOffsetFor(0, logEndOffset));
    }

    @Test
    public void shouldIncreaseAndTrackEpochsAsFollowerReceivesManyMessages() {
        cache.assign(0, 0);
        long logEndOffset = 1;
        cache.assign(0, 1);
        logEndOffset = 2;
        cache.assign(0, 2);
        logEndOffset = 3;

        assertEquals(Optional.of(0), cache.latestEpoch());
        assertEquals(Map.entry(0, logEndOffset), cache.endOffsetFor(0, logEndOffset));

        cache.assign(1, 3);
        logEndOffset = 4;
        cache.assign(1, 4);
        logEndOffset = 5;
        cache.assign(1, 5);
        logEndOffset = 6;

        assertEquals(Optional.of(1), cache.latestEpoch());
        assertEquals(Map.entry(1, logEndOffset), cache.endOffsetFor(1, logEndOffset));

        cache.assign(2, 6);
        logEndOffset = 7;
        cache.assign(2, 7);
        logEndOffset = 8;
        cache.assign(2, 8);
        logEndOffset = 9;

        assertEquals(Optional.of(2), cache.latestEpoch());
        assertEquals(Map.entry(2, logEndOffset), cache.endOffsetFor(2, logEndOffset));

        assertEquals(Map.entry(0, 3L), cache.endOffsetFor(0, logEndOffset));
        assertEquals(Map.entry(1, 6L), cache.endOffsetFor(1, logEndOffset));
    }

    @Test
    public void shouldDropEntriesOnEpochBoundaryWhenRemovingLatestEntries() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromEndAsyncFlush(8);

        assertEquals(List.of(new EpochEntry(2, 6)), cache.epochEntries());
    }

    @Test
    public void shouldPreserveResetOffsetOnClearEarliestIfOneExists() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(8);

        assertEquals(List.of(new EpochEntry(3, 8), new EpochEntry(4, 11)), cache.epochEntries());
    }

    @Test
    public void shouldNotClearAnythingIfOffsetTooEarly() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(1);

        assertEquals(List.of(new EpochEntry(2, 6), new EpochEntry(3, 8), new EpochEntry(4, 11)), cache.epochEntries());
    }

    @Test
    public void shouldNotClearAnythingIfOffsetIsFirstOffset() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(6);

        assertEquals(List.of(new EpochEntry(2, 6), new EpochEntry(3, 8), new EpochEntry(4, 11)), cache.epochEntries());
    }

    @Test
    public void shouldRetainLatestEpochOnClearAllEarliest() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(11);

        assertEquals(List.of(new EpochEntry(4, 11)), cache.epochEntries());
    }

    @Test
    public void shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(9);

        assertEquals(List.of(new EpochEntry(3, 9), new EpochEntry(4, 11)), cache.epochEntries());
    }

    @Test
    public void shouldUpdateOffsetBetweenFirstTwoEpochBoundariesOnClearEarliest() {
        cache.assign(0, 0);
        cache.assign(1, 7);
        cache.assign(2, 10);

        cache.truncateFromStartAsyncFlush(5);

        assertEquals(
                List.of(new EpochEntry(0, 5), new EpochEntry(1, 7), new EpochEntry(2, 10)),
                cache.epochEntries());
    }

    @Test
    public void shouldRetainLatestEpochOnClearAllEarliestAndUpdateItsOffset() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(15);

        assertEquals(List.of(new EpochEntry(4, 15)), cache.epochEntries());
    }

    @Test
    public void shouldDropEntriesBetweenEpochBoundaryWhenRemovingNewest() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromEndAsyncFlush(9);

        assertEquals(Optional.of(3), cache.latestEpoch());
        assertEquals(List.of(new EpochEntry(2, 6), new EpochEntry(3, 8)), cache.epochEntries());
    }

    @Test
    public void shouldClearAllEntries() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.clearAndFlush();

        assertEquals(0, cache.epochEntries().size());
    }

    @Test
    public void shouldNotResetEpochHistoryHeadIfUndefinedPassed() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromStartAsyncFlush(UNDEFINED_EPOCH_OFFSET);

        assertEquals(3, cache.epochEntries().size());
    }

    @Test
    public void shouldNotResetEpochHistoryTailIfUndefinedPassed() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromEndAsyncFlush(UNDEFINED_EPOCH_OFFSET);

        assertEquals(3, cache.epochEntries().size());
    }

    @Test
    public void shouldFetchLatestEpochOfEmptyCache() {
        assertEquals(Optional.empty(), cache.latestEpoch());
    }

    @Test
    public void shouldFetchEndOffsetOfEmptyCache() {
        assertEquals(Map.entry(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(7, 0L));
    }

    @Test
    public void shouldClearEarliestOnEmptyCache() {
        assertDoesNotThrow(() -> cache.truncateFromStartAsyncFlush(7));
    }

    @Test
    public void shouldClearLatestOnEmptyCache() {
        assertDoesNotThrow(() -> cache.truncateFromEndAsyncFlush(7));
    }

    @Test
    public void testFindPreviousEpoch() {
        assertEquals(OptionalInt.empty(), cache.previousEpoch(2));

        cache.assign(2, 10);
        assertEquals(OptionalInt.empty(), cache.previousEpoch(2));

        cache.assign(4, 15);
        assertEquals(OptionalInt.of(2), cache.previousEpoch(4));

        cache.assign(10, 20);
        assertEquals(OptionalInt.of(4), cache.previousEpoch(10));

        cache.truncateFromEndAsyncFlush(18);
        assertEquals(OptionalInt.of(2), cache.previousEpoch(cache.latestEpoch().orElseThrow()));
    }

    @Test
    public void testFindPreviousEntry() {
        assertEquals(Optional.empty(), cache.previousEntry(2));

        cache.assign(2, 10);
        assertEquals(Optional.empty(), cache.previousEntry(2));

        cache.assign(4, 15);
        assertEquals(Optional.of(new EpochEntry(2, 10)), cache.previousEntry(4));

        cache.assign(10, 20);
        assertEquals(Optional.of(new EpochEntry(4, 15)), cache.previousEntry(10));

        cache.truncateFromEndAsyncFlush(18);
        assertEquals(Optional.of(new EpochEntry(2, 10)), cache.previousEntry(cache.latestEpoch().orElseThrow()));
    }

    @Test
    public void testFindNextEpoch() {
        cache.assign(0, 0);
        cache.assign(1, 100);
        cache.assign(2, 200);

        assertEquals(OptionalInt.of(0), cache.nextEpoch(-1));
        assertEquals(OptionalInt.of(1), cache.nextEpoch(0));
        assertEquals(OptionalInt.of(2), cache.nextEpoch(1));
        assertEquals(OptionalInt.empty(), cache.nextEpoch(2));
        assertEquals(OptionalInt.empty(), cache.nextEpoch(100));
    }

    @Test
    public void testGetEpochEntry() {
        cache.assign(2, 100);
        cache.assign(3, 500);
        cache.assign(5, 1000);

        assertEquals(new EpochEntry(2, 100), cache.epochEntry(2).orElseThrow());
        assertEquals(new EpochEntry(3, 500), cache.epochEntry(3).orElseThrow());
        assertEquals(new EpochEntry(5, 1000), cache.epochEntry(5).orElseThrow());
    }

    @Test
    public void shouldFetchEpochForGivenOffset() {
        cache.assign(0, 10);
        cache.assign(1, 20);
        cache.assign(5, 30);

        assertEquals(OptionalInt.of(1), cache.epochForOffset(25));
        assertEquals(OptionalInt.of(1), cache.epochForOffset(20));
        assertEquals(OptionalInt.of(5), cache.epochForOffset(30));
        assertEquals(OptionalInt.of(5), cache.epochForOffset(50));
        assertEquals(OptionalInt.empty(), cache.epochForOffset(5));
    }

    @Test
    public void shouldWriteCheckpointOnTruncation() {
        cache.assign(2, 6);
        cache.assign(3, 8);
        cache.assign(4, 11);

        cache.truncateFromEndAsyncFlush(11);
        cache.truncateFromStartAsyncFlush(8);

        assertEquals(List.of(new EpochEntry(3, 8)), checkpoint.read());
    }
}
