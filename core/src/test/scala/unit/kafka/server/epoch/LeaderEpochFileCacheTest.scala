/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server.epoch

import java.io.File

import scala.collection.Seq
import scala.collection.mutable.ListBuffer

import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import kafka.utils.TestUtils
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
  * Unit test for the LeaderEpochFileCache.
  */
class LeaderEpochFileCacheTest {
  val tp = new TopicPartition("TestTopic", 5)
  private val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }
  private val cache = new LeaderEpochFileCache(tp, checkpoint)

  @Test
  def testPreviousEpoch(): Unit = {
    assertEquals(None, cache.previousEpoch)

    cache.assign(epoch = 2, startOffset = 10)
    assertEquals(None, cache.previousEpoch)

    cache.assign(epoch = 4, startOffset = 15)
    assertEquals(Some(2), cache.previousEpoch)

    cache.assign(epoch = 10, startOffset = 20)
    assertEquals(Some(4), cache.previousEpoch)

    cache.truncateFromEnd(18)
    assertEquals(Some(2), cache.previousEpoch)
  }

  @Test
  def shouldAddEpochAndMessageOffsetToCache() = {
    //When
    cache.assign(epoch = 2, startOffset = 10)
    val logEndOffset = 11

    //Then
    assertEquals(Some(2), cache.latestEpoch)
    assertEquals(EpochEntry(2, 10), cache.epochEntries(0))
    assertEquals((2, logEndOffset), cache.endOffsetFor(2, logEndOffset)) //should match logEndOffset
  }

  @Test
  def shouldReturnLogEndOffsetIfLatestEpochRequested() = {
    //When just one epoch
    cache.assign(epoch = 2, startOffset = 11)
    cache.assign(epoch = 2, startOffset = 12)
    val logEndOffset = 14

    //Then
    assertEquals((2, logEndOffset), cache.endOffsetFor(2, logEndOffset))
  }

  @Test
  def shouldReturnUndefinedOffsetIfUndefinedEpochRequested() = {
    val expectedEpochEndOffset = (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

    // assign couple of epochs
    cache.assign(epoch = 2, startOffset = 11)
    cache.assign(epoch = 3, startOffset = 12)

    //When (say a bootstraping follower) sends request for UNDEFINED_EPOCH
    val epochAndOffsetFor = cache.endOffsetFor(UNDEFINED_EPOCH, 0L)

    //Then
    assertEquals(expectedEpochEndOffset,
                 epochAndOffsetFor, "Expected undefined epoch and offset if undefined epoch requested. Cache not empty.")
  }

  @Test
  def shouldNotOverwriteLogEndOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    //Given
    val logEndOffset = 9

    cache.assign(2, logEndOffset)

    //When called again later
    cache.assign(2, 10)

    //Then the offset should NOT have been updated
    assertEquals(logEndOffset, cache.epochEntries(0).startOffset)
    assertEquals(ListBuffer(EpochEntry(2, 9)), cache.epochEntries)
  }

  @Test
  def shouldEnforceMonotonicallyIncreasingStartOffsets() = {
    //Given
    cache.assign(2, 9)

    //When update epoch new epoch but same offset
    cache.assign(3, 9)

    //Then epoch should have been updated
    assertEquals(ListBuffer(EpochEntry(3, 9)), cache.epochEntries)
  }

  @Test
  def shouldNotOverwriteOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    cache.assign(2, 6)

    //When called again later with a greater offset
    cache.assign(2, 10)

    //Then later update should have been ignored
    assertEquals(6, cache.epochEntries(0).startOffset)
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecorded(): Unit = {
    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(0, 0L))
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecordedAndUndefinedEpochRequested(): Unit = {
    //When (say a follower on older message format version) sends request for UNDEFINED_EPOCH
    val offsetFor = cache.endOffsetFor(UNDEFINED_EPOCH, 73)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
                 offsetFor, "Expected undefined epoch and offset if undefined epoch requested. Empty cache.")
  }

  @Test
  def shouldReturnFirstEpochIfRequestedEpochLessThanFirstEpoch(): Unit = {
    cache.assign(epoch = 5, startOffset = 11)
    cache.assign(epoch = 6, startOffset = 12)
    cache.assign(epoch = 7, startOffset = 13)

    //When
    val epochAndOffset = cache.endOffsetFor(4, 0L)

    //Then
    assertEquals((4, 11), epochAndOffset)
  }

  @Test
  def shouldTruncateIfMatchingEpochButEarlierStartingOffset(): Unit = {
    cache.assign(epoch = 5, startOffset = 11)
    cache.assign(epoch = 6, startOffset = 12)
    cache.assign(epoch = 7, startOffset = 13)

    // epoch 7 starts at an earlier offset
    cache.assign(epoch = 7, startOffset = 12)

    assertEquals((5, 12), cache.endOffsetFor(5, 0L))
    assertEquals((5, 12), cache.endOffsetFor(6, 0L))
  }

  @Test
  def shouldGetFirstOffsetOfSubsequentEpochWhenOffsetRequestedForPreviousEpoch() = {
    //When several epochs
    cache.assign(epoch = 1, startOffset = 11)
    cache.assign(epoch = 1, startOffset = 12)
    cache.assign(epoch = 2, startOffset = 13)
    cache.assign(epoch = 2, startOffset = 14)
    cache.assign(epoch = 3, startOffset = 15)
    cache.assign(epoch = 3, startOffset = 16)

    //Then get the start offset of the next epoch
    assertEquals((2, 15), cache.endOffsetFor(2, 17))
  }

  @Test
  def shouldReturnNextAvailableEpochIfThereIsNoExactEpochForTheOneRequested(): Unit = {
    //When
    cache.assign(epoch = 0, startOffset = 10)
    cache.assign(epoch = 2, startOffset = 13)
    cache.assign(epoch = 4, startOffset = 17)

    //Then
    assertEquals((0, 13), cache.endOffsetFor(1, 0L))
    assertEquals((2, 17), cache.endOffsetFor(2, 0L))
    assertEquals((2, 17), cache.endOffsetFor(3, 0L))
  }

  @Test
  def shouldNotUpdateEpochAndStartOffsetIfItDidNotChange() = {
    //When
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 2, startOffset = 7)

    //Then
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries.toList(0))
  }

  @Test
  def shouldReturnInvalidOffsetIfEpochIsRequestedWhichIsNotCurrentlyTracked(): Unit = {
    //When
    cache.assign(epoch = 2, startOffset = 100)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(3, 100))
  }

  @Test
  def shouldSupportEpochsThatDoNotStartFromZero(): Unit = {
    //When
    cache.assign(epoch = 2, startOffset = 6)
    val logEndOffset = 7

    //Then
    assertEquals((2, logEndOffset), cache.endOffsetFor(2, logEndOffset))
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries(0))
  }

  @Test
  def shouldPersistEpochsBetweenInstances(): Unit = {
    val checkpointPath = TestUtils.tempFile().getAbsolutePath
    val checkpoint = new LeaderEpochCheckpointFile(new File(checkpointPath))

    //Given
    val cache = new LeaderEpochFileCache(tp, checkpoint)
    cache.assign(epoch = 2, startOffset = 6)

    //When
    val checkpoint2 = new LeaderEpochCheckpointFile(new File(checkpointPath))
    val cache2 = new LeaderEpochFileCache(tp, checkpoint2)

    //Then
    assertEquals(1, cache2.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache2.epochEntries.toList(0))
  }

  @Test
  def shouldEnforceMonotonicallyIncreasingEpochs(): Unit = {
    //Given
    cache.assign(epoch = 1, startOffset = 5);
    var logEndOffset = 6
    cache.assign(epoch = 2, startOffset = 6);
    logEndOffset = 7

    //When we update an epoch in the past with a different offset, the log has already reached
    //an inconsistent state. Our options are either to raise an error, ignore the new append,
    //or truncate the cached epochs to the point of conflict. We take this latter approach in
    //order to guarantee that epochs and offsets in the cache increase monotonically, which makes
    //the search logic simpler to reason about.
    cache.assign(epoch = 1, startOffset = 7);
    logEndOffset = 8

    //Then later epochs will be removed
    assertEquals(Some(1), cache.latestEpoch)

    //Then end offset for epoch 1 will have changed
    assertEquals((1, 8), cache.endOffsetFor(1, logEndOffset))

    //Then end offset for epoch 2 is now undefined
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(2, logEndOffset))
    assertEquals(EpochEntry(1, 7), cache.epochEntries(0))
  }

  @Test
  def shouldEnforceOffsetsIncreaseMonotonically() = {
    //When epoch goes forward but offset goes backwards
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 5)

    //The last assignment wins and the conflicting one is removed from the log
    assertEquals(EpochEntry(3, 5), cache.epochEntries.toList(0))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsLeadersChangeManyTimes(): Unit = {
    var logEndOffset = 0L

    //Given
    cache.assign(epoch = 0, startOffset = 0) //logEndOffset=0

    //When
    cache.assign(epoch = 1, startOffset = 0) //logEndOffset=0

    //Then epoch should go up
    assertEquals(Some(1), cache.latestEpoch)
    //offset for 1 should still be 0
    assertEquals((1, 0), cache.endOffsetFor(1, logEndOffset))
    //offset for epoch 0 should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0, logEndOffset))

    //When we write 5 messages as epoch 1
    logEndOffset = 5L

    //Then end offset for epoch(1) should be logEndOffset => 5
    assertEquals((1, 5), cache.endOffsetFor(1, logEndOffset))
    //Epoch 0 should still be at offset 0
    assertEquals((0, 0), cache.endOffsetFor(0, logEndOffset))

    //When
    cache.assign(epoch = 2, startOffset = 5) //logEndOffset=5

    logEndOffset = 10 //write another 5 messages

    //Then end offset for epoch(2) should be logEndOffset => 10
    assertEquals((2, 10), cache.endOffsetFor(2, logEndOffset))

    //end offset for epoch(1) should be the start offset of epoch(2) => 5
    assertEquals((1, 5), cache.endOffsetFor(1, logEndOffset))

    //epoch (0) should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0, logEndOffset))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsFollowerReceivesManyMessages(): Unit = {
    //When Messages come in
    cache.assign(epoch = 0, startOffset = 0);
    var logEndOffset = 1
    cache.assign(epoch = 0, startOffset = 1);
    logEndOffset = 2
    cache.assign(epoch = 0, startOffset = 2);
    logEndOffset = 3

    //Then epoch should stay, offsets should grow
    assertEquals(Some(0), cache.latestEpoch)
    assertEquals((0, logEndOffset), cache.endOffsetFor(0, logEndOffset))

    //When messages arrive with greater epoch
    cache.assign(epoch = 1, startOffset = 3);
    logEndOffset = 4
    cache.assign(epoch = 1, startOffset = 4);
    logEndOffset = 5
    cache.assign(epoch = 1, startOffset = 5);
    logEndOffset = 6

    assertEquals(Some(1), cache.latestEpoch)
    assertEquals((1, logEndOffset), cache.endOffsetFor(1, logEndOffset))

    //When
    cache.assign(epoch = 2, startOffset = 6);
    logEndOffset = 7
    cache.assign(epoch = 2, startOffset = 7);
    logEndOffset = 8
    cache.assign(epoch = 2, startOffset = 8);
    logEndOffset = 9

    assertEquals(Some(2), cache.latestEpoch)
    assertEquals((2, logEndOffset), cache.endOffsetFor(2, logEndOffset))

    //Older epochs should return the start offset of the first message in the subsequent epoch.
    assertEquals((0, 3), cache.endOffsetFor(0, logEndOffset))
    assertEquals((1, 6), cache.endOffsetFor(1, logEndOffset))
  }

  @Test
  def shouldDropEntriesOnEpochBoundaryWhenRemovingLatestEntries(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When clear latest on epoch boundary
    cache.truncateFromEnd(endOffset = 8)

    //Then should remove two latest epochs (remove is inclusive)
    assertEquals(ListBuffer(EpochEntry(2, 6)), cache.epochEntries)
  }

  @Test
  def shouldPreserveResetOffsetOnClearEarliestIfOneExists(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset ON epoch boundary
    cache.truncateFromStart(startOffset = 8)

    //Then should preserve (3, 8)
    assertEquals(ListBuffer(EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateSavedOffsetWhenOffsetToClearToIsBetweenEpochs(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.truncateFromStart(startOffset = 9)

    //Then we should retain epoch 3, but update it's offset to 9 as 8 has been removed
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToEarly(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset before first epoch offset
    cache.truncateFromStart(startOffset = 1)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToFirstOffset(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on earliest epoch boundary
    cache.truncateFromStart(startOffset = 6)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When
    cache.truncateFromStart(startOffset = 11)

    //Then retain the last
    assertEquals(ListBuffer(EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When we clear from a position between offset 8 & offset 11
    cache.truncateFromStart(startOffset = 9)

    //Then we should update the middle epoch entry's offset
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest2(): Unit = {
    //Given
    cache.assign(epoch = 0, startOffset = 0)
    cache.assign(epoch = 1, startOffset = 7)
    cache.assign(epoch = 2, startOffset = 10)

    //When we clear from a position between offset 0 & offset 7
    cache.truncateFromStart(startOffset = 5)

    //Then we should keep epoch 0 but update the offset appropriately
    assertEquals(ListBuffer(EpochEntry(0,5), EpochEntry(1, 7), EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliestAndUpdateItsOffset(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset beyond last epoch
    cache.truncateFromStart(startOffset = 15)

    //Then update the last
    assertEquals(ListBuffer(EpochEntry(4, 15)), cache.epochEntries)
  }

  @Test
  def shouldDropEntriesBetweenEpochBoundaryWhenRemovingNewest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.truncateFromEnd(endOffset = 9)

    //Then should keep the preceding epochs
    assertEquals(Some(3), cache.latestEpoch)
    assertEquals(ListBuffer(EpochEntry(2, 6), EpochEntry(3, 8)), cache.epochEntries)
  }

  @Test
  def shouldClearAllEntries(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When
    cache.clearAndFlush()

    //Then
    assertEquals(0, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryHeadIfUndefinedPassed(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on epoch boundary
    cache.truncateFromStart(startOffset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryTailIfUndefinedPassed(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on epoch boundary
    cache.truncateFromEnd(endOffset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldFetchLatestEpochOfEmptyCache(): Unit = {
    //Then
    assertEquals(None, cache.latestEpoch)
  }

  @Test
  def shouldFetchEndOffsetOfEmptyCache(): Unit = {
    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(7, 0L))
  }

  @Test
  def shouldClearEarliestOnEmptyCache(): Unit = {
    //Then
    cache.truncateFromStart(7)
  }

  @Test
  def shouldClearLatestOnEmptyCache(): Unit = {
    //Then
    cache.truncateFromEnd(7)
  }
}
