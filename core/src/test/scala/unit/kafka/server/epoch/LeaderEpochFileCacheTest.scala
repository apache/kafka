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

import kafka.server.LogOffsetMetadata
import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import org.apache.kafka.common.requests.EpochEndOffset.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable.ListBuffer

/**
  * Unit test for the LeaderEpochFileCache.
  */
class LeaderEpochFileCacheTest {
  val tp = new TopicPartition("TestTopic", 5)
  var checkpoint: LeaderEpochCheckpoint = _

  @Test
  def shouldAddEpochAndMessageOffsetToCache() = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When
    cache.assign(epoch = 2, offset = 10)
    leo = 11

    //Then
    assertEquals(2, cache.latestEpoch())
    assertEquals(EpochEntry(2, 10), cache.epochEntries()(0))
    assertEquals((2, leo), cache.endOffsetFor(2)) //should match leo
  }

  @Test
  def shouldReturnLogEndOffsetIfLatestEpochRequested() = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When just one epoch
    cache.assign(epoch = 2, offset = 11)
    cache.assign(epoch = 2, offset = 12)
    leo = 14

    //Then
    assertEquals((2, leo), cache.endOffsetFor(2))
  }

  @Test
  def shouldReturnUndefinedOffsetIfUndefinedEpochRequested() = {
    def leoFinder() = new LogOffsetMetadata(0)
    val expectedEpochEndOffset = (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

    //Given cache with some data on leader
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    // assign couple of epochs
    cache.assign(epoch = 2, offset = 11)
    cache.assign(epoch = 3, offset = 12)

    //When (say a bootstraping follower) sends request for UNDEFINED_EPOCH
    val epochAndOffsetFor = cache.endOffsetFor(UNDEFINED_EPOCH)

    //Then
    assertEquals("Expected undefined epoch and offset if undefined epoch requested. Cache not empty.",
                 expectedEpochEndOffset, epochAndOffsetFor)
  }

  @Test
  def shouldNotOverwriteLogEndOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    leo = 9
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    cache.assign(2, leo)

    //When called again later
    cache.assign(2, 10)

    //Then the offset should NOT have been updated
    assertEquals(leo, cache.epochEntries()(0).startOffset)
  }

  @Test
  def shouldAllowLeaderEpochToChangeEvenIfOffsetDoesNot() = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(2, 9)

    //When update epoch new epoch but same offset
    cache.assign(3, 9)

    //Then epoch should have been updated
    assertEquals(ListBuffer(EpochEntry(2, 9), EpochEntry(3, 9)), cache.epochEntries())
  }
  
  @Test
  def shouldNotOverwriteOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    //Given
    val cache = new LeaderEpochFileCache(tp, () => new LogOffsetMetadata(0), checkpoint)
    cache.assign(2, 6)

    //When called again later with a greater offset
    cache.assign(2, 10)

    //Then later update should have been ignored
    assertEquals(6, cache.epochEntries()(0).startOffset)
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecorded(){
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(0))
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecordedAndUndefinedEpochRequested(){
    val leo = 73
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When (say a follower on older message format version) sends request for UNDEFINED_EPOCH
    val offsetFor = cache.endOffsetFor(UNDEFINED_EPOCH)

    //Then
    assertEquals("Expected undefined epoch and offset if undefined epoch requested. Empty cache.",
                 (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), offsetFor)
  }

  @Test
  def shouldReturnUnsupportedIfRequestedEpochLessThanFirstEpoch(){
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    cache.assign(epoch = 5, offset = 11)
    cache.assign(epoch = 6, offset = 12)
    cache.assign(epoch = 7, offset = 13)

    //When
    val epochAndOffset = cache.endOffsetFor(5 - 1)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), epochAndOffset)
  }

  @Test
  def shouldGetFirstOffsetOfSubsequentEpochWhenOffsetRequestedForPreviousEpoch() = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When several epochs
    cache.assign(epoch = 1, offset = 11)
    cache.assign(epoch = 1, offset = 12)
    cache.assign(epoch = 2, offset = 13)
    cache.assign(epoch = 2, offset = 14)
    cache.assign(epoch = 3, offset = 15)
    cache.assign(epoch = 3, offset = 16)
    leo = 17

    //Then get the start offset of the next epoch
    assertEquals((2, 15), cache.endOffsetFor(2))
  }

  @Test
  def shouldReturnNextAvailableEpochIfThereIsNoExactEpochForTheOneRequested(){
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When
    cache.assign(epoch = 0, offset = 10)
    cache.assign(epoch = 2, offset = 13)
    cache.assign(epoch = 4, offset = 17)

    //Then
    assertEquals((0, 13), cache.endOffsetFor(requestedEpoch = 1))
    assertEquals((2, 17), cache.endOffsetFor(requestedEpoch = 2))
    assertEquals((2, 17), cache.endOffsetFor(requestedEpoch = 3))
  }

  @Test
  def shouldNotUpdateEpochAndStartOffsetIfItDidNotChange() = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 2, offset = 7)

    //Then
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries.toList(0))
  }

  @Test
  def shouldReturnInvalidOffsetIfEpochIsRequestedWhichIsNotCurrentlyTracked(): Unit = {
    val leo = 100
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When
    cache.assign(epoch = 2, offset = 100)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(3))
  }

  @Test
  def shouldSupportEpochsThatDoNotStartFromZero(): Unit = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When
    cache.assign(epoch = 2, offset = 6)
    leo = 7

    //Then
    assertEquals((2, leo), cache.endOffsetFor(2))
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries()(0))
  }

  @Test
  def shouldPersistEpochsBetweenInstances(){
    def leoFinder() = new LogOffsetMetadata(0)
    val checkpointPath = TestUtils.tempFile().getAbsolutePath
    checkpoint = new LeaderEpochCheckpointFile(new File(checkpointPath))

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)

    //When
    val checkpoint2 = new LeaderEpochCheckpointFile(new File(checkpointPath))
    val cache2 = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint2)

    //Then
    assertEquals(1, cache2.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache2.epochEntries.toList(0))
  }

  @Test
  def shouldNotLetEpochGoBackwardsEvenIfMessageEpochsDo(): Unit = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Given
    cache.assign(epoch = 1, offset = 5); leo = 6
    cache.assign(epoch = 2, offset = 6); leo = 7

    //When we update an epoch in the past with an earlier offset
    cache.assign(epoch = 1, offset = 7); leo = 8

    //Then epoch should not be changed
    assertEquals(2, cache.latestEpoch())

    //Then end offset for epoch 1 shouldn't have changed
    assertEquals((1, 6), cache.endOffsetFor(1))

    //Then end offset for epoch 2 has to be the offset of the epoch 1 message (I can't think of a better option)
    assertEquals((2, 8), cache.endOffsetFor(2))

    //Epoch history shouldn't have changed
    assertEquals(EpochEntry(1, 5), cache.epochEntries()(0))
    assertEquals(EpochEntry(2, 6), cache.epochEntries()(1))
  }

  @Test
  def shouldNotLetOffsetsGoBackwardsEvenIfEpochsProgress() = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When epoch goes forward but offset goes backwards
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 5)

    //Then latter assign should be ignored
    assertEquals(EpochEntry(2, 6), cache.epochEntries.toList(0))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsLeadersChangeManyTimes(): Unit = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 0, offset = 0) //leo=0

    //When
    cache.assign(epoch = 1, offset = 0) //leo=0

    //Then epoch should go up
    assertEquals(1, cache.latestEpoch())
    //offset for 1 should still be 0
    assertEquals((1, 0), cache.endOffsetFor(1))
    //offset for epoch 0 should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0))

    //When we write 5 messages as epoch 1
    leo = 5

    //Then end offset for epoch(1) should be leo => 5
    assertEquals((1, 5), cache.endOffsetFor(1))
    //Epoch 0 should still be at offset 0
    assertEquals((0, 0), cache.endOffsetFor(0))

    //When
    cache.assign(epoch = 2, offset = 5) //leo=5

    leo = 10 //write another 5 messages

    //Then end offset for epoch(2) should be leo => 10
    assertEquals((2, 10), cache.endOffsetFor(2))

    //end offset for epoch(1) should be the start offset of epoch(2) => 5
    assertEquals((1, 5), cache.endOffsetFor(1))

    //epoch (0) should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsFollowerReceivesManyMessages(): Unit = {
    var leo = 0
    def leoFinder() = new LogOffsetMetadata(leo)

    //When new
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //When Messages come in
    cache.assign(epoch = 0, offset = 0); leo = 1
    cache.assign(epoch = 0, offset = 1); leo = 2
    cache.assign(epoch = 0, offset = 2); leo = 3

    //Then epoch should stay, offsets should grow
    assertEquals(0, cache.latestEpoch())
    assertEquals((0, leo), cache.endOffsetFor(0))

    //When messages arrive with greater epoch
    cache.assign(epoch = 1, offset = 3); leo = 4
    cache.assign(epoch = 1, offset = 4); leo = 5
    cache.assign(epoch = 1, offset = 5); leo = 6

    assertEquals(1, cache.latestEpoch())
    assertEquals((1, leo), cache.endOffsetFor(1))

    //When
    cache.assign(epoch = 2, offset = 6); leo = 7
    cache.assign(epoch = 2, offset = 7); leo = 8
    cache.assign(epoch = 2, offset = 8); leo = 9

    assertEquals(2, cache.latestEpoch())
    assertEquals((2, leo), cache.endOffsetFor(2))

    //Older epochs should return the start offset of the first message in the subsequent epoch.
    assertEquals((0, 3), cache.endOffsetFor(0))
    assertEquals((1, 6), cache.endOffsetFor(1))
  }

  @Test
  def shouldDropEntriesOnEpochBoundaryWhenRemovingLatestEntries(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When clear latest on epoch boundary
    cache.clearAndFlushLatest(offset = 8)

    //Then should remove two latest epochs (remove is inclusive)
    assertEquals(ListBuffer(EpochEntry(2, 6)), cache.epochEntries)
  }

  @Test
  def shouldPreserveResetOffsetOnClearEarliestIfOneExists(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset ON epoch boundary
    cache.clearAndFlushEarliest(offset = 8)

    //Then should preserve (3, 8)
    assertEquals(ListBuffer(EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateSavedOffsetWhenOffsetToClearToIsBetweenEpochs(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.clearAndFlushEarliest(offset = 9)

    //Then we should retain epoch 3, but update it's offset to 9 as 8 has been removed
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToEarly(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset before first epoch offset
    cache.clearAndFlushEarliest(offset = 1)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToFirstOffset(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset on earliest epoch boundary
    cache.clearAndFlushEarliest(offset = 6)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliest(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When
    cache.clearAndFlushEarliest(offset = 11)

    //Then retain the last
    assertEquals(ListBuffer(EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When we clear from a postition between offset 8 & offset 11
    cache.clearAndFlushEarliest(offset = 9)

    //Then we should update the middle epoch entry's offset
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest2(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 0, offset = 0)
    cache.assign(epoch = 1, offset = 7)
    cache.assign(epoch = 2, offset = 10)

    //When we clear from a postition between offset 0 & offset 7
    cache.clearAndFlushEarliest(offset = 5)

    //Then we should keeep epoch 0 but update the offset appropriately
    assertEquals(ListBuffer(EpochEntry(0,5), EpochEntry(1, 7), EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliestAndUpdateItsOffset(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset beyond last epoch
    cache.clearAndFlushEarliest(offset = 15)

    //Then update the last
    assertEquals(ListBuffer(EpochEntry(4, 15)), cache.epochEntries)
  }

  @Test
  def shouldDropEntriesBetweenEpochBoundaryWhenRemovingNewest(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.clearAndFlushLatest(offset = 9)

    //Then should keep the preceding epochs
    assertEquals(3, cache.latestEpoch())
    assertEquals(ListBuffer(EpochEntry(2, 6), EpochEntry(3, 8)), cache.epochEntries)
  }

  @Test
  def shouldClearAllEntries(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When 
    cache.clearAndFlush()

    //Then 
    assertEquals(0, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryHeadIfUndefinedPassed(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset on epoch boundary
    cache.clearAndFlushLatest(offset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryTailIfUndefinedPassed(): Unit = {
    def leoFinder() = new LogOffsetMetadata(0)

    //Given
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)
    cache.assign(epoch = 2, offset = 6)
    cache.assign(epoch = 3, offset = 8)
    cache.assign(epoch = 4, offset = 11)

    //When reset to offset on epoch boundary
    cache.clearAndFlushEarliest(offset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldFetchLatestEpochOfEmptyCache(): Unit = {
    //Given
    def leoFinder() = new LogOffsetMetadata(0)

    //When
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Then
    assertEquals(-1, cache.latestEpoch)
  }

  @Test
  def shouldFetchEndOffsetOfEmptyCache(): Unit = {
    //Given
    def leoFinder() = new LogOffsetMetadata(0)

    //When
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(7))
  }

  @Test
  def shouldClearEarliestOnEmptyCache(): Unit = {
    //Given
    def leoFinder() = new LogOffsetMetadata(0)

    //When
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Then
    cache.clearAndFlushEarliest(7)
  }

  @Test
  def shouldClearLatestOnEmptyCache(): Unit = {
    //Given
    def leoFinder() = new LogOffsetMetadata(0)

    //When
    val cache = new LeaderEpochFileCache(tp, () => leoFinder, checkpoint)

    //Then
    cache.clearAndFlushLatest(7)
  }

  @Before
  def setUp() {
    checkpoint = new LeaderEpochCheckpointFile(TestUtils.tempFile())
  }
}
