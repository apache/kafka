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
package kafka.log.remote

import kafka.log.UnifiedLog
import kafka.utils.MockTime
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.internals.{OffsetIndex, OffsetPosition, TimeIndex}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.Collections
import scala.collection.mutable

class RemoteIndexCacheTest {

  val time = new MockTime()
  val partition = new TopicPartition("foo", 0)
  val idPartition = new TopicIdPartition(Uuid.randomUuid(), partition)
  val logDir: File = TestUtils.tempDirectory("kafka-logs")
  val tpDir: File = new File(logDir, partition.toString)
  val brokerId = 1
  val baseOffset = 45L
  val lastOffset = 75L
  val segmentSize = 1024

  val rsm: RemoteStorageManager = mock(classOf[RemoteStorageManager])
  val cache: RemoteIndexCache =  new RemoteIndexCache(remoteStorageManager = rsm, logDir = logDir.toString)
  val remoteLogSegmentId = new RemoteLogSegmentId(idPartition, Uuid.randomUuid())
  val rlsMetadata: RemoteLogSegmentMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset, lastOffset,
    time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L))

  @BeforeEach
  def setup(): Unit = {
    Files.createDirectory(tpDir.toPath)
    val txnIdxFile = new File(tpDir, "txn-index" + UnifiedLog.TxnIndexFileSuffix)
    txnIdxFile.createNewFile()
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
        val offsetIdx = new OffsetIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.IndexFileSuffix),
          metadata.startOffset(), maxEntries * 8)
        val timeIdx = new TimeIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.TimeIndexFileSuffix),
          metadata.startOffset(), maxEntries * 12)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdxFile)
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })
  }

  @AfterEach
  def cleanup(): Unit = {
    reset(rsm)
    cache.entries.forEach((_, v) => v.cleanup())
    cache.close()
  }

  @Test
  def testFetchIndexFromRemoteStorage(): Unit = {
    val offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex.get
    val offsetPosition1 = offsetIndex.entry(1)
    // this call should have invoked fetchOffsetIndex, fetchTimestampIndex once
    val resultPosition = cache.lookupOffset(rlsMetadata, offsetPosition1.offset)
    assertEquals(offsetPosition1.position, resultPosition)
    verifyFetchIndexInvocation(count = 1, Seq(IndexType.OFFSET, IndexType.TIMESTAMP))

    // this should not cause fetching index from RemoteStorageManager as it is already fetched earlier
    reset(rsm)
    val offsetPosition2 = offsetIndex.entry(2)
    val resultPosition2 = cache.lookupOffset(rlsMetadata, offsetPosition2.offset)
    assertEquals(offsetPosition2.position, resultPosition2)
    assertNotNull(cache.getIndexEntry(rlsMetadata))
    verifyNoInteractions(rsm)
  }

  @Test
  def testPositionForNonExistingIndexFromRemoteStorage(): Unit = {
    val offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex.get
    val lastOffsetPosition = cache.lookupOffset(rlsMetadata, offsetIndex.lastOffset)
    val greaterOffsetThanLastOffset = offsetIndex.lastOffset + 1
    assertEquals(lastOffsetPosition, cache.lookupOffset(rlsMetadata, greaterOffsetThanLastOffset))

    // offsetIndex.lookup() returns OffsetPosition(baseOffset, 0) for offsets smaller than least entry in the offset index.
    val nonExistentOffsetPosition = new OffsetPosition(baseOffset, 0)
    val lowerOffsetThanBaseOffset = offsetIndex.baseOffset - 1
    assertEquals(nonExistentOffsetPosition.position, cache.lookupOffset(rlsMetadata, lowerOffsetThanBaseOffset))
  }

  @Test
  def testCacheEntryExpiry(): Unit = {
    val cache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.entries.size())
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList.head)
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.entries.size())
    verifyFetchIndexInvocation(count = 2)

    // getting index for metadataList.last should call rsm#fetchIndex, but metadataList(1) is already in cache.
    cache.getIndexEntry(metadataList.last)
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.entries.size())
    assertTrue(cache.entries.containsKey(metadataList.last.remoteLogSegmentId().id()))
    assertTrue(cache.entries.containsKey(metadataList(1).remoteLogSegmentId().id()))
    verifyFetchIndexInvocation(count = 3)

    // getting index for metadataList.head should call rsm#fetchIndex as that entry was expired earlier,
    // but metadataList(1) is already in cache.
    cache.getIndexEntry(metadataList(1))
    cache.getIndexEntry(metadataList.head)
    assertEquals(2, cache.entries.size())
    assertFalse(cache.entries.containsKey(metadataList.last.remoteLogSegmentId().id()))
    verifyFetchIndexInvocation(count = 4)
  }

  @Test
  def testGetIndexAfterCacheClose(): Unit = {
    val cache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.entries.size())
    verifyFetchIndexInvocation(count = 1)

    cache.close()

    // Check IllegalStateException is thrown when index is accessed after it is closed.
    assertThrows(classOf[IllegalStateException], () => cache.getIndexEntry(metadataList.head))
  }

  @Test
  def testReloadCacheAfterClose(): Unit = {
    val cache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.entries.size())
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList(1))
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.entries.size())
    verifyFetchIndexInvocation(count = 2)

    // Here a new key metadataList(2) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList(2))
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(2))
    assertEquals(2, cache.entries.size())
    verifyFetchIndexInvocation(count = 3)

    // Close the cache
    cache.close()

    // Reload the cache from the disk and check the cache size is same as earlier
    val reloadedCache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    assertEquals(2, reloadedCache.entries.size())
    reloadedCache.close()
  }

  private def verifyFetchIndexInvocation(count: Int,
                                         indexTypes: Seq[IndexType] =
                                         Seq(IndexType.OFFSET, IndexType.TIMESTAMP, IndexType.TRANSACTION)): Unit = {
    for (indexType <- indexTypes) {
      verify(rsm, times(count)).fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(indexType))
    }
  }

  private def generateRemoteLogSegmentMetadata(size: Int,
                                               tpId: TopicIdPartition): List[RemoteLogSegmentMetadata] = {
    val metadataList = mutable.Buffer.empty[RemoteLogSegmentMetadata]
    for (i <- 0 until size) {
      metadataList.append(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tpId, Uuid.randomUuid()), baseOffset * i,
        baseOffset * i + 10, time.milliseconds(), brokerId, time.milliseconds(), segmentSize,
        Collections.singletonMap(0, 0L)))
    }
    metadataList.toList
  }

  private def maybeAppendIndexEntries(offsetIndex: OffsetIndex,
                                      timeIndex: TimeIndex): Unit = {
    if (!offsetIndex.isFull) {
      val curTime = time.milliseconds()
      for (i <- 0 until offsetIndex.maxEntries) {
        val offset = offsetIndex.baseOffset + i
        offsetIndex.append(offset, i)
        timeIndex.maybeAppend(curTime + i, offset, true)
      }
      offsetIndex.flush()
      timeIndex.flush()
    }
  }
}
