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
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{OffsetIndex, OffsetPosition, TimeIndex}
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable

class RemoteIndexCacheTest {
  val logger: Logger = LoggerFactory.getLogger(classOf[RemoteIndexCacheTest])
  val time = new MockTime()
  val partition = new TopicPartition("foo", 0)
  val idPartition = new TopicIdPartition(Uuid.randomUuid(), partition)
  val logDir: File = TestUtils.tempDir()
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
    // delete the files on the disk after every test
    cache.internalCache.asMap().forEach((_, entry) => entry.cleanup())
    cache.close()
  }

  @Test
  def testFetchIndexFromRemoteStorage(): Unit = {
    val offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex
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
    val offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex
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

    assertEquals(0, cache.internalCache.estimatedSize())
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.estimatedSize())
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.estimatedSize())
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList.head)
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.internalCache.estimatedSize())
    verifyFetchIndexInvocation(count = 2)

    // getting index for metadataList.last should call rsm#fetchIndex, but metadataList(1) is already in cache.
    cache.getIndexEntry(metadataList.last)
    cache.getIndexEntry(metadataList(1))
    // Cache may grow beyond the size temporarily while evicting, hence, run in a loop to validate
    // that cache reaches correct state eventually
    TestUtils.waitUntilTrue(() => cache.internalCache.asMap().size() == 2, msg = "cache did not adhere to max size of 2")
    assertNotNull(cache.internalCache.getIfPresent(metadataList.last.remoteLogSegmentId().id()))
    assertNotNull(cache.internalCache.getIfPresent(metadataList(1).remoteLogSegmentId().id()))
    verifyFetchIndexInvocation(count = 3)

    // getting index for metadataList.head should call rsm#fetchIndex as that entry was expired earlier,
    // but metadataList(1) is already in cache.
    cache.getIndexEntry(metadataList(1))
    cache.getIndexEntry(metadataList.head)
    // Cache may grow beyond the size temporarily while evicting, hence, run in a loop to validate
    // that cache reaches correct state eventually
    TestUtils.waitUntilTrue(() => cache.internalCache.asMap().size() == 2, msg = "cache did not adhere to max size of 2")
    assertNull(cache.internalCache.getIfPresent(metadataList.last.remoteLogSegmentId().id()))
    verifyFetchIndexInvocation(count = 4)
  }

  @Test
  def testGetIndexAfterCacheClose(): Unit = {
    val cache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertEquals(0, cache.internalCache.asMap().size())
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.asMap().size())
    verifyFetchIndexInvocation(count = 1)

    cache.close()

    // Check IllegalStateException is thrown when index is accessed after it is closed.
    assertThrows(classOf[IllegalStateException], () => cache.getIndexEntry(metadataList.head))
  }

  @Test
  def testCloseIsIdempotent(): Unit = {
    val spyInternalCache = spy(cache.internalCache)
    cache.internalCache = spyInternalCache
    cache.close()
    cache.close()
    // verify that cleanup is only called once
    verify(spyInternalCache).cleanUp()
  }

  @Test
  def testClose(): Unit = {
    val spyInternalCache = spy(cache.internalCache)
    val spyCleanerThread = spy(cache.cleanerThread)

    cache.internalCache = spyInternalCache
    cache.cleanerThread = spyCleanerThread

    // use the cache
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 1, tpId)
    val entry = cache.getIndexEntry(metadataList.head)
    val spyTxnIndex = spy(entry.txnIndex)
    val spyOffsetIndex = spy(entry.offsetIndex)
    val spyTimeIndex = spy(entry.timeIndex)
    // remove this entry and replace with spied entry
    cache.internalCache.invalidateAll()
    cache.internalCache.put(metadataList.head.remoteLogSegmentId().id(), new Entry(spyOffsetIndex, spyTimeIndex, spyTxnIndex))

    // close the cache
    cache.close()

    // cleaner thread should be closed properly
    verify(spyCleanerThread).initiateShutdown()
    verify(spyCleanerThread).awaitShutdown()

    // internal cache cleanup should be called
    verify(spyInternalCache).cleanUp()

    // close for all index entries must be invoked
    verify(spyTxnIndex).close()
    verify(spyOffsetIndex).close()
    verify(spyTimeIndex).close()

    // index files must not be deleted
    verify(spyTxnIndex, times(0)).deleteIfExists()
    verify(spyOffsetIndex, times(0)).deleteIfExists()
    verify(spyTimeIndex, times(0)).deleteIfExists()
  }

  @Test
  def testConcurrentReadWriteAccessForCache(): Unit = {
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertEquals(0, cache.internalCache.asMap().size())
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.asMap().size())
    verifyFetchIndexInvocation(count = 1, Seq(IndexType.OFFSET, IndexType.TIMESTAMP))
    reset(rsm)

    // Simulate a concurrency situation where one thread is reading the entry already present in the cache (cache hit)
    // and the other thread is reading an entry which is not available in the cache (cache miss). The expected behaviour
    // is for the former thread to succeed while latter is fetching from rsm.
    // In this this test we simulate the situation using latches. We perform the following operations:
    // 1. Start the CacheMiss thread and wait until it starts executing the rsm.fetchIndex
    // 2. Block the CacheMiss thread inside the call to rsm.fetchIndex.
    // 3. Start the CacheHit thread. Assert that it performs a successful read.
    // 4. On completion of successful read by CacheHit thread, signal the CacheMiss thread to release it's block.
    // 5. Validate that the test passes. If the CacheMiss thread was blocking the CacheHit thread, the test will fail.
    //
    val latchForCacheHit = new CountDownLatch(1)
    val latchForCacheMiss = new CountDownLatch(1)

    val readerCacheHit = (() => {
      // Wait for signal to start executing the read
      logger.debug(s"Waiting for signal to begin read from ${Thread.currentThread()}")
      latchForCacheHit.await()
      val entry = cache.getIndexEntry(metadataList.head)
      assertNotNull(entry)
      // Signal the CacheMiss to unblock itself
      logger.debug(s"Signaling CacheMiss to unblock from ${Thread.currentThread()}")
      latchForCacheMiss.countDown()
    }): Runnable

    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(_ => {
        logger.debug(s"Signaling CacheHit to begin read from ${Thread.currentThread()}")
        latchForCacheHit.countDown()
        logger.debug("Waiting for signal to complete rsm fetch from" + Thread.currentThread())
        latchForCacheMiss.await()
      })

    val readerCacheMiss = (() => {
      val entry = cache.getIndexEntry(metadataList.last)
      assertNotNull(entry)
    }): Runnable

    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.submit(readerCacheMiss: Runnable)
      executor.submit(readerCacheHit: Runnable)
      assertTrue(latchForCacheMiss.await(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testReloadCacheAfterClose(): Unit = {
    val cache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertEquals(0, cache.internalCache.asMap().size())
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.asMap().size())
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertEquals(1, cache.internalCache.asMap().size())
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.internalCache.asMap().size())
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(1))
    assertEquals(2, cache.internalCache.asMap().size())
    verifyFetchIndexInvocation(count = 2)

    // Here a new key metadataList(2) is invoked, that should call rsm#fetchIndex
    // The cache max size is 2, it will remove one entry and keep the overall size to 2
    cache.getIndexEntry(metadataList(2))
    // Cache may grow beyond the size temporarily while evicting, hence, run in a loop to validate
    // that cache reaches correct state eventually
    TestUtils.waitUntilTrue(() => cache.internalCache.asMap().size() == 2, msg = "cache did not adhere to max size of 2")
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(2))
    assertEquals(2, cache.internalCache.asMap().size())
    verifyFetchIndexInvocation(count = 3)

    // Close the cache
    cache.close()

    // Reload the cache from the disk and check the cache size is same as earlier
    val reloadedCache = new RemoteIndexCache(maxSize = 2, rsm, logDir = logDir.toString)
    assertEquals(2, cache.internalCache.asMap().size())
    reloadedCache.close()

    verifyNoMoreInteractions(rsm)
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
