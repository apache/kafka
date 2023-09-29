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

import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteResourceNotFoundException, RemoteStorageManager}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.RemoteIndexCache.{REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, remoteOffsetIndexFile, remoteOffsetIndexFileName, remoteTimeIndexFile, remoteTimeIndexFileName, remoteTransactionIndexFile, remoteTransactionIndexFileName}
import org.apache.kafka.storage.internals.log.{LogFileUtils, OffsetIndex, OffsetPosition, RemoteIndexCache, TimeIndex, TransactionIndex}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream, IOException, PrintWriter}
import java.nio.file.Files
import java.util
import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable

class RemoteIndexCacheTest {
  private val logger: Logger = LoggerFactory.getLogger(classOf[RemoteIndexCacheTest])
  private val time = new MockTime()
  private val brokerId = 1
  private val baseOffset: Long = Int.MaxValue.toLong + 101337 // start with a base offset which is a long
  private val lastOffset: Long = baseOffset + 30L
  private val segmentSize: Int = 1024
  private val rsm: RemoteStorageManager = mock(classOf[RemoteStorageManager])
  private var cache: RemoteIndexCache = _
  private var rlsMetadata: RemoteLogSegmentMetadata = _
  private var logDir: File = _
  private var tpDir: File = _
  private var idPartition: TopicIdPartition = _

  @BeforeEach
  def setup(): Unit = {
    idPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    logDir = JTestUtils.tempDirectory(s"kafka-${this.getClass.getSimpleName}")
    tpDir = new File(logDir, idPartition.toString)
    Files.createDirectory(tpDir.toPath)

    val remoteLogSegmentId = RemoteLogSegmentId.generateNew(idPartition)
    rlsMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset, lastOffset,
      time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L))

    cache = new RemoteIndexCache(rsm, tpDir.toString)

    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata)
        val txnIdx = createTxIndexForSegmentMetadata(metadata)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdx.file)
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })
  }

  @AfterEach
  def cleanup(): Unit = {
    reset(rsm)
    // the files created for the test will be deleted automatically on thread exit since we use temp dir
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")
    // best effort to delete the per-test resource. Even if we don't delete, it is ok because the parent directory
    // will be deleted at the end of test.
    try {
      Utils.delete(logDir)
    } catch {
      case _: IOException => // ignore
    }
    // Verify no lingering threads. It is important to have this as the very last statement in the @aftereach
    // because this may throw an exception and prevent cleanup after it
    TestUtils.assertNoNonDaemonThreads(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD)
  }

  @Test
  def testIndexFileNameAndLocationOnDisk(): Unit = {
    val entry = cache.getIndexEntry(rlsMetadata)
    val offsetIndexFile = entry.offsetIndex.file().toPath
    val txnIndexFile = entry.txnIndex.file().toPath
    val timeIndexFile = entry.timeIndex.file().toPath

    val expectedOffsetIndexFileName: String = remoteOffsetIndexFileName(rlsMetadata)
    val expectedTimeIndexFileName: String = remoteTimeIndexFileName(rlsMetadata)
    val expectedTxnIndexFileName: String = remoteTransactionIndexFileName(rlsMetadata)

    assertEquals(expectedOffsetIndexFileName, offsetIndexFile.getFileName.toString)
    assertEquals(expectedTxnIndexFileName, txnIndexFile.getFileName.toString)
    assertEquals(expectedTimeIndexFileName, timeIndexFile.getFileName.toString)

    // assert that parent directory for the index files is correct
    assertEquals(RemoteIndexCache.DIR_NAME, offsetIndexFile.getParent.getFileName.toString,
      s"offsetIndex=$offsetIndexFile is created under incorrect parent")
    assertEquals(RemoteIndexCache.DIR_NAME, txnIndexFile.getParent.getFileName.toString,
      s"txnIndex=$txnIndexFile is created under incorrect parent")
    assertEquals(RemoteIndexCache.DIR_NAME, timeIndexFile.getParent.getFileName.toString,
      s"timeIndex=$timeIndexFile is created under incorrect parent")
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
  def testFetchIndexForMissingTransactionIndex(): Unit = {
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          // Throw RemoteResourceNotFoundException since transaction index is not available
          case IndexType.TRANSACTION => throw new RemoteResourceNotFoundException("txn index not found")
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })

    val entry = cache.getIndexEntry(rlsMetadata)
    // Verify an empty file is created in the cache directory
    assertTrue(entry.txnIndex().file().exists())
    assertEquals(0, entry.txnIndex().file().length())
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
    // close existing cache created in test setup before creating a new one
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")
    cache = new RemoteIndexCache(2, rsm, tpDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertCacheSize(0)
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList.head)
    cache.getIndexEntry(metadataList(1))
    assertCacheSize(2)
    verifyFetchIndexInvocation(count = 2)

    // Getting index for metadataList.last should call rsm#fetchIndex
    // to populate this entry one of the other 2 entries will be evicted. We don't know which one since it's based on
    // a probabilistic formula for Window TinyLfu. See docs for RemoteIndexCache
    assertNotNull(cache.getIndexEntry(metadataList.last))
    assertAtLeastOnePresent(cache, metadataList(1).remoteLogSegmentId().id(), metadataList.head.remoteLogSegmentId().id())
    assertCacheSize(2)
    verifyFetchIndexInvocation(count = 3)

    // getting index for last expired entry should call rsm#fetchIndex as that entry was expired earlier
    val missingEntryOpt = {
      metadataList.find(segmentMetadata => {
        val segmentId = segmentMetadata.remoteLogSegmentId().id()
        !cache.internalCache.asMap().containsKey(segmentId)
      })
    }
    assertFalse(missingEntryOpt.isEmpty)
    cache.getIndexEntry(missingEntryOpt.get)
    assertCacheSize(2)
    verifyFetchIndexInvocation(count = 4)
  }

  @Test
  def testGetIndexAfterCacheClose(): Unit = {
    // close existing cache created in test setup before creating a new one
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")

    cache = new RemoteIndexCache(2, rsm, tpDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertCacheSize(0)
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    verifyFetchIndexInvocation(count = 1)

    cache.close()

    // Check IllegalStateException is thrown when index is accessed after it is closed.
    assertThrows(classOf[IllegalStateException], () => cache.getIndexEntry(metadataList.head))
  }

  @Test
  def testCloseIsIdempotent(): Unit = {
    // generate and add entry to cache
    val spyEntry = generateSpyCacheEntry()
    cache.internalCache.put(rlsMetadata.remoteLogSegmentId().id(), spyEntry)

    cache.close()
    cache.close()

    // verify that entry is only closed once
    verify(spyEntry).close()
  }

  @Test
  def testCacheEntryIsDeletedOnInvalidation(): Unit = {
    def getIndexFileFromDisk(suffix: String) = {
      Files.walk(tpDir.toPath)
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName.toString.endsWith(suffix))
        .findAny()
    }

    val internalIndexKey = rlsMetadata.remoteLogSegmentId().id()
    val cacheEntry = generateSpyCacheEntry()

    // verify index files on disk
    assertTrue(getIndexFileFromDisk(LogFileUtils.INDEX_FILE_SUFFIX).isPresent, s"Offset index file should be present on disk at ${tpDir.toPath}")
    assertTrue(getIndexFileFromDisk(LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent, s"Txn index file should be present on disk at ${tpDir.toPath}")
    assertTrue(getIndexFileFromDisk(LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent, s"Time index file should be present on disk at ${tpDir.toPath}")

    // add the spied entry into the cache, it will overwrite the non-spied entry
    cache.internalCache.put(internalIndexKey, cacheEntry)

    // no expired entries yet
    assertEquals(0, cache.expiredIndexes.size, "expiredIndex queue should be zero at start of test")

    // invalidate the cache. it should async mark the entry for removal
    cache.internalCache.invalidate(internalIndexKey)

    // wait until entry is marked for deletion
    TestUtils.waitUntilTrue(() => cacheEntry.isMarkedForCleanup,
      "Failed to mark cache entry for cleanup after invalidation")
    TestUtils.waitUntilTrue(() => cacheEntry.isCleanStarted,
      "Failed to cleanup cache entry after invalidation")

    // first it will be marked for cleanup, second time markForCleanup is called when cleanup() is called
    verify(cacheEntry, times(2)).markForCleanup()
    // after that async it will be cleaned up
    verify(cacheEntry).cleanup()

    // verify that index(s) rename is only called 1 time
    verify(cacheEntry.timeIndex).renameTo(any(classOf[File]))
    verify(cacheEntry.offsetIndex).renameTo(any(classOf[File]))
    verify(cacheEntry.txnIndex).renameTo(any(classOf[File]))

    // verify no index files on disk
    assertFalse(getIndexFileFromDisk(LogFileUtils.INDEX_FILE_SUFFIX).isPresent,
      s"Offset index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromDisk(LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent,
      s"Txn index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromDisk(LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent,
      s"Time index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromDisk(LogFileUtils.DELETED_FILE_SUFFIX).isPresent,
      s"Index file marked for deletion should not be present on disk at ${tpDir.toPath}")
  }

  @Test
  def testCleanerThreadShutdown(): Unit = {
    // cache is empty at beginning
    assertTrue(cache.internalCache.asMap().isEmpty)
    // verify that cleaner thread is running
    TestUtils.numThreadsRunning(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, isDaemon = true)
    // create a new entry
    val spyEntry = generateSpyCacheEntry()
    // an exception should not close the cleaner thread
    when(spyEntry.cleanup()).thenThrow(new RuntimeException("kaboom! I am expected exception in unit test."))
    val key = Uuid.randomUuid()
    cache.internalCache.put(key, spyEntry)
    // trigger cleanup
    cache.internalCache.invalidate(key)
    // wait for cleanup to start
    TestUtils.waitUntilTrue(() => spyEntry.isCleanStarted, "Failed while waiting for clean up to start")
    // Give the thread cleaner thread some time to throw an exception
    Thread.sleep(100)
    // Verify that Cleaner thread is still running even when exception is thrown in doWork()
    var threads = TestUtils.numThreadsRunning(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, isDaemon = true)
    assertEquals(1, threads.size,
      s"Found unexpected ${threads.size} threads=${threads.map(t => t.getName).mkString(", ")}")

    // close the cache properly
    cache.close()

    // verify that the thread is closed properly
    threads = TestUtils.numThreadsRunning(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, isDaemon = true)
    assertTrue(threads.isEmpty, s"Found unexpected ${threads.size} threads=${threads.map(t => t.getName).mkString(", ")}")
    // if the thread is correctly being shutdown it will not be running
    assertFalse(cache.cleanerThread.isRunning, "Unexpected thread state=running. Check error logs.")
  }

  @Test
  def testClose(): Unit = {
    val spyEntry = generateSpyCacheEntry()
    cache.internalCache.put(rlsMetadata.remoteLogSegmentId().id(), spyEntry)

    // close the cache
    cache.close()

    // closing the cache should close the entry
    verify(spyEntry).close()

    // close for all index entries must be invoked
    verify(spyEntry.txnIndex).close()
    verify(spyEntry.offsetIndex).close()
    verify(spyEntry.timeIndex).close()

    // index files must not be deleted
    verify(spyEntry.txnIndex, times(0)).deleteIfExists()
    verify(spyEntry.offsetIndex, times(0)).deleteIfExists()
    verify(spyEntry.timeIndex, times(0)).deleteIfExists()

    // verify cleaner thread is shutdown
    assertTrue(cache.cleanerThread.isShutdownComplete)
  }

  @Test
  def testConcurrentReadWriteAccessForCache(): Unit = {
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertCacheSize(0)
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
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
        logger.debug(s"Waiting for signal to complete rsm fetch from ${Thread.currentThread()}")
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
    // close existing cache created in test setup before creating a new one
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")
    cache = new RemoteIndexCache(2, rsm, tpDir.toString)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertCacheSize(0)
    // getIndex for first time will call rsm#fetchIndex
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    verifyFetchIndexInvocation(count = 1)

    // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
    cache.getIndexEntry(metadataList(1))
    assertCacheSize(2)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(1))
    assertCacheSize(2)
    verifyFetchIndexInvocation(count = 2)

    // Here a new key metadataList(2) is invoked, that should call rsm#fetchIndex
    // The cache max size is 2, it will remove one entry and keep the overall size to 2
    cache.getIndexEntry(metadataList(2))
    assertCacheSize(2)
    // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
    cache.getIndexEntry(metadataList(2))
    assertCacheSize(2)
    verifyFetchIndexInvocation(count = 3)

    // Close the cache
    cache.close()

    // Reload the cache from the disk and check the cache size is same as earlier
    val reloadedCache = new RemoteIndexCache(2, rsm, tpDir.toString)
    assertEquals(2, reloadedCache.internalCache.asMap().size())
    reloadedCache.close()

    verifyNoMoreInteractions(rsm)
  }

  @Test
  def testRemoveItem(): Unit = {
    val segmentId = rlsMetadata.remoteLogSegmentId()
    val segmentUuid = segmentId.id()
    // generate and add entry to cache
    val spyEntry = generateSpyCacheEntry(segmentId)
    cache.internalCache.put(segmentUuid, spyEntry)
    assertTrue(cache.internalCache().asMap().containsKey(segmentUuid))
    assertFalse(spyEntry.isMarkedForCleanup)

    cache.remove(segmentId.id())
    assertFalse(cache.internalCache().asMap().containsKey(segmentUuid))
    TestUtils.waitUntilTrue(() => spyEntry.isMarkedForCleanup, "Failed to mark cache entry for cleanup after invalidation")
  }

  @Test
  def testRemoveNonExistentItem(): Unit = {
    // generate and add entry to cache
    val segmentId = rlsMetadata.remoteLogSegmentId()
    val segmentUuid = segmentId.id()
    // generate and add entry to cache
    val spyEntry = generateSpyCacheEntry(segmentId)
    cache.internalCache.put(segmentUuid, spyEntry)
    assertTrue(cache.internalCache().asMap().containsKey(segmentUuid))

    // remove a random Uuid
    cache.remove(Uuid.randomUuid())
    assertTrue(cache.internalCache().asMap().containsKey(segmentUuid))
    assertFalse(spyEntry.isMarkedForCleanup)
  }

  @Test
  def testRemoveMultipleItems(): Unit = {
    // generate and add entry to cache
    val uuidAndEntryList = new util.HashMap[Uuid, RemoteIndexCache.Entry]()
    for (_ <- 0 until 10) {
      val segmentId = RemoteLogSegmentId.generateNew(idPartition)
      val segmentUuid = segmentId.id()
      val spyEntry = generateSpyCacheEntry(segmentId)
      uuidAndEntryList.put(segmentUuid, spyEntry)

      cache.internalCache.put(segmentUuid, spyEntry)
      assertTrue(cache.internalCache().asMap().containsKey(segmentUuid))
      assertFalse(spyEntry.isMarkedForCleanup)
    }
    cache.removeAll(uuidAndEntryList.keySet())
    uuidAndEntryList.values().forEach { entry =>
      TestUtils.waitUntilTrue(() => entry.isMarkedForCleanup, "Failed to mark cache entry for cleanup after invalidation")
    }
  }

  @Test
  def testCorruptOffsetIndexFileExistsButNotInCache(): Unit = {
    // create Corrupt Offset Index File
    createCorruptRemoteIndexCacheOffsetFile()
    val entry = cache.getIndexEntry(rlsMetadata)
    // Test would fail if it throws corrupt Exception
    val expectedOffsetIndexFileName: String = remoteOffsetIndexFileName(rlsMetadata)
    val offsetIndexFile = entry.offsetIndex.file().toPath

    assertEquals(expectedOffsetIndexFileName, offsetIndexFile.getFileName.toString)
    // assert that parent directory for the index files is correct
    assertEquals(RemoteIndexCache.DIR_NAME, offsetIndexFile.getParent.getFileName.toString,
      s"offsetIndex=$offsetIndexFile is not overwrite under incorrect parent")
    // file is corrupted it should fetch from remote storage again
    verifyFetchIndexInvocation(count = 1)
  }

  private def generateSpyCacheEntry(remoteLogSegmentId: RemoteLogSegmentId
                                    = RemoteLogSegmentId.generateNew(idPartition)): RemoteIndexCache.Entry = {
    val rlsMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset, lastOffset,
      time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L))
    val timeIndex = spy(createTimeIndexForSegmentMetadata(rlsMetadata))
    val txIndex = spy(createTxIndexForSegmentMetadata(rlsMetadata))
    val offsetIndex = spy(createOffsetIndexForSegmentMetadata(rlsMetadata))
    spy(new RemoteIndexCache.Entry(offsetIndex, timeIndex, txIndex))
  }

  private def assertAtLeastOnePresent(cache: RemoteIndexCache, uuids: Uuid*): Unit = {
    uuids.foreach {
      uuid => {
        if (cache.internalCache.asMap().containsKey(uuid)) return
      }
    }
    fail("all uuids are not present in cache")
  }

  private def assertCacheSize(expectedSize: Int): Unit = {
    // Cache may grow beyond the size temporarily while evicting, hence, run in a loop to validate
    // that cache reaches correct state eventually
    TestUtils.waitUntilTrue(() => cache.internalCache.asMap().size() == expectedSize,
      msg = s"cache did not adhere to expected size of $expectedSize")
  }

  private def verifyFetchIndexInvocation(count: Int,
                                         indexTypes: Seq[IndexType] =
                                         Seq(IndexType.OFFSET, IndexType.TIMESTAMP, IndexType.TRANSACTION)): Unit = {
    for (indexType <- indexTypes) {
      verify(rsm, times(count)).fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(indexType))
    }
  }

  private def createTxIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata): TransactionIndex = {
    val txnIdxFile = remoteTransactionIndexFile(tpDir, metadata)
    txnIdxFile.createNewFile()
    new TransactionIndex(metadata.startOffset(), txnIdxFile)
  }

  private def createTimeIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata): TimeIndex = {
    val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
    new TimeIndex(remoteTimeIndexFile(tpDir, metadata), metadata.startOffset(), maxEntries * 12)
  }

  private def createOffsetIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata) = {
    val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
    new OffsetIndex(remoteOffsetIndexFile(tpDir, metadata), metadata.startOffset(), maxEntries * 8)
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

  private def createCorruptRemoteIndexCacheOffsetFile(): Unit = {
    val pw =  new PrintWriter(remoteOffsetIndexFile(new File(tpDir, RemoteIndexCache.DIR_NAME), rlsMetadata))
    pw.write("Hello, world")
    // The size of the string written in the file is 12 bytes,
    // but it should be multiple of Offset Index EntrySIZE which is equal to 8.
    pw.close()
  }

}
