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
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteResourceNotFoundException, RemoteStorageManager}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.RemoteIndexCache.{DIR_NAME, Entry, REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, remoteDeletedSuffixIndexFileName, remoteOffsetIndexFile, remoteOffsetIndexFileName, remoteTimeIndexFile, remoteTimeIndexFileName, remoteTransactionIndexFile, remoteTransactionIndexFileName}
import org.apache.kafka.storage.internals.log.{AbortedTxn, CorruptIndexException, LogFileUtils, OffsetIndex, OffsetPosition, RemoteIndexCache, TimeIndex, TransactionIndex}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.Mockito._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream, IOException, PrintWriter, UncheckedIOException}
import java.nio.file.{Files, NoSuchFileException, Paths}
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.{CountDownLatch, Executors, Future, TimeUnit}
import scala.collection.mutable

class RemoteIndexCacheTest {
  private val defaultRemoteIndexCacheSizeBytes = 1024 * 1024L
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

    cache = new RemoteIndexCache(defaultRemoteIndexCacheSizeBytes, rsm, tpDir.toString)

    mockRsmFetchIndex(rsm)
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
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir)
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
    val estimateEntryBytesSize = estimateOneEntryBytesSize()
    // close existing cache created in test setup before creating a new one
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")
    cache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, tpDir.toString)
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

    cache = new RemoteIndexCache(2 * estimateOneEntryBytesSize(), rsm, tpDir.toString)
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
  def testCacheEntryIsDeletedOnRemoval(): Unit = {
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

    // call remove function to mark the entry for removal
    cache.remove(internalIndexKey)

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
    assertFalse(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent,
      s"Offset index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent,
      s"Txn index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent,
      s"Time index file should not be present on disk at ${tpDir.toPath}")
    assertFalse(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent,
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

    TestUtils.waitUntilTrue(() => cache.cleanerThread().isStarted, "Cleaner thread should be started")

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
    val estimateEntryBytesSize = estimateOneEntryBytesSize()
    // close existing cache created in test setup before creating a new one
    Utils.closeQuietly(cache, "RemoteIndexCache created for unit test")
    cache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, tpDir.toString)
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
    val reloadedCache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, tpDir.toString)
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
  def testClearCacheAndIndexFilesWhenResizeCache(): Unit = {
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 1, tpId)

    assertCacheSize(0)
    // getIndex for first time will call rsm#fetchIndex
    val cacheEntry = cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent)

    cache.resizeCacheSize(1L)

    // wait until entry is marked for deletion
    TestUtils.waitUntilTrue(() => cacheEntry.isMarkedForCleanup,
      "Failed to mark cache entry for cleanup after resizing cache.")
    TestUtils.waitUntilTrue(() => cacheEntry.isCleanStarted,
      "Failed to cleanup cache entry after resizing cache.")

    // verify no index files on remote cache dir
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent,
      s"Offset index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent,
      s"Txn index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent,
      s"Time index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent,
      s"Index file marked for deletion should not be present on disk at ${cache.cacheDir()}")

    assertCacheSize(0)
  }

  @Test
  def testCorrectnessForCacheAndIndexFilesWhenResizeCache(): Unit = {

    def verifyEntryIsEvicted(metadataToVerify: RemoteLogSegmentMetadata, entryToVerify: Entry): Unit = {
      // wait until `entryToVerify` is marked for deletion
      TestUtils.waitUntilTrue(() => entryToVerify.isMarkedForCleanup,
        "Failed to mark evicted cache entry for cleanup after resizing cache.")
      TestUtils.waitUntilTrue(() => entryToVerify.isCleanStarted,
        "Failed to cleanup evicted cache entry after resizing cache.")
      // verify no index files for `entryToVerify` on remote cache dir
      TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, remoteOffsetIndexFileName(metadataToVerify)).isPresent,
        s"Offset index file for evicted entry should not be present on disk at ${cache.cacheDir()}")
      TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, remoteTimeIndexFileName(metadataToVerify)).isPresent,
        s"Time index file for evicted entry should not be present on disk at ${cache.cacheDir()}")
      TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, remoteTransactionIndexFileName(metadataToVerify)).isPresent,
        s"Txn index file for evicted entry should not be present on disk at ${cache.cacheDir()}")
      TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, remoteDeletedSuffixIndexFileName(metadataToVerify)).isPresent,
        s"Index file marked for deletion for evicted entry should not be present on disk at ${cache.cacheDir()}")
    }

    def verifyEntryIsKept(metadataToVerify: RemoteLogSegmentMetadata): Unit = {
      assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteOffsetIndexFileName(metadataToVerify)).isPresent)
      assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteTimeIndexFileName(metadataToVerify)).isPresent)
      assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteTransactionIndexFileName(metadataToVerify)).isPresent)
      assertTrue(!getIndexFileFromRemoteCacheDir(cache, remoteDeletedSuffixIndexFileName(metadataToVerify)).isPresent)
    }

    // The test process for resizing is: put 1 entry -> evict to empty -> put 3 entries with limited capacity of 2 entries ->
    // evict to 1 entry -> resize to 1 entry size -> resize to 2 entries size
    val estimateEntryBytesSize = estimateOneEntryBytesSize()
    val tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val metadataList = generateRemoteLogSegmentMetadata(size = 3, tpId)

    assertCacheSize(0)
    // getIndex for first time will call rsm#fetchIndex
    val cacheEntry = cache.getIndexEntry(metadataList.head)
    assertCacheSize(1)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent)
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent)

    // Reduce the cache size to 1 byte to ensure that all the entries are evicted from it.
    cache.resizeCacheSize(1L)

    // wait until entry is marked for deletion
    TestUtils.waitUntilTrue(() => cacheEntry.isMarkedForCleanup,
      "Failed to mark cache entry for cleanup after resizing cache.")
    TestUtils.waitUntilTrue(() => cacheEntry.isCleanStarted,
      "Failed to cleanup cache entry after resizing cache.")

    // verify no index files on remote cache dir
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent,
      s"Offset index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent,
      s"Txn index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent,
      s"Time index file should not be present on disk at ${cache.cacheDir()}")
    TestUtils.waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent,
      s"Index file marked for deletion should not be present on disk at ${cache.cacheDir()}")

    assertCacheSize(0)

    // Increase cache capacity to only store 2 entries
    cache.resizeCacheSize(2 * estimateEntryBytesSize)
    assertCacheSize(0)

    val entry0 = cache.getIndexEntry(metadataList(0))
    val entry1 = cache.getIndexEntry(metadataList(1))
    cache.getIndexEntry(metadataList(2))
    assertCacheSize(2)
    verifyEntryIsEvicted(metadataList(0), entry0)

    // Reduce cache capacity to only store 1 entries
    cache.resizeCacheSize(1 * estimateEntryBytesSize)
    assertCacheSize(1)
    verifyEntryIsEvicted(metadataList(1), entry1)

    // resize to the same size, all entries should be kept
    cache.resizeCacheSize(1 * estimateEntryBytesSize)

    // verify all existing entries (`cache.getIndexEntry(metadataList(2))`) are kept
    verifyEntryIsKept(metadataList(2))
    assertCacheSize(1)

    // increase the size
    cache.resizeCacheSize(2 * estimateEntryBytesSize)

    // verify all existing entries (`cache.getIndexEntry(metadataList(2))`) are kept
    verifyEntryIsKept(metadataList(2))
    assertCacheSize(1)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[IndexType], names = Array("OFFSET", "TIMESTAMP", "TRANSACTION"))
  def testCorruptCacheIndexFileExistsButNotInCache(indexType: IndexType): Unit = {
    // create Corrupted Index File in remote index cache
    createCorruptedIndexFile(indexType, cache.cacheDir())
    val entry = cache.getIndexEntry(rlsMetadata)
    // Test would fail if it throws Exception other than CorruptIndexException
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

    // file is corrupted it should fetch from remote storage again
    verifyFetchIndexInvocation(count = 1)
  }

  @Test
  def testConcurrentRemoveReadForCache(): Unit = {
    // Create a spy Cache Entry
    val rlsMetadata = new RemoteLogSegmentMetadata(RemoteLogSegmentId.generateNew(idPartition), baseOffset, lastOffset,
      time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L))

    val timeIndex = spy(createTimeIndexForSegmentMetadata(rlsMetadata, new File(tpDir, DIR_NAME)))
    val txIndex = spy(createTxIndexForSegmentMetadata(rlsMetadata, new File(tpDir, DIR_NAME)))
    val offsetIndex = spy(createOffsetIndexForSegmentMetadata(rlsMetadata, new File(tpDir, DIR_NAME)))

    val spyEntry = spy(new RemoteIndexCache.Entry(offsetIndex, timeIndex, txIndex))
    cache.internalCache.put(rlsMetadata.remoteLogSegmentId().id(), spyEntry)

    assertCacheSize(1)

    var entry: RemoteIndexCache.Entry = null

    val latchForCacheRead = new CountDownLatch(1)
    val latchForCacheRemove = new CountDownLatch(1)
    val latchForTestWait = new CountDownLatch(1)

    var markForCleanupCallCount = 0

    doAnswer((invocation: InvocationOnMock) => {
      markForCleanupCallCount += 1

      if (markForCleanupCallCount == 1) {
        // Signal the CacheRead to unblock itself
        latchForCacheRead.countDown()
        // Wait for signal to start renaming the files
        latchForCacheRemove.await()
        // Calling the markForCleanup() actual method to start renaming the files
        invocation.callRealMethod()
        // Signal TestWait to unblock itself so that test can be completed
        latchForTestWait.countDown()
      }
    }).when(spyEntry).markForCleanup()

    val removeCache = (() => {
      cache.remove(rlsMetadata.remoteLogSegmentId().id())
    }): Runnable

    val readCache = (() => {
      // Wait for signal to start CacheRead
      latchForCacheRead.await()
      entry = cache.getIndexEntry(rlsMetadata)
      // Signal the CacheRemove to start renaming the files
      latchForCacheRemove.countDown()
    }): Runnable

    val executor = Executors.newFixedThreadPool(2)
    try {
      val removeCacheFuture: Future[_] = executor.submit(removeCache: Runnable)
      val readCacheFuture: Future[_] = executor.submit(readCache: Runnable)

      // Verify both tasks are completed without any exception
      removeCacheFuture.get()
      readCacheFuture.get()

      // Wait for signal to complete the test
      latchForTestWait.await()

      // We can't determine read thread or remove thread will go first so if,
      // 1. Read thread go first, cache file should not exist and cache size should be zero.
      // 2. Remove thread go first, cache file should present and cache size should be one.
      // so basically here we are making sure that if cache existed, the cache file should exist,
      // and if cache is non-existed, the cache file should not exist.
      if (getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent) {
        assertCacheSize(1)
      } else {
        assertCacheSize(0)
      }
    } finally {
      executor.shutdownNow()
    }

  }

  @Test
  def testMultipleIndexEntriesExecutionInCorruptException(): Unit = {
    reset(rsm)
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir)
        val txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        // Create corrupted index file
        createCorruptTimeIndexOffsetFile(tpDir)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdx.file)
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })

    assertThrows(classOf[CorruptIndexException], () => cache.getIndexEntry(rlsMetadata))
    assertNull(cache.internalCache().getIfPresent(rlsMetadata.remoteLogSegmentId().id()))
    verifyFetchIndexInvocation(1, Seq(IndexType.OFFSET, IndexType.TIMESTAMP))
    verifyFetchIndexInvocation(0, Seq(IndexType.TRANSACTION))
    // Current status
    // (cache is null)
    // RemoteCacheDir contain
    // 1. Offset Index File is fine and not corrupted
    // 2. Time Index File is corrupted
    // What should be the code flow in next execution
    // 1. No rsm call for fetching OffSet Index File.
    // 2. Time index file should be fetched from remote storage again as it is corrupted in the first execution.
    // 3. Transaction index file should be fetched from remote storage.
    reset(rsm)
    // delete all files created in tpDir
    Files.walk(tpDir.toPath, 1)
      .filter(Files.isRegularFile(_))
      .forEach(path => Files.deleteIfExists(path))
    // rsm should return no corrupted file in the 2nd execution
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir)
        val txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdx.file)
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })
    cache.getIndexEntry(rlsMetadata)
    // rsm should not be called to fetch offset Index
    verifyFetchIndexInvocation(0, Seq(IndexType.OFFSET))
    verifyFetchIndexInvocation(1, Seq(IndexType.TIMESTAMP))
    // Transaction index would be fetched again
    // as previous getIndexEntry failed before fetchTransactionIndex
    verifyFetchIndexInvocation(1, Seq(IndexType.TRANSACTION))
  }

  @Test
  def testIndexFileAlreadyExistOnDiskButNotInCache(): Unit = {
    val remoteIndexCacheDir = cache.cacheDir()
    val tempSuffix = ".tmptest"

    def renameRemoteCacheIndexFileFromDisk(suffix: String) = {
      Files.walk(remoteIndexCacheDir.toPath)
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName.toString.endsWith(suffix))
        .forEach(f => Utils.atomicMoveWithFallback(f, f.resolveSibling(f.getFileName().toString().stripSuffix(tempSuffix))))
    }

    val entry = cache.getIndexEntry(rlsMetadata)
    verifyFetchIndexInvocation(count = 1)
    // copy files with temporary name
    Files.copy(entry.offsetIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.offsetIndex().file().getPath(), "", tempSuffix)))
    Files.copy(entry.txnIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.txnIndex().file().getPath(), "", tempSuffix)))
    Files.copy(entry.timeIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.timeIndex().file().getPath(), "", tempSuffix)))

    cache.remove(rlsMetadata.remoteLogSegmentId().id())

    // wait until entry is marked for deletion
    TestUtils.waitUntilTrue(() => entry.isMarkedForCleanup,
      "Failed to mark cache entry for cleanup after invalidation")
    TestUtils.waitUntilTrue(() => entry.isCleanStarted,
      "Failed to cleanup cache entry after invalidation")

    // restore index files
    renameRemoteCacheIndexFileFromDisk(tempSuffix)
    // validate cache entry for the above key should be null
    assertNull(cache.internalCache().getIfPresent(rlsMetadata.remoteLogSegmentId().id()))
    cache.getIndexEntry(rlsMetadata)
    // Index  Files already exist ,rsm should not fetch them again.
    verifyFetchIndexInvocation(count = 1)
    // verify index files on disk
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent, s"Offset index file should be present on disk at ${remoteIndexCacheDir.toPath}")
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent, s"Txn index file should be present on disk at ${remoteIndexCacheDir.toPath}")
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent, s"Time index file should be present on disk at ${remoteIndexCacheDir.toPath}")
  }

  @ParameterizedTest
  @EnumSource(value = classOf[IndexType], names = Array("OFFSET", "TIMESTAMP", "TRANSACTION"))
  def testRSMReturnCorruptedIndexFile(testIndexType: IndexType): Unit = {
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir)
        val txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir)
        maybeAppendIndexEntries(offsetIdx, timeIdx)
        // Create corrupt index file return from RSM
        createCorruptedIndexFile(testIndexType, tpDir)
        indexType match {
          case IndexType.OFFSET => new FileInputStream(offsetIdx.file)
          case IndexType.TIMESTAMP => new FileInputStream(timeIdx.file)
          case IndexType.TRANSACTION => new FileInputStream(txnIdx.file)
          case IndexType.LEADER_EPOCH => // leader-epoch-cache is not accessed.
          case IndexType.PRODUCER_SNAPSHOT => // producer-snapshot is not accessed.
        }
      })
    assertThrows(classOf[CorruptIndexException], () => cache.getIndexEntry(rlsMetadata))
  }

  @Test
  def testConcurrentCacheDeletedFileExists(): Unit = {
    val remoteIndexCacheDir = cache.cacheDir()

    val entry = cache.getIndexEntry(rlsMetadata)
    // verify index files on disk
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent, s"Offset index file should be present on disk at ${remoteIndexCacheDir.toPath}")
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent, s"Txn index file should be present on disk at ${remoteIndexCacheDir.toPath}")
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent, s"Time index file should be present on disk at ${remoteIndexCacheDir.toPath}")

    // Simulating a concurrency issue where deleted files already exist on disk
    // This happen when cleanerThread is slow and not able to delete index entries
    // while same index Entry is cached again and invalidated.
    // The new deleted file created should be replaced by existing deleted file.

    // create deleted suffix file
    Files.copy(entry.offsetIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.offsetIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)))
    Files.copy(entry.txnIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.txnIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)))
    Files.copy(entry.timeIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.timeIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)))

    // verify deleted file exists on disk
    assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent, s"Deleted Offset index file should be present on disk at ${remoteIndexCacheDir.toPath}")

    cache.remove(rlsMetadata.remoteLogSegmentId().id())

    // wait until entry is marked for deletion
    TestUtils.waitUntilTrue(() => entry.isMarkedForCleanup,
      "Failed to mark cache entry for cleanup after invalidation")
    TestUtils.waitUntilTrue(() => entry.isCleanStarted,
      "Failed to cleanup cache entry after invalidation")

    // verify no index files on disk
    waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent,
      s"Offset index file should not be present on disk at ${remoteIndexCacheDir.toPath}")
    waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent,
      s"Txn index file should not be present on disk at ${remoteIndexCacheDir.toPath}")
    waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent,
      s"Time index file should not be present on disk at ${remoteIndexCacheDir.toPath}")
    waitUntilTrue(() => !getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent,
      s"Index file marked for deletion should not be present on disk at ${remoteIndexCacheDir.toPath}")
  }

  private def generateSpyCacheEntry(remoteLogSegmentId: RemoteLogSegmentId
                                    = RemoteLogSegmentId.generateNew(idPartition)): RemoteIndexCache.Entry = {
    val rlsMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset, lastOffset,
      time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L))
    val timeIndex = spy(createTimeIndexForSegmentMetadata(rlsMetadata, tpDir))
    val txIndex = spy(createTxIndexForSegmentMetadata(rlsMetadata, tpDir))
    val offsetIndex = spy(createOffsetIndexForSegmentMetadata(rlsMetadata, tpDir))
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

  private def createTxIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata, dir: File): TransactionIndex = {
    val txnIdxFile = remoteTransactionIndexFile(dir, metadata)
    txnIdxFile.createNewFile()
    new TransactionIndex(metadata.startOffset(), txnIdxFile)
  }

  private def createCorruptTxnIndexForSegmentMetadata(dir: File, metadata: RemoteLogSegmentMetadata): TransactionIndex = {
    val txnIdxFile = remoteTransactionIndexFile(dir, metadata)
    txnIdxFile.createNewFile()
    val txnIndex = new TransactionIndex(metadata.startOffset(), txnIdxFile)
    val abortedTxns = List(
      new AbortedTxn(0L, 0, 10, 11),
      new AbortedTxn(1L, 5, 15, 13),
      new AbortedTxn(2L, 18, 35, 25),
      new AbortedTxn(3L, 32, 50, 40))
    abortedTxns.foreach(txnIndex.append)
    txnIndex.close()

    // open the index with a different starting offset to fake invalid data
    return new TransactionIndex(100L, txnIdxFile)
  }

  private def createTimeIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata, dir: File): TimeIndex = {
    val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
    new TimeIndex(remoteTimeIndexFile(dir, metadata), metadata.startOffset(), maxEntries * 12)
  }

  private def createOffsetIndexForSegmentMetadata(metadata: RemoteLogSegmentMetadata, dir: File) = {
    val maxEntries = (metadata.endOffset() - metadata.startOffset()).asInstanceOf[Int]
    new OffsetIndex(remoteOffsetIndexFile(dir, metadata), metadata.startOffset(), maxEntries * 8)
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

  private def estimateOneEntryBytesSize(): Long = {
    val tp = new TopicPartition("estimate-entry-bytes-size", 0)
    val tpId = new TopicIdPartition(Uuid.randomUuid(), tp)
    val tpDir = new File(logDir, tpId.toString)
    Files.createDirectory(tpDir.toPath)
    val rsm = mock(classOf[RemoteStorageManager])
    mockRsmFetchIndex(rsm)
    val cache = new RemoteIndexCache(2L, rsm, tpDir.toString)
    val metadataList = generateRemoteLogSegmentMetadata(size = 1, tpId)
    val entry = cache.getIndexEntry(metadataList.head)
    val entrySizeInBytes = entry.entrySizeBytes()
    Utils.closeQuietly(cache, "RemoteIndexCache created for estimating entry size")
    entrySizeInBytes
  }

  private def mockRsmFetchIndex(rsm: RemoteStorageManager): Unit = {
    when(rsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val metadata = ans.getArgument[RemoteLogSegmentMetadata](0)
        val indexType = ans.getArgument[IndexType](1)
        val offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir)
        val timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir)
        val txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir)
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

  private def createCorruptOffsetIndexFile(dir: File): Unit = {
    val pw = new PrintWriter(remoteOffsetIndexFile(dir, rlsMetadata))
    pw.write("Hello, world")
    // The size of the string written in the file is 12 bytes,
    // but it should be multiple of Offset Index EntrySIZE which is equal to 8.
    pw.close()
  }

  private def createCorruptTimeIndexOffsetFile(dir: File): Unit = {
    val pw = new PrintWriter(remoteTimeIndexFile(dir, rlsMetadata))
    pw.write("Hello, world1")
    // The size of the string written in the file is 13 bytes,
    // but it should be multiple of Time Index EntrySIZE which is equal to 12.
    pw.close()
  }

  private def createCorruptedIndexFile(indexType: IndexType, dir: File): Unit = {
    if (indexType == IndexType.OFFSET) {
      createCorruptOffsetIndexFile(dir)
    } else if (indexType == IndexType.TIMESTAMP) {
      createCorruptTimeIndexOffsetFile(dir)
    } else if (indexType == IndexType.TRANSACTION) {
      createCorruptTxnIndexForSegmentMetadata(dir, rlsMetadata)
    }
  }

  private def getIndexFileFromRemoteCacheDir(cache: RemoteIndexCache, suffix: String) = {
    try {
      Files.walk(cache.cacheDir().toPath())
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName.toString.endsWith(suffix))
        .findAny()
    } catch {
      case e @ (_ : NoSuchFileException | _ : UncheckedIOException) => Optional.empty()
    }
  }
}
