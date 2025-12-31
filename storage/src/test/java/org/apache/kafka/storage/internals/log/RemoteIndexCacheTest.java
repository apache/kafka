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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.storage.internals.log.RemoteIndexCache.DIR_NAME;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteOffsetIndexFile;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteOffsetIndexFileName;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteTimeIndexFile;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteTimeIndexFileName;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteTransactionIndexFile;
import static org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteTransactionIndexFileName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteIndexCacheTest {

    private final long defaultRemoteIndexCacheSizeBytes = 1024 * 1024L;
    private final Logger logger = LoggerFactory.getLogger(RemoteIndexCacheTest.class);
    private final MockTime time = new MockTime();
    private final int brokerId = 1;
    private final long baseOffset = Integer.MAX_VALUE + 101337L; // start with a base offset which is a long
    private final long lastOffset = baseOffset + 30L;
    private final int segmentSize = 1024;
    private final RemoteStorageManager rsm = mock(RemoteStorageManager.class);
    private RemoteIndexCache cache;
    private RemoteLogSegmentMetadata rlsMetadata;
    private File logDir;
    private File tpDir;
    private TopicIdPartition idPartition;

    @BeforeEach
    public void setup() throws IOException, RemoteStorageException {
        idPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        logDir = TestUtils.tempDirectory("kafka-" + this.getClass().getSimpleName());
        tpDir = new File(logDir, idPartition.toString());
        Files.createDirectory(tpDir.toPath());

        RemoteLogSegmentId remoteLogSegmentId = RemoteLogSegmentId.generateNew(idPartition);
        rlsMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset, lastOffset, time.milliseconds(),
                brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L));
        cache = new RemoteIndexCache(defaultRemoteIndexCacheSizeBytes, rsm, logDir.toString());
        cache.setFileDeleteDelayMs(20);
        mockRsmFetchIndex(rsm);
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        reset(rsm);
        // the files created for the test will be deleted automatically on thread exit since we use temp dir
        Utils.closeQuietly(cache, "RemoteIndexCache created for unit test");
        // best effort to delete the per-test resource. Even if we don't delete, it is ok because the parent directory
        // will be deleted at the end of test.
        try {
            Utils.delete(logDir);
        } catch (IOException ioe) {
            // ignore
        }
        // Verify no lingering threads. It is important to have this as the very last statement in the @AfterEach
        // because this may throw an exception and prevent cleanup after it
        TestUtils.assertNoLeakedThreadsWithNameAndDaemonStatus(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD, true);
    }

    @Test
    public void testIndexFileNameAndLocationOnDisk() {
        RemoteIndexCache.Entry entry = cache.getIndexEntry(rlsMetadata);
        Path offsetIndexFile = entry.offsetIndex().file().toPath();
        Path txnIndexFile = entry.txnIndex().file().toPath();
        Path timeIndexFile = entry.timeIndex().file().toPath();

        String expectedOffsetIndexFileName = remoteOffsetIndexFileName(rlsMetadata);
        String expectedTimeIndexFileName = remoteTimeIndexFileName(rlsMetadata);
        String expectedTxnIndexFileName = remoteTransactionIndexFileName(rlsMetadata);

        assertEquals(expectedOffsetIndexFileName, offsetIndexFile.getFileName().toString());
        assertEquals(expectedTxnIndexFileName, txnIndexFile.getFileName().toString());
        assertEquals(expectedTimeIndexFileName, timeIndexFile.getFileName().toString());

        // assert that parent directory for the index files is correct
        assertEquals(DIR_NAME, offsetIndexFile.getParent().getFileName().toString(),
                "offsetIndex=" + offsetIndexFile + " is created under incorrect parent");
        assertEquals(DIR_NAME, txnIndexFile.getParent().getFileName().toString(),
                "txnIndex=" + txnIndexFile + " is created under incorrect parent");
        assertEquals(DIR_NAME, timeIndexFile.getParent().getFileName().toString(),
                "timeIndex=" + timeIndexFile + " is created under incorrect parent");
    }

    @Test
    public void testFetchIndexFromRemoteStorage() throws RemoteStorageException {
        OffsetIndex offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex();
        OffsetPosition offsetPosition1 = offsetIndex.entry(1);
        // this call should have invoked fetchOffsetIndex, fetchTimestampIndex once
        int resultPosition = cache.lookupOffset(rlsMetadata, offsetPosition1.offset());
        assertEquals(offsetPosition1.position(), resultPosition);
        verifyFetchIndexInvocation(1, List.of(IndexType.OFFSET, IndexType.TIMESTAMP));

        // this should not cause fetching index from RemoteStorageManager as it is already fetched earlier
        reset(rsm);
        OffsetPosition offsetPosition2 = offsetIndex.entry(2);
        int resultPosition2 = cache.lookupOffset(rlsMetadata, offsetPosition2.offset());
        assertEquals(offsetPosition2.position(), resultPosition2);
        assertNotNull(cache.getIndexEntry(rlsMetadata));
        verifyNoInteractions(rsm);
    }

    @Test
    public void testFetchIndexForMissingTransactionIndex() throws RemoteStorageException {
        when(rsm.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class))).thenAnswer(ans -> {
            RemoteLogSegmentMetadata metadata = ans.getArgument(0);
            IndexType indexType = ans.getArgument(1);
            OffsetIndex offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir);
            TimeIndex timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir);
            maybeAppendIndexEntries(offsetIdx, timeIdx);
            return switch (indexType) {
                case OFFSET -> new FileInputStream(offsetIdx.file());
                case TIMESTAMP -> new FileInputStream(timeIdx.file());
                // Throw RemoteResourceNotFoundException since transaction index is not available
                case TRANSACTION -> throw new RemoteResourceNotFoundException("txn index not found");
                case LEADER_EPOCH -> null; // leader-epoch-cache is not accessed.
                case PRODUCER_SNAPSHOT -> null;  // producer-snapshot is not accessed.
            };
        });

        RemoteIndexCache.Entry entry = cache.getIndexEntry(rlsMetadata);
        // Verify an empty file is created in the cache directory
        assertTrue(entry.txnIndex().file().exists());
        assertEquals(0, entry.txnIndex().file().length());
    }

    @Test
    public void testPositionForNonExistentEntry() {
        OffsetIndex offsetIndex = cache.getIndexEntry(rlsMetadata).offsetIndex();
        int lastOffsetPosition = cache.lookupOffset(rlsMetadata, offsetIndex.lastOffset());
        long greaterOffsetThanLastOffset = offsetIndex.lastOffset() + 1;
        assertEquals(lastOffsetPosition, cache.lookupOffset(rlsMetadata, greaterOffsetThanLastOffset));

        // offsetIndex.lookup() returns OffsetPosition(baseOffset, 0) for offsets smaller than the last entry in the offset index.
        OffsetPosition nonExistentOffsetPosition = new OffsetPosition(baseOffset, 0);
        long lowerOffsetThanBaseOffset = offsetIndex.baseOffset() - 1;
        assertEquals(nonExistentOffsetPosition.position(), cache.lookupOffset(rlsMetadata, lowerOffsetThanBaseOffset));
    }

    @Test
    public void testCacheEntryExpiry() throws IOException, RemoteStorageException, InterruptedException {
        long estimateEntryBytesSize = estimateOneEntryBytesSize();
        // close existing cache created in test setup before creating a new one
        Utils.closeQuietly(cache, "RemoteIndexCache created for unit test");
        cache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, logDir.toString());
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(3, tpId);

        assertCacheSize(0);
        // getIndex for first time will call rsm#fetchIndex
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        verifyFetchIndexInvocation(1);

        // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
        cache.getIndexEntry(metadataList.get(0));
        cache.getIndexEntry(metadataList.get(1));
        assertCacheSize(2);
        verifyFetchIndexInvocation(2);

        // Getting index for metadataList.last should call rsm#fetchIndex
        // to populate this entry one of the other 2 entries will be evicted. We don't know which one since it's based on
        // a probabilistic formula for Window TinyLfu. See docs for RemoteIndexCache
        int size = metadataList.size();
        assertNotNull(cache.getIndexEntry(metadataList.get(size - 1)));
        assertAtLeastOnePresent(cache, metadataList.get(1).remoteLogSegmentId().id(), metadataList.get(0).remoteLogSegmentId().id());
        assertCacheSize(2);
        verifyFetchIndexInvocation(3);

        // getting index for last expired entry should call rsm#fetchIndex as that entry was expired earlier
        Optional<RemoteLogSegmentMetadata> missingEntryOpt = Optional.empty();
        for (RemoteLogSegmentMetadata entry : metadataList) {
            Uuid segmentId = entry.remoteLogSegmentId().id();
            if (!cache.internalCache().asMap().containsKey(segmentId)) {
                missingEntryOpt = Optional.of(entry);
                break;
            }
        }
        assertFalse(missingEntryOpt.isEmpty());
        cache.getIndexEntry(missingEntryOpt.get());
        assertCacheSize(2);
        verifyFetchIndexInvocation(4);
    }

    @Test
    public void shouldThrowErrorWhenAccessedAfterCacheClose() throws RemoteStorageException, InterruptedException {
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(3, tpId);

        assertCacheSize(0);
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        verifyFetchIndexInvocation(1);

        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getIndexEntry(metadataList.get(0)));
    }

    @Test
    public void testCloseIsIdempotent() throws IOException {
        // generate and add entry to cache
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry();
        cache.internalCache().put(rlsMetadata.remoteLogSegmentId().id(), spyEntry);

        cache.close();
        cache.close();

        // verify that entry is only closed once
        verify(spyEntry).close();
    }

    @Test
    public void testCacheEntryIsDeletedOnRemoval() throws IOException, InterruptedException {
        Uuid internalIndexKey = rlsMetadata.remoteLogSegmentId().id();
        RemoteIndexCache.Entry cacheEntry = generateSpyCacheEntry();

        // verify index files on disk
        assertTrue(getIndexFileFromDisk(LogFileUtils.INDEX_FILE_SUFFIX).isPresent(), "Offset index file should be present on disk at " + tpDir.toPath());
        assertTrue(getIndexFileFromDisk(LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent(), "Txn index file should be present on disk at " + tpDir.toPath());
        assertTrue(getIndexFileFromDisk(LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent(), "Time index file should be present on disk at " + tpDir.toPath());

        // add the spied entry into the cache, it will overwrite the non-spied entry
        cache.internalCache().put(internalIndexKey, cacheEntry);

        // no expired entries yet
        assertEquals(0, cache.expiredIdxPendingForDeletion(), "expiredIndex queue should be zero at start of test");

        // call remove function to mark the entry for removal
        cache.remove(internalIndexKey);

        // wait until entry is marked for deletion
        assertTrue(cacheEntry::isMarkedForCleanup,
                "Failed to mark cache entry for cleanup after remove");
        TestUtils.waitForCondition(cacheEntry::isCleanStarted,
                "Failed to cleanup cache entry after remove");

        verify(cacheEntry).markForCleanup();
        // after that async it will be cleaned up
        verify(cacheEntry).cleanup();

        // verify that index(s) rename is only called 1 time
        verify(cacheEntry.timeIndex()).renameTo(any(File.class));
        verify(cacheEntry.offsetIndex()).renameTo(any(File.class));
        verify(cacheEntry.txnIndex()).renameTo(any(File.class));

        // wait until the delete method is invoked
        TestUtils.waitForCondition(() -> {
            try {
                verify(cacheEntry.timeIndex()).deleteIfExists();
                verify(cacheEntry.offsetIndex()).deleteIfExists();
                verify(cacheEntry.txnIndex()).deleteIfExists();
                return true;
            } catch (Exception e) {
                return false;
            }
        }, "Failed to delete index file");

        // verify no index files on disk
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isEmpty(),
                "Offset index file should not be present on disk at " + tpDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isEmpty(),
                "Txn index file should not be present on disk at " + tpDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isEmpty(),
                "Time index file should not be present on disk at " + tpDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty(),
                "Index file marked for deletion should not be present on disk at " + tpDir.toPath());
    }

    private Optional<Path> getIndexFileFromDisk(String suffix) throws IOException {
        return Files.walk(cache.cacheDir().toPath())
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(suffix))
                .findAny();
    }

    @Test
    public void testCleanerThreadShutdown() throws IOException, InterruptedException {
        // cache is empty at beginning
        assertTrue(cache.internalCache().asMap().isEmpty());
        // create a new entry
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry();
        doAnswer(invocation -> {
            invocation.callRealMethod();
            // an exception should not close the cleaner thread
            throw new RuntimeException("kaboom! I am expected exception in unit test.");
        }).when(spyEntry).cleanup();
        Uuid key = Uuid.randomUuid();
        cache.internalCache().put(key, spyEntry);
        // trigger cleanup
        cache.remove(key);
        // wait for cleanup to start
        TestUtils.waitForCondition(spyEntry::isCleanStarted,
                "Failed while waiting for clean up to start");
        // Give the thread cleaner thread some time to throw an exception
        Thread.sleep(100);
        verify(spyEntry, times(1)).cleanup();
        // Verify that Cleaner thread is still running even when exception is thrown in doWork()
        Set<Thread> threads = getRunningCleanerThread();
        assertEquals(1, threads.size(),
                "Found unexpected " + threads.size() + " threads=" + threads.stream().map(Thread::getName).collect(Collectors.joining(", ")));
        // close the cache properly
        cache.close();
        // verify that the thread is closed properly
        TestUtils.waitForCondition(
                () -> getRunningCleanerThread().isEmpty(),
                () -> "Failed while waiting for cleaner threads to shutdown. Remaining threads: " +
                        getRunningCleanerThread().stream().map(Thread::getName).collect(Collectors.joining(", ")));
        // if the thread is correctly being shutdown it will not be running
        assertFalse(cache.cleanerScheduler().isStarted(), "Unexpected thread state=running. Check error logs.");
    }

    @Test
    public void testClose() throws IOException, InterruptedException {
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry();
        cache.internalCache().put(rlsMetadata.remoteLogSegmentId().id(), spyEntry);

        TestUtils.waitForCondition(() -> cache.cleanerScheduler().isStarted(), "Cleaner thread should be started");

        // close the cache
        cache.close();

        // closing the cache should close the entry
        verify(spyEntry).close();

        // close for all index entries must be invoked
        verify(spyEntry.txnIndex()).close();
        verify(spyEntry.offsetIndex()).close();
        verify(spyEntry.timeIndex()).close();

        // index files must not be deleted
        verify(spyEntry.txnIndex(), times(0)).deleteIfExists();
        verify(spyEntry.offsetIndex(), times(0)).deleteIfExists();
        verify(spyEntry.timeIndex(), times(0)).deleteIfExists();

        // verify cleaner thread is shutdown
        assertFalse(cache.cleanerScheduler().isStarted());
    }

    @Test
    public void testConcurrentReadWriteAccessForCache() throws InterruptedException, RemoteStorageException {
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(3, tpId);

        assertCacheSize(0);
        // getIndex for first time will call rsm#fetchIndex
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        verifyFetchIndexInvocation(1, List.of(IndexType.OFFSET, IndexType.TIMESTAMP));
        reset(rsm);

        // Simulate a concurrency situation where one thread is reading the entry already present in the cache (cache hit)
        // and the other thread is reading an entry which is not available in the cache (cache miss). The expected behaviour
        // is for the former thread to succeed while latter is fetching from rsm.
        // In this test we simulate the situation using latches. We perform the following operations:
        // 1. Start the CacheMiss thread and wait until it starts executing the rsm.fetchIndex
        // 2. Block the CacheMiss thread inside the call to rsm.fetchIndex.
        // 3. Start the CacheHit thread. Assert that it performs a successful read.
        // 4. On completion of successful read by CacheHit thread, signal the CacheMiss thread to release its block.
        // 5. Validate that the test passes. If the CacheMiss thread was blocking the CacheHit thread, the test will fail.
        CountDownLatch latchForCacheHit = new CountDownLatch(1);
        CountDownLatch latchForCacheMiss = new CountDownLatch(1);

        Runnable readerCacheHit = () -> {
            // Wait for signal to start executing the read
            logger.debug("Waiting for signal to begin read from {}", Thread.currentThread());
            try {
                latchForCacheHit.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            RemoteIndexCache.Entry entry = cache.getIndexEntry(metadataList.get(0));
            assertNotNull(entry);
            // Signal the CacheMiss to unblock itself
            logger.debug("Signaling CacheMiss to unblock from {}", Thread.currentThread());
            latchForCacheMiss.countDown();
        };

        when(rsm.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class))).thenAnswer(answer -> {
            logger.debug("Signaling CacheHit to begin read from {}", Thread.currentThread());
            latchForCacheHit.countDown();
            logger.debug("Waiting for signal to complete rsm fetch from {}", Thread.currentThread());
            latchForCacheMiss.await();
            return null;
        });

        Runnable readerCacheMiss = () -> {
            int size = metadataList.size();
            RemoteIndexCache.Entry entry = cache.getIndexEntry(metadataList.get(size - 1));
            assertNotNull(entry);
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            executor.submit(readerCacheMiss);
            executor.submit(readerCacheHit);
            assertTrue(latchForCacheMiss.await(30, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReloadCacheAfterClose() throws IOException, RemoteStorageException, InterruptedException {
        long estimateEntryBytesSize = estimateOneEntryBytesSize();
        // close existing cache created in test setup before creating a new one
        Utils.closeQuietly(cache, "RemoteIndexCache created for unit test");
        cache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, logDir.toString());
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(3, tpId);

        assertCacheSize(0);
        // getIndex for first time will call rsm#fetchIndex
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
        cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        verifyFetchIndexInvocation(1);

        // Here a new key metadataList(1) is invoked, that should call rsm#fetchIndex, making the count to 2
        cache.getIndexEntry(metadataList.get(1));
        assertCacheSize(2);
        // Calling getIndex on the same entry should not call rsm#fetchIndex again, but it should retrieve from cache
        cache.getIndexEntry(metadataList.get(1));
        assertCacheSize(2);
        verifyFetchIndexInvocation(2);

        // Here a new key metadataList(2) is invoked, that should call rsm#fetchIndex
        // The cache max size is 2, it will remove one entry and keep the overall size to 2
        cache.getIndexEntry(metadataList.get(2));
        assertCacheSize(2);
        // Calling getIndex on the same entry may call rsm#fetchIndex or not, it depends on the cache implementation so
        // we only need to verify the number of calling is in our range.
        cache.getIndexEntry(metadataList.get(2));
        assertCacheSize(2);
        verifyFetchIndexInvocationWithRange(3, 4);

        // Close the cache
        cache.close();

        // Reload the cache from the disk and check the cache size is same as earlier
        RemoteIndexCache reloadedCache = new RemoteIndexCache(2 * estimateEntryBytesSize, rsm, logDir.toString());
        assertEquals(2, reloadedCache.internalCache().asMap().size());
        reloadedCache.close();

        verifyNoMoreInteractions(rsm);
    }

    @Test
    public void testRemoveItem() throws IOException {
        RemoteLogSegmentId segmentId = rlsMetadata.remoteLogSegmentId();
        Uuid segmentUuid = segmentId.id();
        // generate and add entry to cache
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry(segmentId);
        cache.internalCache().put(segmentUuid, spyEntry);
        assertTrue(cache.internalCache().asMap().containsKey(segmentUuid));
        assertFalse(spyEntry.isMarkedForCleanup());

        cache.remove(segmentId.id());
        assertFalse(cache.internalCache().asMap().containsKey(segmentUuid));
        assertTrue(spyEntry::isMarkedForCleanup, "Failed to mark cache entry for cleanup after remove");
    }

    @Test
    public void testRemoveNonExistentItem() throws IOException {
        // generate and add entry to cache
        RemoteLogSegmentId segmentId = rlsMetadata.remoteLogSegmentId();
        Uuid segmentUuid = segmentId.id();
        // generate and add entry to cache
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry(segmentId);
        cache.internalCache().put(segmentUuid, spyEntry);
        assertTrue(cache.internalCache().asMap().containsKey(segmentUuid));

        // remove a random Uuid
        cache.remove(Uuid.randomUuid());
        assertTrue(cache.internalCache().asMap().containsKey(segmentUuid));
        assertFalse(spyEntry.isMarkedForCleanup());
    }

    @Test
    public void testRemoveMultipleItems() throws IOException {
        // generate and add entry to cache
        Map<Uuid, RemoteIndexCache.Entry> uuidAndEntryList = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            RemoteLogSegmentId segmentId = RemoteLogSegmentId.generateNew(idPartition);
            Uuid segmentUuid = segmentId.id();
            RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry(segmentId);
            uuidAndEntryList.put(segmentUuid, spyEntry);

            cache.internalCache().put(segmentUuid, spyEntry);
            assertTrue(cache.internalCache().asMap().containsKey(segmentUuid));
            assertFalse(spyEntry.isMarkedForCleanup());
        }
        cache.removeAll(uuidAndEntryList.keySet());
        for (RemoteIndexCache.Entry entry : uuidAndEntryList.values()) {
            assertTrue(entry::isMarkedForCleanup, "Failed to mark cache entry for cleanup after removeAll");
        }
    }

    @Test
    public void testClearCacheAndIndexFilesWhenResizeCache() throws InterruptedException {
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(1, tpId);

        assertCacheSize(0);
        // getIndex for first time will call rsm#fetchIndex
        RemoteIndexCache.Entry cacheEntry = cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent());

        cache.resizeCacheSize(1L);

        // wait until entry is marked for deletion
        TestUtils.waitForCondition(cacheEntry::isMarkedForCleanup,
                "Failed to mark cache entry for cleanup after resizing cache.");
        TestUtils.waitForCondition(cacheEntry::isCleanStarted,
                "Failed to cleanup cache entry after resizing cache.");

        // verify no index files on remote cache dir
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isEmpty(),
                "Offset index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isEmpty(),
                "Txn index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isEmpty(),
                "Time index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty(),
                "Index file marked for deletion should not be present on disk at " + cache.cacheDir());

        assertCacheSize(0);
    }

    @Test
    public void testCorrectnessForCacheAndIndexFilesWhenResizeCache() throws IOException, InterruptedException, RemoteStorageException {
        // The test process for resizing is: put 1 entry -> evict to empty -> put 3 entries with limited capacity of 2 entries ->
        // evict to 1 entry -> resize to 1 entry size -> resize to 2 entries size
        long estimateEntryBytesSize = estimateOneEntryBytesSize();
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(3, tpId);

        assertCacheSize(0);
        // getIndex for first time will call rsm#fetchIndex
        RemoteIndexCache.Entry cacheEntry = cache.getIndexEntry(metadataList.get(0));
        assertCacheSize(1);
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent());

        // Reduce the cache size to 1 byte to ensure that all the entries are evicted from it.
        cache.resizeCacheSize(1L);

        // wait until entry is marked for deletion
        TestUtils.waitForCondition(cacheEntry::isMarkedForCleanup,
                "Failed to mark cache entry for cleanup after resizing cache.");
        TestUtils.waitForCondition(cacheEntry::isCleanStarted,
                "Failed to cleanup cache entry after resizing cache.");

        // verify no index files on remote cache dir
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isEmpty(),
                "Offset index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isEmpty(),
                "Txn index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isEmpty(),
                "Time index file should not be present on disk at " + cache.cacheDir());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty(),
                "Index file marked for deletion should not be present on disk at " + cache.cacheDir());

        assertCacheSize(0);

        // Increase cache capacity to only store 2 entries
        cache.resizeCacheSize(2 * estimateEntryBytesSize);
        assertCacheSize(0);

        RemoteIndexCache.Entry entry0 = cache.getIndexEntry(metadataList.get(0));
        RemoteIndexCache.Entry entry1 = cache.getIndexEntry(metadataList.get(1));
        RemoteIndexCache.Entry entry2 = cache.getIndexEntry(metadataList.get(2));
        List<RemoteIndexCache.Entry> entries = List.of(entry0, entry1, entry2);
        assertCacheSize(2);
        EvictionResult result = verifyEntryIsEvicted(metadataList, entries, 1);

        // Reduce cache capacity to only store 1 entry
        cache.resizeCacheSize(1 * estimateEntryBytesSize);
        assertCacheSize(1);
        // After resize, we need to check an entry is deleted from cache and the existing segmentMetadata
        List<RemoteIndexCache.Entry> entryInCache = entries.stream().filter(e -> !result.evictedEntries.contains(e)).toList();
        List<RemoteLogSegmentMetadata> updatedSegmentMetadata = metadataList.stream().filter(e -> !result.evictedSegmentMetadata.contains(e)).toList();
        verifyEntryIsEvicted(updatedSegmentMetadata, entryInCache, 1);

        // resize to the same size, all entries should be kept
        cache.resizeCacheSize(1 * estimateEntryBytesSize);

        List<RemoteLogSegmentMetadata> entriesKept = getRemoteLogSegMetadataIsKept(metadataList);
        // verify all existing entries (`cache.getIndexEntry(metadataList(2))`) are kept
        verifyEntryIsKept(entriesKept);
        assertCacheSize(1);

        // increase the size
        cache.resizeCacheSize(2 * estimateEntryBytesSize);

        // verify all entries are kept
        verifyEntryIsKept(entriesKept);
        assertCacheSize(1);
    }

    private List<RemoteLogSegmentMetadata> getRemoteLogSegMetadataIsKept(List<RemoteLogSegmentMetadata> metadataToVerify) {
        return metadataToVerify
                .stream()
                .filter(s -> cache.internalCache().asMap().containsKey(s.remoteLogSegmentId().id()))
                .toList();
    }

    record EvictionResult(List<RemoteLogSegmentMetadata> evictedSegmentMetadata, List<RemoteIndexCache.Entry> evictedEntries) { }

    private EvictionResult verifyEntryIsEvicted(List<RemoteLogSegmentMetadata> metadataToVerify, List<RemoteIndexCache.Entry> entriesToVerify, int numOfMarkAsDeleted) throws InterruptedException {
        TestUtils.waitForCondition(() -> entriesToVerify.stream().filter(RemoteIndexCache.Entry::isMarkedForCleanup).count() == numOfMarkAsDeleted,
                "Failed to mark evicted cache entry for cleanup after resizing cache.");

        TestUtils.waitForCondition(() -> entriesToVerify.stream().filter(RemoteIndexCache.Entry::isCleanStarted).count() == numOfMarkAsDeleted,
                "Failed to cleanup evicted cache entry after resizing cache.");

        List<RemoteIndexCache.Entry> entriesIsMarkedForCleanup = entriesToVerify.stream().filter(RemoteIndexCache.Entry::isMarkedForCleanup).toList();
        List<RemoteIndexCache.Entry> entriesIsCleanStarted = entriesToVerify.stream().filter(RemoteIndexCache.Entry::isCleanStarted).toList();
        // clean up entries and clean start entries should be the same
        assertEquals(entriesIsMarkedForCleanup, entriesIsCleanStarted);

        // get the logSegMetadata are evicted
        List<RemoteLogSegmentMetadata> metadataDeleted = metadataToVerify
                .stream()
                .filter(s -> !cache.internalCache().asMap().containsKey(s.remoteLogSegmentId().id()))
                .toList();
        assertEquals(numOfMarkAsDeleted, metadataDeleted.size());
        for (RemoteLogSegmentMetadata metadata : metadataDeleted) {
            // verify no index files for `entryToVerify` on remote cache dir
            TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, remoteOffsetIndexFileName(metadata)).isEmpty(),
                    "Offset index file for evicted entry should not be present on disk at " + cache.cacheDir());
            TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, remoteTimeIndexFileName(metadata)).isEmpty(),
                    "Time index file for evicted entry should not be present on disk at " + cache.cacheDir());
            TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, remoteTransactionIndexFileName(metadata)).isEmpty(),
                    "Txn index file for evicted entry should not be present on disk at " + cache.cacheDir());
            TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty(),
                    "Index file marked for deletion for evicted entry should not be present on disk at " + cache.cacheDir());
        }
        return new EvictionResult(metadataDeleted, entriesIsMarkedForCleanup);
    }

    private void verifyEntryIsKept(List<RemoteLogSegmentMetadata> metadataToVerify) {
        for (RemoteLogSegmentMetadata metadata : metadataToVerify) {
            assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteOffsetIndexFileName(metadata)).isPresent());
            assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteTimeIndexFileName(metadata)).isPresent());
            assertTrue(getIndexFileFromRemoteCacheDir(cache, remoteTransactionIndexFileName(metadata)).isPresent());
            assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty());
        }
    }

    @ParameterizedTest
    @EnumSource(value = IndexType.class, names = {"OFFSET", "TIMESTAMP", "TRANSACTION"})
    public void testCorruptCacheIndexFileExistsButNotInCache(IndexType indexType) throws IOException, RemoteStorageException {
        // create Corrupted Index File in remote index cache
        createCorruptedIndexFile(indexType, cache.cacheDir());
        RemoteIndexCache.Entry entry = cache.getIndexEntry(rlsMetadata);
        // Test would fail if it throws Exception other than CorruptIndexException
        Path offsetIndexFile = entry.offsetIndex().file().toPath();
        Path txnIndexFile = entry.txnIndex().file().toPath();
        Path timeIndexFile = entry.timeIndex().file().toPath();

        String expectedOffsetIndexFileName = remoteOffsetIndexFileName(rlsMetadata);
        String expectedTimeIndexFileName = remoteTimeIndexFileName(rlsMetadata);
        String expectedTxnIndexFileName = remoteTransactionIndexFileName(rlsMetadata);

        assertEquals(expectedOffsetIndexFileName, offsetIndexFile.getFileName().toString());
        assertEquals(expectedTxnIndexFileName, txnIndexFile.getFileName().toString());
        assertEquals(expectedTimeIndexFileName, timeIndexFile.getFileName().toString());

        // assert that parent directory for the index files is correct
        assertEquals(DIR_NAME, offsetIndexFile.getParent().getFileName().toString(),
                "offsetIndex=" + offsetIndexFile + " is created under incorrect parent");
        assertEquals(DIR_NAME, txnIndexFile.getParent().getFileName().toString(),
                "txnIndex=" + txnIndexFile + " is created under incorrect parent");
        assertEquals(DIR_NAME, timeIndexFile.getParent().getFileName().toString(),
                "timeIndex=" + timeIndexFile + " is created under incorrect parent");

        // file is corrupted it should fetch from remote storage again
        verifyFetchIndexInvocation(1);
    }

    @Test
    public void testConcurrentRemoveReadForCache1() throws IOException, InterruptedException, ExecutionException {
        // Create a spy Cache Entry
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry();
        cache.internalCache().put(rlsMetadata.remoteLogSegmentId().id(), spyEntry);
        assertCacheSize(1);

        CountDownLatch latchForCacheRead = new CountDownLatch(1);
        CountDownLatch latchForCacheRemove = new CountDownLatch(1);
        CountDownLatch latchForTestWait = new CountDownLatch(1);

        AtomicInteger cleanupCallCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            cleanupCallCount.incrementAndGet();

            if (cleanupCallCount.get() == 1) {
                // Signal the CacheRead to unblock itself
                latchForCacheRead.countDown();
                // Wait for signal to start deleting the renamed files
                latchForCacheRemove.await();
                // Calling the cleanup() actual method to remove the renamed files
                invocation.callRealMethod();
                // Signal TestWait to unblock itself so that test can be completed
                latchForTestWait.countDown();
            }
            return null;
        }).when(spyEntry).cleanup();

        Runnable removeCache = () -> cache.remove(rlsMetadata.remoteLogSegmentId().id());

        Runnable readCache = () -> {
            // Wait for signal to start CacheRead
            try {
                latchForCacheRead.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            cache.getIndexEntry(rlsMetadata);
            // Signal the CacheRemove to start renaming the files
            latchForCacheRemove.countDown();
        };
        executeConcurrentRemoveRead(removeCache, readCache, latchForTestWait);
    }

    @Test
    public void testConcurrentRemoveReadForCache2() throws IOException, InterruptedException, ExecutionException {
        RemoteIndexCache.Entry spyEntry = generateSpyCacheEntry();
        cache.internalCache().put(rlsMetadata.remoteLogSegmentId().id(), spyEntry);
        assertCacheSize(1);

        CountDownLatch latchForCacheRead = new CountDownLatch(1);
        CountDownLatch latchForCacheRemove = new CountDownLatch(1);
        CountDownLatch latchForTestWait = new CountDownLatch(1);

        AtomicInteger cleanupCallCount = new AtomicInteger(0);
        doAnswer((InvocationOnMock invocation) -> {
            cleanupCallCount.incrementAndGet();

            if (cleanupCallCount.get() == 1) {
                // Wait for signal to start renaming the files
                latchForCacheRemove.await();
                // Calling the cleanup() actual method to remove the renamed files
                invocation.callRealMethod();
                // Signal the CacheRead to unblock itself
                latchForCacheRead.countDown();
            }
            return null;
        }).when(spyEntry).cleanup();

        Runnable removeCache = () -> cache.remove(rlsMetadata.remoteLogSegmentId().id());

        Runnable readCache = () -> {
            // Wait for signal to start CacheRead
            latchForCacheRemove.countDown();
            try {
                latchForCacheRead.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            cache.getIndexEntry(rlsMetadata);
            // Signal TestWait to unblock itself so that test can be completed
            latchForTestWait.countDown();
        };
        executeConcurrentRemoveRead(removeCache, readCache, latchForTestWait);
    }

    private void executeConcurrentRemoveRead(Runnable removeCache,
                                             Runnable readCache,
                                             CountDownLatch latchForTestWait) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> removeCacheFuture = executor.submit(removeCache);
            Future<?> readCacheFuture = executor.submit(readCache);

            // Verify both tasks are completed without any exception
            removeCacheFuture.get();
            readCacheFuture.get();

            // Wait for signal to complete the test
            latchForTestWait.await();

            // Read or cleaner thread whichever goes first, the cache size should remain one:
            // 1. If reader thread runs first, then it will fetch the entry from remote since the previous entry in
            //    local disk was renamed with ".deleted" as suffix. The previous and current entry objects are different.
            //    And, the cleaner thread should only remove the files with suffix as ".deleted".
            // 2. If removal thread runs first, then it will remove the files with ".deleted" suffix. And, the reader
            //    thread will fetch the entry again from remote storage.
            assertCacheSize(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testMultipleIndexEntriesExecutionInCorruptException() throws IOException, RemoteStorageException {
        reset(rsm);
        when(rsm.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class))).thenAnswer(ans -> {
            RemoteLogSegmentMetadata metadata = ans.getArgument(0);
            IndexType indexType = ans.getArgument(1);
            OffsetIndex offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir);
            TimeIndex timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir);
            TransactionIndex txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir);
            maybeAppendIndexEntries(offsetIdx, timeIdx);
            // Create corrupted index file
            createCorruptTimeIndexOffsetFile(tpDir);
            return switch (indexType) {
                case OFFSET -> new FileInputStream(offsetIdx.file());
                case TIMESTAMP -> new FileInputStream(timeIdx.file());
                case TRANSACTION -> new FileInputStream(txnIdx.file());
                case LEADER_EPOCH -> null; // leader-epoch-cache is not accessed.
                case PRODUCER_SNAPSHOT -> null; // producer-snapshot is not accessed.
            };
        });

        assertThrows(CorruptIndexException.class, () -> cache.getIndexEntry(rlsMetadata));
        assertNull(cache.internalCache().getIfPresent(rlsMetadata.remoteLogSegmentId().id()));
        verifyFetchIndexInvocation(1, List.of(IndexType.OFFSET, IndexType.TIMESTAMP));
        verifyFetchIndexInvocation(0, List.of(IndexType.TRANSACTION));
        // Current status
        // (cache is null)
        // RemoteCacheDir contain
        // 1. Offset Index File is fine and not corrupted
        // 2. Time Index File is corrupted
        // What should be the code flow in next execution
        // 1. No rsm call for fetching OffSet Index File.
        // 2. Time index file should be fetched from remote storage again as it is corrupted in the first execution.
        // 3. Transaction index file should be fetched from remote storage.
        reset(rsm);
        // delete all files created in tpDir
        List<Path> paths = Files.walk(tpDir.toPath(), 1)
                .filter(Files::isRegularFile)
                .toList();
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
        // rsm should return no corrupted file in the 2nd execution
        mockRsmFetchIndex(rsm);
        cache.getIndexEntry(rlsMetadata);
        // rsm should not be called to fetch offset Index
        verifyFetchIndexInvocation(0, List.of(IndexType.OFFSET));
        verifyFetchIndexInvocation(1, List.of(IndexType.TIMESTAMP));
        // Transaction index would be fetched again
        // as previous getIndexEntry failed before fetchTransactionIndex
        verifyFetchIndexInvocation(1, List.of(IndexType.TRANSACTION));
    }

    @Test
    public void testIndexFileAlreadyExistOnDiskButNotInCache() throws InterruptedException, IOException, RemoteStorageException {
        File remoteIndexCacheDir = cache.cacheDir();
        String tempSuffix = ".tmptest";

        RemoteIndexCache.Entry entry = cache.getIndexEntry(rlsMetadata);
        verifyFetchIndexInvocation(1);
        // copy files with temporary name
        Path tmpOffsetIdxPath = Files.copy(entry.offsetIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.offsetIndex().file().getPath(), "", tempSuffix)));
        Path tmpTxnIdxPath = Files.copy(entry.txnIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.txnIndex().file().getPath(), "", tempSuffix)));
        Path tmpTimeIdxPath = Files.copy(entry.timeIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.timeIndex().file().getPath(), "", tempSuffix)));

        cache.remove(rlsMetadata.remoteLogSegmentId().id());

        // wait until entry is marked for deletion
        TestUtils.waitForCondition(entry::isMarkedForCleanup,
                "Failed to mark cache entry for cleanup after invalidation");
        TestUtils.waitForCondition(entry::isCleanStarted,
                "Failed to cleanup cache entry after invalidation");

        // restore index files
        renameRemoteCacheIndexFileFromDisk(tmpOffsetIdxPath, tmpTxnIdxPath, tmpTimeIdxPath, tempSuffix);
        // validate cache entry for the above key should be null
        assertNull(cache.internalCache().getIfPresent(rlsMetadata.remoteLogSegmentId().id()));
        cache.getIndexEntry(rlsMetadata);
        // Index  Files already exist ,rsm should not fetch them again.
        verifyFetchIndexInvocation(1);
        // verify index files on disk
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent(), "Offset index file should be present on disk at " + remoteIndexCacheDir.toPath());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent(), "Txn index file should be present on disk at " + remoteIndexCacheDir.toPath());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent(), "Time index file should be present on disk at " + remoteIndexCacheDir.toPath());
    }

    private void renameRemoteCacheIndexFileFromDisk(Path tmpOffsetIdxFile,
                                                    Path tmpTxnIdxFile,
                                                    Path tmpTimeIdxFile,
                                                    String tempSuffix) throws IOException {
        for (Path path : new Path[]{tmpOffsetIdxFile, tmpTxnIdxFile, tmpTimeIdxFile}) {
            Utils.atomicMoveWithFallback(path,
                    path.resolveSibling(path.getFileName().toString().replace(tempSuffix, "")));
        }
    }

    @ParameterizedTest
    @EnumSource(value = IndexType.class, names = {"OFFSET", "TIMESTAMP", "TRANSACTION"})
    public void testRSMReturnCorruptedIndexFile(IndexType testIndexType) throws RemoteStorageException {
        when(rsm.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class))).thenAnswer(ans -> {
            RemoteLogSegmentMetadata metadata = ans.getArgument(0);
            IndexType indexType = ans.getArgument(1);
            OffsetIndex offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir);
            TimeIndex timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir);
            TransactionIndex txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir);
            maybeAppendIndexEntries(offsetIdx, timeIdx);
            // Create corrupt index file return from RSM
            createCorruptedIndexFile(testIndexType, tpDir);
            return switch (indexType) {
                case OFFSET -> new FileInputStream(offsetIdx.file());
                case TIMESTAMP -> new FileInputStream(timeIdx.file());
                case TRANSACTION -> new FileInputStream(txnIdx.file());
                case LEADER_EPOCH -> null; // leader-epoch-cache is not accessed.
                case PRODUCER_SNAPSHOT -> null; // producer-snapshot is not accessed.
            };
        });
        assertThrows(CorruptIndexException.class, () -> cache.getIndexEntry(rlsMetadata));
    }

    @Test
    public void testConcurrentCacheDeletedFileExists() throws InterruptedException, IOException {
        File remoteIndexCacheDir = cache.cacheDir();

        RemoteIndexCache.Entry entry = cache.getIndexEntry(rlsMetadata);
        // verify index files on disk
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isPresent(), "Offset index file should be present on disk at " + remoteIndexCacheDir.toPath());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isPresent(), "Txn index file should be present on disk at " + remoteIndexCacheDir.toPath());
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isPresent(), "Time index file should be present on disk at " + remoteIndexCacheDir.toPath());

        // Simulating a concurrency issue where deleted files already exist on disk
        // This happens when cleanerThread is slow and not able to delete index entries
        // while same index Entry is cached again and invalidated.
        // The new deleted file created should be replaced by existing deleted file.

        // create deleted suffix file
        Files.copy(entry.offsetIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.offsetIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
        Files.copy(entry.txnIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.txnIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
        Files.copy(entry.timeIndex().file().toPath(), Paths.get(Utils.replaceSuffix(entry.timeIndex().file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));

        // verify deleted file exists on disk
        assertTrue(getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isPresent(), "Deleted Offset index file should be present on disk at " + remoteIndexCacheDir.toPath());

        cache.remove(rlsMetadata.remoteLogSegmentId().id());

        // wait until entry is marked for deletion
        TestUtils.waitForCondition(entry::isMarkedForCleanup,
                "Failed to mark cache entry for cleanup after invalidation");
        TestUtils.waitForCondition(entry::isCleanStarted,
                "Failed to cleanup cache entry after invalidation");

        // verify no index files on disk
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.INDEX_FILE_SUFFIX).isEmpty(),
                "Offset index file should not be present on disk at " + remoteIndexCacheDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TXN_INDEX_FILE_SUFFIX).isEmpty(),
                "Txn index file should not be present on disk at " + remoteIndexCacheDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.TIME_INDEX_FILE_SUFFIX).isEmpty(),
                "Time index file should not be present on disk at " + remoteIndexCacheDir.toPath());
        TestUtils.waitForCondition(() -> getIndexFileFromRemoteCacheDir(cache, LogFileUtils.DELETED_FILE_SUFFIX).isEmpty(),
                "Index file marked for deletion should not be present on disk at " + remoteIndexCacheDir.toPath());
    }

    @Test
    public void testDeleteInvalidIndexFilesOnInit() throws IOException {
        File cacheDir = cache.cacheDir();
        long baseOffset = 100L;
        UUID uuid = UUID.randomUUID();

        String invalidOffsetIdxFilename = String.format("%s_%s%s%s", baseOffset, uuid, LogFileUtils.INDEX_FILE_SUFFIX, LogFileUtils.DELETED_FILE_SUFFIX);
        File invalidOffsetIdxFile = new File(cacheDir, invalidOffsetIdxFilename);
        invalidOffsetIdxFile.createNewFile();

        String invalidTimeIdxFilename = String.format("%s_%s%s%s", baseOffset, uuid, LogFileUtils.TIME_INDEX_FILE_SUFFIX, ".tmp");
        File invalidTimeIndexFile = new File(cacheDir, invalidTimeIdxFilename);
        invalidTimeIndexFile.createNewFile();

        RemoteLogSegmentMetadata rlsMetadata = new RemoteLogSegmentMetadata(RemoteLogSegmentId.generateNew(idPartition), baseOffset + 100,
                lastOffset, time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L));
        OffsetIndex validOffsetIdx = createOffsetIndexForSegmentMetadata(rlsMetadata, logDir);
        TransactionIndex validTimeIdx = createTxIndexForSegmentMetadata(rlsMetadata, logDir);

        new RemoteIndexCache(defaultRemoteIndexCacheSizeBytes, rsm, logDir.toString());
        assertFalse(invalidOffsetIdxFile.exists());
        assertFalse(invalidTimeIndexFile.exists());
        assertTrue(validOffsetIdx.file().exists());
        assertTrue(validTimeIdx.file().exists());
    }

    @Test
    public void testFetchIndexAccessibleWhenMarkedForCleanup() throws IOException, RemoteStorageException {
        // setting the delayMs to a large value to disable file deletion by scheduler thread to have deterministic test
        cache.setFileDeleteDelayMs(300_000);
        File cacheDir = cache.cacheDir();

        Uuid segmentUuid = rlsMetadata.remoteLogSegmentId().id();
        RemoteIndexCache.Entry indexEntry = cache.getIndexEntry(rlsMetadata);
        cache.remove(segmentUuid);
        // Once marked for cleanup, the 3 index files should be renamed with ".deleted" suffix
        assertEquals(3, countFiles(cacheDir, name -> true));
        assertEquals(3, countFiles(cacheDir,
                name -> name.contains(segmentUuid.toString()) && name.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)));
        // Ensure that the `indexEntry` object still able to access the renamed index files after being marked for deletion
        OffsetPosition offsetPosition = indexEntry.offsetIndex().entry(2);
        assertEquals(offsetPosition.position(), indexEntry.lookupOffset(offsetPosition.offset()).position());
        assertNull(cache.internalCache().asMap().get(segmentUuid));
        verifyFetchIndexInvocation(1);

        // Once the entry gets removed from cache, the subsequent call to the cache should re-fetch the entry from remote.
        assertEquals(offsetPosition.position(), cache.lookupOffset(rlsMetadata, offsetPosition.offset()));
        verifyFetchIndexInvocation(2);
        RemoteIndexCache.Entry indexEntry2 = cache.getIndexEntry(rlsMetadata);
        assertNotNull(indexEntry2);
        verifyFetchIndexInvocation(2);
        // There will be 6 files in the remote-log-index-cache dir: 3 original index files and 3 files with ".deleted" suffix
        assertEquals(6, countFiles(cacheDir, name -> true));
        assertEquals(3, countFiles(cacheDir,
                name -> name.contains(segmentUuid.toString()) && !name.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)));
        assertEquals(3, countFiles(cacheDir,
                name -> name.contains(segmentUuid.toString()) && name.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)));

        // Once the indexEntry2 is marked for cleanup, the 3 index files should be renamed with ".deleted" suffix.
        // Both indexEntry and indexEntry2 should be able to access the renamed index files.
        cache.remove(segmentUuid);
        assertEquals(3, countFiles(cacheDir, name -> true));
        assertEquals(3, countFiles(cacheDir,
                name -> name.contains(segmentUuid.toString()) && name.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)));
        assertEquals(offsetPosition.position(), indexEntry.lookupOffset(offsetPosition.offset()).position());
        assertEquals(offsetPosition.position(), indexEntry2.lookupOffset(offsetPosition.offset()).position());

        indexEntry.cleanup();
        assertEquals(0, countFiles(cacheDir, name -> true));
        assertThrows(IllegalStateException.class, () -> indexEntry.lookupOffset(offsetPosition.offset()));
        assertEquals(offsetPosition.position(), indexEntry2.lookupOffset(offsetPosition.offset()).position());

        indexEntry2.cleanup();
        assertEquals(0, countFiles(cacheDir, name -> true));
        assertThrows(IllegalStateException.class, () -> indexEntry.lookupOffset(offsetPosition.offset()));
        assertThrows(IllegalStateException.class, () -> indexEntry2.lookupOffset(offsetPosition.offset()));
    }

    private int countFiles(File cacheDir, Predicate<String> condition) {
        return Objects.requireNonNull(cacheDir.listFiles((dir, name) -> condition.test(name))).length;
    }

    private RemoteIndexCache.Entry generateSpyCacheEntry() throws IOException {
        return generateSpyCacheEntry(RemoteLogSegmentId.generateNew(idPartition));
    }

    private RemoteIndexCache.Entry generateSpyCacheEntry(RemoteLogSegmentId remoteLogSegmentId) throws IOException {
        return generateSpyCacheEntry(remoteLogSegmentId, new File(logDir, DIR_NAME));
    }

    private RemoteIndexCache.Entry generateSpyCacheEntry(RemoteLogSegmentId remoteLogSegmentId,
                                                         File dir) throws IOException {
        RemoteLogSegmentMetadata rlsMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId, baseOffset,
                lastOffset, time.milliseconds(), brokerId, time.milliseconds(),
                segmentSize, Collections.singletonMap(0, 0L));
        TimeIndex timeIndex = spy(createTimeIndexForSegmentMetadata(rlsMetadata, dir));
        TransactionIndex txIndex = spy(createTxIndexForSegmentMetadata(rlsMetadata, dir));
        OffsetIndex offsetIndex = spy(createOffsetIndexForSegmentMetadata(rlsMetadata, dir));
        return spy(new RemoteIndexCache.Entry(offsetIndex, timeIndex, txIndex));
    }

    private void assertAtLeastOnePresent(RemoteIndexCache cache, Uuid... uuids) {
        for (Uuid uuid : uuids) {
            if (cache.internalCache().asMap().containsKey(uuid)) return;
        }
        fail("all uuids are not present in cache");
    }

    private void assertCacheSize(int expectedSize) throws InterruptedException {
        // Cache may grow beyond the size temporarily while evicting, hence, run in a loop to validate
        // that cache reaches correct state eventually
        TestUtils.waitForCondition(() -> cache.internalCache().asMap().size() == expectedSize,
                "cache did not adhere to expected size of " + expectedSize);
    }

    private void verifyFetchIndexInvocation(int count) throws RemoteStorageException {
        verifyFetchIndexInvocation(count, List.of(IndexType.OFFSET, IndexType.TIMESTAMP, IndexType.TRANSACTION));
    }

    private void verifyFetchIndexInvocation(int count, List<IndexType> indexTypes) throws RemoteStorageException {
        for (IndexType indexType : indexTypes) {
            verify(rsm, times(count)).fetchIndex(any(RemoteLogSegmentMetadata.class), eq(indexType));
        }
    }

    private void verifyFetchIndexInvocationWithRange(int lower, int upper) throws RemoteStorageException {
        List<IndexType> types = List.of(IndexType.OFFSET, IndexType.TIMESTAMP, IndexType.TRANSACTION);
        for (IndexType indexType : types) {
            verify(rsm, atLeast(lower)).fetchIndex(any(RemoteLogSegmentMetadata.class), eq(indexType));
            verify(rsm, atMost(upper)).fetchIndex(any(RemoteLogSegmentMetadata.class), eq(indexType));
        }
    }

    private TransactionIndex createTxIndexForSegmentMetadata(RemoteLogSegmentMetadata metadata, File dir) throws IOException {
        File txnIdxFile = remoteTransactionIndexFile(dir, metadata);
        txnIdxFile.createNewFile();
        return new TransactionIndex(metadata.startOffset(), txnIdxFile);
    }

    private void createCorruptTxnIndexForSegmentMetadata(File dir, RemoteLogSegmentMetadata metadata) throws IOException {
        File txnIdxFile = remoteTransactionIndexFile(dir, metadata);
        txnIdxFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(metadata.startOffset(), txnIdxFile);
        List<AbortedTxn> abortedTxns = List.of(
                new AbortedTxn(0L, 0, 10, 11),
                new AbortedTxn(1L, 5, 15, 13),
                new AbortedTxn(2L, 18, 35, 25),
                new AbortedTxn(3L, 32, 50, 40));
        for (AbortedTxn abortedTxn : abortedTxns) {
            txnIndex.append(abortedTxn);
        }
        txnIndex.close();

        // open the index with a different starting offset to fake invalid data
        new TransactionIndex(100L, txnIdxFile);
    }

    private TimeIndex createTimeIndexForSegmentMetadata(RemoteLogSegmentMetadata metadata, File dir) throws IOException {
        int maxEntries = (int) (metadata.endOffset() - metadata.startOffset());
        return new TimeIndex(remoteTimeIndexFile(dir, metadata), metadata.startOffset(), maxEntries * 12);
    }

    private OffsetIndex createOffsetIndexForSegmentMetadata(RemoteLogSegmentMetadata metadata, File dir) throws IOException {
        int maxEntries = (int) (metadata.endOffset() - metadata.startOffset());
        return new OffsetIndex(remoteOffsetIndexFile(dir, metadata), metadata.startOffset(), maxEntries * 8);
    }

    private List<RemoteLogSegmentMetadata> generateRemoteLogSegmentMetadata(int size, TopicIdPartition tpId) {
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            metadataList.add(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tpId, Uuid.randomUuid()), baseOffset * i, baseOffset * i + 10, time.milliseconds(), brokerId, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L)));
        }
        return metadataList;
    }

    private void maybeAppendIndexEntries(OffsetIndex offsetIndex, TimeIndex timeIndex) {
        if (!offsetIndex.isFull()) {
            long curTime = time.milliseconds();
            for (int i = 0; i < offsetIndex.maxEntries(); i++) {
                long offset = offsetIndex.baseOffset() + i;
                offsetIndex.append(offset, i);
                timeIndex.maybeAppend(curTime + i, offset, true);
            }
            offsetIndex.flush();
            timeIndex.flush();
        }
    }

    private long estimateOneEntryBytesSize() throws IOException, RemoteStorageException {
        TopicPartition tp = new TopicPartition("estimate-entry-bytes-size", 0);
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        mockRsmFetchIndex(rsm);
        RemoteIndexCache cache = new RemoteIndexCache(2L, rsm, logDir.toString());
        List<RemoteLogSegmentMetadata> metadataList = generateRemoteLogSegmentMetadata(1, tpId);
        RemoteIndexCache.Entry entry = cache.getIndexEntry(metadataList.get(0));
        long entrySizeInBytes = entry.entrySizeBytes();
        entry.markForCleanup();
        entry.cleanup();
        Utils.closeQuietly(cache, "RemoteIndexCache created for estimating entry size");
        return entrySizeInBytes;
    }

    private void mockRsmFetchIndex(RemoteStorageManager rsm) throws RemoteStorageException {
        when(rsm.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class))).thenAnswer(ans -> {
            RemoteLogSegmentMetadata metadata = ans.getArgument(0);
            IndexType indexType = ans.getArgument(1);
            OffsetIndex offsetIdx = createOffsetIndexForSegmentMetadata(metadata, tpDir);
            TimeIndex timeIdx = createTimeIndexForSegmentMetadata(metadata, tpDir);
            TransactionIndex txnIdx = createTxIndexForSegmentMetadata(metadata, tpDir);
            maybeAppendIndexEntries(offsetIdx, timeIdx);
            return switch (indexType) {
                case OFFSET -> new FileInputStream(offsetIdx.file());
                case TIMESTAMP -> new FileInputStream(timeIdx.file());
                case TRANSACTION -> new FileInputStream(txnIdx.file());
                case LEADER_EPOCH -> null; // leader-epoch-cache is not accessed.
                case PRODUCER_SNAPSHOT -> null; // producer-snapshot is not accessed.
            };
        });
    }

    private void createCorruptOffsetIndexFile(File dir) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(remoteOffsetIndexFile(dir, rlsMetadata));
        pw.write("Hello, world");
        // The size of the string written in the file is 12 bytes,
        // but it should be multiple of Offset Index EntrySIZE which is equal to 8.
        pw.close();
    }

    private void createCorruptTimeIndexOffsetFile(File dir) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(remoteTimeIndexFile(dir, rlsMetadata));
        pw.write("Hello, world1");
        // The size of the string written in the file is 13 bytes,
        // but it should be multiple of Time Index EntrySIZE which is equal to 12.
        pw.close();
    }

    private void createCorruptedIndexFile(RemoteStorageManager.IndexType indexType, File dir) throws IOException {
        if (indexType == RemoteStorageManager.IndexType.OFFSET) {
            createCorruptOffsetIndexFile(dir);
        } else if (indexType == IndexType.TIMESTAMP) {
            createCorruptTimeIndexOffsetFile(dir);
        } else if (indexType == IndexType.TRANSACTION) {
            createCorruptTxnIndexForSegmentMetadata(dir, rlsMetadata);
        }
    }

    private Optional<Path> getIndexFileFromRemoteCacheDir(RemoteIndexCache cache, String suffix) {
        try {
            return Files.walk(cache.cacheDir().toPath())
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(suffix))
            .findAny();
        } catch (IOException exc) {
            return Optional.empty();
        }
    }

    private Set<Thread> getRunningCleanerThread() {
        return Thread.getAllStackTraces().keySet()
                .stream()
                .filter(t -> t.isAlive() && t.getName().startsWith(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD))
                .collect(Collectors.toSet());
    }
}
