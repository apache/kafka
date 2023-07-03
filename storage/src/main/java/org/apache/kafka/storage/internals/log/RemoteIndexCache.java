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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.util.ShutdownableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.storage.internals.log.LogFileUtils.INDEX_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.TIME_INDEX_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.TXN_INDEX_FILE_SUFFIX;

/**
 * This is a LFU (Least Frequently Used) cache of remote index files stored in `$logdir/remote-log-index-cache`.
 * This is helpful to avoid re-fetching the index files like offset, time indexes from the remote storage for every
 * fetch call. The cache is re-initialized from the index files on disk on startup, if the index files are available.
 *
 * The cache contains a garbage collection thread which will delete the files for entries that have been removed from
 * the cache.
 *
 * Note that closing this cache does not delete the index files on disk.
 * Note that the cache eviction policy is based on the default implementation of Caffeine i.e.
 * <a href="https://github.com/ben-manes/caffeine/wiki/Efficiency">Window TinyLfu</a>. TinyLfu relies on a frequency
 * sketch to probabilistically estimate the historic usage of an entry.
 *
 */
public class RemoteIndexCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RemoteIndexCache.class);

    public static final String DIR_NAME = "remote-log-index-cache";

    private static final String TMP_FILE_SUFFIX = ".tmp";

    public static final String REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD = "remote-log-index-cleaner";

    /**
     * Directory where the index files will be stored on disk.
     */
    private final File cacheDir;

    /**
     * Represents if the cache is closed or not. Closing the cache is an irreversible operation.
     */
    private final AtomicBoolean isRemoteIndexCacheClosed = new AtomicBoolean(false);

    /**
     * Unbounded queue containing the removed entries from the cache which are waiting to be garbage collected.
     */
    private final LinkedBlockingQueue<Entry> expiredIndexes = new LinkedBlockingQueue<>();

    /**
     * Lock used to synchronize close with other read operations. This ensures that when we close, we don't have any other
     * concurrent reads in-progress.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Actual cache implementation that this file wraps around.
     *
     * The requirements for this internal cache is as follows:
     * 1. Multiple threads should be able to read concurrently.
     * 2. Fetch for missing keys should not block read for available keys.
     * 3. Only one thread should fetch for a specific key.
     * 4. Should support LRU-like policy.
     *
     * We use [[Caffeine]] cache instead of implementing a thread safe LRU cache on our own.
     *
     * Visible for testing.
     */
    private final Cache<Uuid, Entry> internalCache;
    private final RemoteStorageManager remoteStorageManager;
    private final ShutdownableThread cleanerThread;

    public RemoteIndexCache(RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this(1024, remoteStorageManager, logDir);
    }

    /**
     * Creates RemoteIndexCache with the given configs.
     *
     * @param maxSize              maximum number of segment index entries to be cached.
     * @param remoteStorageManager RemoteStorageManager instance, to be used in fetching indexes.
     * @param logDir               log directory
     */
    public RemoteIndexCache(int maxSize, RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this.remoteStorageManager = remoteStorageManager;
        cacheDir = new File(logDir, DIR_NAME);

        internalCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                // removeListener is invoked when either the entry is invalidated (means manual removal by the caller) or
                // evicted (means removal due to the policy)
                .removalListener((Uuid key, Entry entry, RemovalCause cause) -> {
                    // Mark the entries for cleanup and add them to the queue to be garbage collected later by the background thread.
                    try {
                        entry.markForCleanup();
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                    if (!expiredIndexes.offer(entry)) {
                        log.error("Error while inserting entry {} into the cleaner queue", entry);
                    }
                }).build();

        init();

        // Start cleaner thread that will clean the expired entries.
        cleanerThread = createCleanerThread();
        cleanerThread.start();
    }

    public LinkedBlockingQueue<Entry> expiredIndexes() {
        return expiredIndexes;
    }

    public Cache<Uuid, Entry> internalCache() {
        return internalCache;
    }

    public ShutdownableThread cleanerThread() {
        return cleanerThread;
    }

    private ShutdownableThread createCleanerThread() {
        ShutdownableThread thread = new ShutdownableThread("remote-log-index-cleaner") {
            public void doWork() {
                while (!isRemoteIndexCacheClosed.get()) {
                    try {
                        Entry entry = expiredIndexes.take();
                        log.info("Cleaning up index entry {}", entry);
                        entry.cleanup();
                    } catch (InterruptedException ex) {
                        // cleaner thread should only be interrupted when cache is being closed, else it's an error
                        if (!isRemoteIndexCacheClosed.get()) {
                            log.error("Cleaner thread received interruption but remote index cache is not closed", ex);
                            throw new KafkaException(ex);
                        } else {
                            log.debug("Cleaner thread was interrupted on cache shutdown");
                        }
                    } catch (Exception ex) {
                        log.error("Error occurred while fetching/cleaning up expired entry", ex);
                    }
                }
            }
        };
        thread.setDaemon(true);

        return thread;
    }

    private void init() throws IOException {
        try {
            Files.createDirectory(cacheDir.toPath());
            log.info("Created new file {} for RemoteIndexCache", cacheDir);
        } catch (FileAlreadyExistsException e) {
            log.info("RemoteIndexCache directory {} already exists. Re-using the same directory.", cacheDir);
        } catch (Exception e) {
            log.error("Unable to create directory {} for RemoteIndexCache.", cacheDir, e);
            throw new KafkaException(e);
        }

        // Delete any .deleted files remained from the earlier run of the broker.
        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            paths.forEach(path -> {
                if (path.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
            });
        }

        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            Iterator<Path> iterator = paths.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                Path fileNamePath = path.getFileName();
                if (fileNamePath == null)
                    throw new KafkaException("Empty file name in remote index cache directory: " + cacheDir);

                String fileNameStr = fileNamePath.toString();
                String name = fileNameStr.substring(0, fileNameStr.lastIndexOf("_") + 1);

                // Create entries for each path if all the index files exist.
                int firstIndex = name.indexOf('_');
                int offset = Integer.parseInt(name.substring(0, firstIndex));
                Uuid uuid = Uuid.fromString(name.substring(firstIndex + 1, name.lastIndexOf('_')));

                // It is safe to update the internalCache non-atomically here since this function is always called by a single
                // thread only.
                if (!internalCache.asMap().containsKey(uuid)) {
                    File offsetIndexFile = new File(cacheDir, name + INDEX_FILE_SUFFIX);
                    File timestampIndexFile = new File(cacheDir, name + TIME_INDEX_FILE_SUFFIX);
                    File txnIndexFile = new File(cacheDir, name + TXN_INDEX_FILE_SUFFIX);

                    if (offsetIndexFile.exists() && timestampIndexFile.exists() && txnIndexFile.exists()) {

                        OffsetIndex offsetIndex = new OffsetIndex(offsetIndexFile, offset, Integer.MAX_VALUE, false);
                        offsetIndex.sanityCheck();

                        TimeIndex timeIndex = new TimeIndex(timestampIndexFile, offset, Integer.MAX_VALUE, false);
                        timeIndex.sanityCheck();

                        TransactionIndex txnIndex = new TransactionIndex(offset, txnIndexFile);
                        txnIndex.sanityCheck();

                        Entry entry = new Entry(offsetIndex, timeIndex, txnIndex);
                        internalCache.put(uuid, entry);
                    } else {
                        // Delete all of them if any one of those indexes is not available for a specific segment id
                        tryAll(Arrays.asList(
                                () -> {
                                    Files.deleteIfExists(offsetIndexFile.toPath());
                                    return null;
                                },
                                () -> {
                                    Files.deleteIfExists(timestampIndexFile.toPath());
                                    return null;
                                },
                                () -> {
                                    Files.deleteIfExists(txnIndexFile.toPath());
                                    return null;
                                }));
                    }
                }
            }
        }
    }

    private <T> T loadIndexFile(String fileName, String suffix, RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                Function<RemoteLogSegmentMetadata, InputStream> fetchRemoteIndex,
                                Function<File, T> readIndex) throws IOException {
        File indexFile = new File(cacheDir, fileName + suffix);
        T index = null;
        if (indexFile.exists()) {
            try {
                index = readIndex.apply(indexFile);
            } catch (CorruptRecordException ex) {
                log.info("Error occurred while loading the stored index", ex);
            }
        }

        if (index == null) {
            File tmpIndexFile = new File(indexFile.getParentFile(), indexFile.getName() + RemoteIndexCache.TMP_FILE_SUFFIX);

            try (InputStream inputStream = fetchRemoteIndex.apply(remoteLogSegmentMetadata);) {
                Files.copy(inputStream, tmpIndexFile.toPath());
            }

            Utils.atomicMoveWithFallback(tmpIndexFile.toPath(), indexFile.toPath(), false);
            index = readIndex.apply(indexFile);
        }

        return index;
    }

    public Entry getIndexEntry(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        if (isRemoteIndexCacheClosed.get()) throw new IllegalStateException("Unable to fetch index for " +
                "segment id=" + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ". Instance is already closed.");

        lock.readLock().lock();
        try {
            Uuid cacheKey = remoteLogSegmentMetadata.remoteLogSegmentId().id();
            return internalCache.get(cacheKey, (Uuid uuid) -> {
                long startOffset = remoteLogSegmentMetadata.startOffset();
                // uuid.toString uses URL encoding which is safe for filenames and URLs.
                String fileName = startOffset + "_" + uuid.toString() + "_";

                try {
                    OffsetIndex offsetIndex = loadIndexFile(fileName, INDEX_FILE_SUFFIX, remoteLogSegmentMetadata, rlsMetadata -> {
                        try {
                            return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET);
                        } catch (RemoteStorageException e) {
                            throw new KafkaException(e);
                        }
                    }, file -> {
                        try {
                            OffsetIndex index = new OffsetIndex(file, startOffset, Integer.MAX_VALUE, false);
                            index.sanityCheck();
                            return index;
                        } catch (IOException e) {
                            throw new KafkaException(e);
                        }
                    });

                    TimeIndex timeIndex = loadIndexFile(fileName, TIME_INDEX_FILE_SUFFIX, remoteLogSegmentMetadata, rlsMetadata -> {
                        try {
                            return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP);
                        } catch (RemoteStorageException e) {
                            throw new KafkaException(e);
                        }
                    }, file -> {
                        try {
                            TimeIndex index = new TimeIndex(file, startOffset, Integer.MAX_VALUE, false);
                            index.sanityCheck();
                            return index;
                        } catch (IOException e) {
                            throw new KafkaException(e);
                        }
                    });

                    TransactionIndex txnIndex = loadIndexFile(fileName, TXN_INDEX_FILE_SUFFIX, remoteLogSegmentMetadata, rlsMetadata -> {
                        try {
                            return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TRANSACTION);
                        } catch (RemoteStorageException e) {
                            throw new KafkaException(e);
                        }
                    }, file -> {
                        try {
                            TransactionIndex index = new TransactionIndex(startOffset, file);
                            index.sanityCheck();
                            return index;
                        } catch (IOException e) {
                            throw new KafkaException(e);
                        }
                    });

                    return new Entry(offsetIndex, timeIndex, txnIndex);
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    public int lookupOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
        lock.readLock().lock();
        try {
            return getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int lookupTimestamp(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long timestamp, long startingOffset) throws IOException {
        lock.readLock().lock();
        try {
            return getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() {
        // Make close idempotent and ensure no more reads allowed from henceforth. The in-progress reads will continue to
        // completion (release the read lock) and then close will begin executing. Cleaner thread will immediately stop work.
        if (!isRemoteIndexCacheClosed.getAndSet(true)) {
            lock.writeLock().lock();
            try {
                log.info("Close initiated for RemoteIndexCache. Cache stats={}. Cache entries pending delete={}",
                        internalCache.stats(), expiredIndexes.size());
                // Initiate shutdown for cleaning thread
                boolean shutdownRequired = cleanerThread.initiateShutdown();
                // Close all the opened indexes to force unload mmap memory. This does not delete the index files from disk.
                internalCache.asMap().forEach((uuid, entry) -> entry.close());
                // wait for cleaner thread to shutdown
                if (shutdownRequired) cleanerThread.awaitShutdown();

                // Note that internal cache does not require explicit cleaning/closing. We don't want to invalidate or cleanup
                // the cache as both would lead to triggering of removal listener.

                log.info("Close completed for RemoteIndexCache");
            } catch (InterruptedException e) {
                throw new KafkaException(e);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public Map<Uuid, Entry> entries() {
        return Collections.unmodifiableMap(internalCache.asMap());
    }

    public static class Entry {

        private final OffsetIndex offsetIndex;
        private final TimeIndex timeIndex;
        private final TransactionIndex txnIndex;

        // This lock is used to synchronize cleanup methods and read methods. This ensures that cleanup (which changes the
        // underlying files of the index) isn't performed while a read is in-progress for the entry. This is required in
        // addition to using the thread safe cache because, while the thread safety of the cache ensures that we can read
        // entries concurrently, it does not ensure that we won't mutate underlying files belonging to an entry.
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        // Visible for testing
        private boolean cleanStarted = false;

        // Visible for testing
        private boolean markedForCleanup = false;

        public Entry(OffsetIndex offsetIndex, TimeIndex timeIndex, TransactionIndex txnIndex) {
            this.offsetIndex = offsetIndex;
            this.timeIndex = timeIndex;
            this.txnIndex = txnIndex;
        }

        public OffsetIndex offsetIndex() {
            return offsetIndex;
        }

        public TimeIndex timeIndex() {
            return timeIndex;
        }

        public TransactionIndex txnIndex() {
            return txnIndex;
        }

        // Visible for testing
        public boolean isCleanStarted() {
            return cleanStarted;
        }

        // Visible for testing
        public boolean isMarkedForCleanup() {
            return markedForCleanup;
        }

        public OffsetPosition lookupOffset(long targetOffset) {
            lock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");
                else return offsetIndex.lookup(targetOffset);
            } finally {
                lock.readLock().unlock();
            }
        }

        public OffsetPosition lookupTimestamp(long timestamp, long startingOffset) throws IOException {
            lock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");

                TimestampOffset timestampOffset = timeIndex.lookup(timestamp);
                return offsetIndex.lookup(Math.max(startingOffset, timestampOffset.offset));
            } finally {
                lock.readLock().unlock();
            }
        }

        public void markForCleanup() throws IOException {
            lock.writeLock().lock();
            try {
                if (!markedForCleanup) {
                    markedForCleanup = true;
                    offsetIndex.renameTo(new File(Utils.replaceSuffix(offsetIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                    timeIndex.renameTo(new File(Utils.replaceSuffix(timeIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                    txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void cleanup() throws IOException {
            markForCleanup();
            // no-op if clean is done already
            if (!cleanStarted) {
                cleanStarted = true;

                List<StorageAction<Void, Exception>> actions = Arrays.asList(() -> {
                    offsetIndex.deleteIfExists();
                    return null;
                }, () -> {
                    timeIndex.deleteIfExists();
                    return null;
                }, () -> {
                    txnIndex.deleteIfExists();
                    return null;
                });

                tryAll(actions);
            }
        }

        public void close() {
            Utils.closeQuietly(offsetIndex, "OffsetIndex");
            Utils.closeQuietly(timeIndex, "TimeIndex");
            Utils.closeQuietly(txnIndex, "TransactionIndex");
        }
    }

    /**
     * Executes each entry in `actions` list even if one or more throws an exception. If any of them throws
     * an IOException, it will be rethrown and adds all other encountered exceptions as suppressed to that IOException.
     * Otherwise, it throws KafkaException wrapped with the first exception and the remaining exceptions are added as
     * suppressed to the KafkaException.
     *
     * @param actions actions to be executes
     * @throws IOException Any IOException encountered while executing those actions.
     */
    private static void tryAll(List<StorageAction<Void, Exception>> actions) throws IOException {
        IOException ioException = null;
        List<Exception> exceptions = Collections.emptyList();
        for (StorageAction<Void, Exception> action : actions) {
            try {
                action.execute();
            } catch (IOException e) {
                if (ioException == null) {
                    ioException = e;
                } else {
                    if (exceptions.isEmpty()) {
                        exceptions = new ArrayList<>();
                    }
                    exceptions.add(e);
                }
            } catch (Exception e) {
                if (exceptions.isEmpty()) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(e);
            }
        }

        if (ioException != null) {
            for (Exception exception : exceptions) {
                ioException.addSuppressed(exception);
            }
            throw ioException;
        } else if (!exceptions.isEmpty()) {
            Iterator<Exception> iterator = exceptions.iterator();
            KafkaException kafkaException = new KafkaException(iterator.next());
            while (iterator.hasNext()) {
                kafkaException.addSuppressed(iterator.next());
            }

            throw kafkaException;
        }
    }
}
