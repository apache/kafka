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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.util.ShutdownableThread;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
 * This class is thread safe.
 */
public class RemoteIndexCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RemoteIndexCache.class);
    private static final String TMP_FILE_SUFFIX = ".tmp";
    public static final String REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD = "remote-log-index-cleaner";
    public static final String DIR_NAME = "remote-log-index-cache";

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
    private final RemoteStorageManager remoteStorageManager;
    private final ShutdownableThread cleanerThread;

    /**
     * Actual cache implementation that this file wraps around.
     *
     * The requirements for this internal cache is as follows:
     * 1. Multiple threads should be able to read concurrently.
     * 2. Fetch for missing keys should not block read for available keys.
     * 3. Only one thread should fetch for a specific key.
     * 4. Should support LRU-like policy.
     *
     * We use {@link Caffeine} cache instead of implementing a thread safe LRU cache on our own.
     */
    private final Cache<Uuid, Entry> internalCache;

    /**
     * Creates RemoteIndexCache with the given configs.
     *
     * @param maxSize              maximum bytes size of segment index entries to be cached.
     * @param remoteStorageManager RemoteStorageManager instance, to be used in fetching indexes.
     * @param logDir               log directory
     */
    public RemoteIndexCache(long maxSize, RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this.remoteStorageManager = remoteStorageManager;
        cacheDir = new File(logDir, DIR_NAME);

        internalCache = initEmptyCache(maxSize);
        init();

        // Start cleaner thread that will clean the expired entries.
        cleanerThread = createCleanerThread();
        cleanerThread.start();
    }

    public void resizeCacheSize(long remoteLogIndexFileCacheSize) {
        lock.writeLock().lock();
        try {
            internalCache.policy().eviction().orElseThrow(() -> new NoSuchElementException("No eviction policy is set for the remote index cache.")
            ).setMaximum(remoteLogIndexFileCacheSize);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Cache<Uuid, Entry> initEmptyCache(long maxSize) {
        return Caffeine.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Uuid key, Entry entry) -> {
                    return (int) entry.entrySizeBytes;
                })
                // This listener is invoked each time an entry is being automatically removed due to eviction. The cache will invoke this listener
                // during the atomic operation to remove the entry (refer: https://github.com/ben-manes/caffeine/wiki/Removal),
                // hence, care must be taken to ensure that this operation is not expensive. Note that this listener is not invoked when
                // RemovalCause from cache is EXPLICIT or REPLACED (e.g. on Cache.invalidate(), Cache.put() etc.) For a complete list see:
                // https://github.com/ben-manes/caffeine/blob/0cef55168986e3816314e7fdba64cb0b996dd3cc/caffeine/src/main/java/com/github/benmanes/caffeine/cache/RemovalCause.java#L23
                // Hence, any operation required after removal from cache must be performed manually for these scenarios.
                .evictionListener((Uuid key, Entry entry, RemovalCause cause) -> {
                    // Mark the entries for cleanup and add them to the queue to be garbage collected later by the background thread.
                    if (entry != null) {
                        enqueueEntryForCleanup(entry, key);
                    } else {
                        log.error("Received entry as null for key {} when the it is removed from the cache.", key);
                    }
                }).build();
    }

    public Collection<Entry> expiredIndexes() {
        return Collections.unmodifiableCollection(expiredIndexes);
    }

    // Visible for testing
    public Cache<Uuid, Entry> internalCache() {
        return internalCache;
    }

    // Visible for testing
    public File cacheDir() {
        return cacheDir;
    }

    public void remove(Uuid key) {
        lock.readLock().lock();
        try {
            internalCache.asMap().computeIfPresent(key, (k, v) -> {
                enqueueEntryForCleanup(v, k);
                // Returning null to remove the key from the cache
                return null;
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    public void removeAll(Collection<Uuid> keys) {
        lock.readLock().lock();
        try {
            keys.forEach(key -> internalCache.asMap().computeIfPresent(key, (k, v) -> {
                enqueueEntryForCleanup(v, k);
                // Returning null to remove the key from the cache
                return null;
            }));
        } finally {
            lock.readLock().unlock();
        }
    }

    private void enqueueEntryForCleanup(Entry entry, Uuid key) {
        try {
            entry.markForCleanup();
            if (!expiredIndexes.offer(entry)) {
                log.error("Error while inserting entry {} for key {} into the cleaner queue because queue is full.", entry, key);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    // Visible for testing
    public ShutdownableThread cleanerThread() {
        return cleanerThread;
    }

    private ShutdownableThread createCleanerThread() {
        ShutdownableThread thread = new ShutdownableThread(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD) {
            public void doWork() {
                try {
                    Entry entry = expiredIndexes.take();
                    log.debug("Cleaning up index entry {}", entry);
                    entry.cleanup();
                } catch (InterruptedException ie) {
                    // cleaner thread should only be interrupted when cache is being closed, else it's an error
                    if (!isRemoteIndexCacheClosed.get()) {
                        log.error("Cleaner thread received interruption but remote index cache is not closed", ie);
                        // propagate the InterruptedException outside to correctly close the thread.
                        throw new KafkaException(ie);
                    } else {
                        log.debug("Cleaner thread was interrupted on cache shutdown");
                    }
                } catch (Exception ex) {
                    // do not exit for exceptions other than InterruptedException
                    log.error("Error occurred while cleaning up expired entry", ex);
                }
            }
        };
        thread.setDaemon(true);
        return thread;
    }

    private void init() throws IOException {
        long start = Time.SYSTEM.hiResClockMs();

        try {
            Files.createDirectory(cacheDir.toPath());
            log.info("Created new file {} for RemoteIndexCache", cacheDir);
        } catch (FileAlreadyExistsException e) {
            log.info("RemoteIndexCache directory {} already exists. Re-using the same directory.", cacheDir);
        } catch (Exception e) {
            log.error("Unable to create directory {} for RemoteIndexCache.", cacheDir, e);
            throw new KafkaException(e);
        }

        // Delete any .deleted or .tmp files remained from the earlier run of the broker.
        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            paths.forEach(path -> {
                if (path.endsWith(LogFileUtils.DELETED_FILE_SUFFIX) ||
                        path.endsWith(TMP_FILE_SUFFIX)) {
                    try {
                        if (Files.deleteIfExists(path)) {
                            log.debug("Deleted file path {} on cache initialization", path);
                        }
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

                String indexFileName = fileNamePath.toString();
                Uuid uuid = remoteLogSegmentIdFromRemoteIndexFileName(indexFileName);

                // It is safe to update the internalCache non-atomically here since this function is always called by a single
                // thread only.
                if (!internalCache.asMap().containsKey(uuid)) {
                    String fileNameWithoutSuffix = indexFileName.substring(0, indexFileName.indexOf("."));
                    File offsetIndexFile = new File(cacheDir, fileNameWithoutSuffix + INDEX_FILE_SUFFIX);
                    File timestampIndexFile = new File(cacheDir, fileNameWithoutSuffix + TIME_INDEX_FILE_SUFFIX);
                    File txnIndexFile = new File(cacheDir, fileNameWithoutSuffix + TXN_INDEX_FILE_SUFFIX);

                    // Create entries for each path if all the index files exist.
                    if (Files.exists(offsetIndexFile.toPath()) &&
                            Files.exists(timestampIndexFile.toPath()) &&
                            Files.exists(txnIndexFile.toPath())) {
                        long offset = offsetFromRemoteIndexFileName(indexFileName);
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
        log.info("RemoteIndexCache starts up in {} ms.", Time.SYSTEM.hiResClockMs() - start);
    }

    private <T> T loadIndexFile(File file, RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                Function<RemoteLogSegmentMetadata, InputStream> fetchRemoteIndex,
                                Function<File, T> readIndex) throws IOException {
        File indexFile = new File(cacheDir, file.getName());
        T index = null;
        if (Files.exists(indexFile.toPath())) {
            try {
                index = readIndex.apply(indexFile);
            } catch (CorruptIndexException ex) {
                log.info("Error occurred while loading the stored index file {}", indexFile.getPath(), ex);
            }
        }
        if (index == null) {
            File tmpIndexFile = new File(indexFile.getParentFile(), indexFile.getName() + RemoteIndexCache.TMP_FILE_SUFFIX);
            try (InputStream inputStream = fetchRemoteIndex.apply(remoteLogSegmentMetadata)) {
                Files.copy(inputStream, tmpIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            Utils.atomicMoveWithFallback(tmpIndexFile.toPath(), indexFile.toPath(), false);
            index = readIndex.apply(indexFile);
        }
        return index;
    }

    public Entry getIndexEntry(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        if (isRemoteIndexCacheClosed.get()) {
            throw new IllegalStateException("Unable to fetch index for " +
                    "segment id=" + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ". Instance is already closed.");
        }
        lock.readLock().lock();
        try {
            // while this thread was waiting for lock, another thread may have changed the value of isRemoteIndexCacheClosed.
            // check for index close again
            if (isRemoteIndexCacheClosed.get()) {
                throw new IllegalStateException("Unable to fetch index for segment-id = "
                        + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ". Index instance is already closed.");
            }
            return internalCache.get(remoteLogSegmentMetadata.remoteLogSegmentId().id(),
                    uuid -> createCacheEntry(remoteLogSegmentMetadata));
        } finally {
            lock.readLock().unlock();
        }
    }

    private RemoteIndexCache.Entry createCacheEntry(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        long startOffset = remoteLogSegmentMetadata.startOffset();
        try {
            File offsetIndexFile = remoteOffsetIndexFile(cacheDir, remoteLogSegmentMetadata);
            OffsetIndex offsetIndex = loadIndexFile(offsetIndexFile, remoteLogSegmentMetadata, rlsMetadata -> {
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
            File timeIndexFile = remoteTimeIndexFile(cacheDir, remoteLogSegmentMetadata);
            TimeIndex timeIndex = loadIndexFile(timeIndexFile, remoteLogSegmentMetadata, rlsMetadata -> {
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
            File txnIndexFile = remoteTransactionIndexFile(cacheDir, remoteLogSegmentMetadata);
            TransactionIndex txnIndex = loadIndexFile(txnIndexFile, remoteLogSegmentMetadata, rlsMetadata -> {
                try {
                    return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TRANSACTION);
                } catch (RemoteResourceNotFoundException e) {
                    // Don't throw an exception since the transaction index may not exist because of no transactional
                    // records. Instead, we return an empty stream so that an empty file is created in the cache
                    return new ByteArrayInputStream(new byte[0]);
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
    }

    public int lookupOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
        lock.readLock().lock();
        try {
            return getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int lookupTimestamp(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long timestamp, long startingOffset) {
        lock.readLock().lock();
        try {
            return getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
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

    public static class Entry implements AutoCloseable {

        private final OffsetIndex offsetIndex;
        private final TimeIndex timeIndex;
        private final TransactionIndex txnIndex;

        // This lock is used to synchronize cleanup methods and read methods. This ensures that cleanup (which changes the
        // underlying files of the index) isn't performed while a read is in-progress for the entry. This is required in
        // addition to using the thread safe cache because, while the thread safety of the cache ensures that we can read
        // entries concurrently, it does not ensure that we won't mutate underlying files belonging to an entry.
        private final ReentrantReadWriteLock entryLock = new ReentrantReadWriteLock();

        private boolean cleanStarted = false;

        private boolean markedForCleanup = false;

        private final long entrySizeBytes;

        public Entry(OffsetIndex offsetIndex, TimeIndex timeIndex, TransactionIndex txnIndex) {
            this.offsetIndex = offsetIndex;
            this.timeIndex = timeIndex;
            // If txn index does not exist on the source, it's an empty file on the index entry
            this.txnIndex = txnIndex;
            this.entrySizeBytes = estimatedEntrySize();
        }

        // Visible for testing
        public OffsetIndex offsetIndex() {
            return offsetIndex;
        }

        // Visible for testing
        public TimeIndex timeIndex() {
            return timeIndex;
        }

        // Visible for testing
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

        public long entrySizeBytes() {
            return entrySizeBytes;
        }

        private long estimatedEntrySize() {
            entryLock.readLock().lock();
            try {
                return offsetIndex.sizeInBytes() + timeIndex.sizeInBytes() + Files.size(txnIndex.path());
            } catch (IOException e) {
                log.warn("Error occurred when estimating remote index cache entry bytes size, just set 0 firstly.", e);
                return 0L;
            } finally {
                entryLock.readLock().unlock();
            }
        }

        public OffsetPosition lookupOffset(long targetOffset) {
            entryLock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");
                else return offsetIndex.lookup(targetOffset);
            } finally {
                entryLock.readLock().unlock();
            }
        }

        public OffsetPosition lookupTimestamp(long timestamp, long startingOffset) {
            entryLock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");

                TimestampOffset timestampOffset = timeIndex.lookup(timestamp);
                return offsetIndex.lookup(Math.max(startingOffset, timestampOffset.offset));
            } finally {
                entryLock.readLock().unlock();
            }
        }

        public void markForCleanup() throws IOException {
            entryLock.writeLock().lock();
            try {
                if (!markedForCleanup) {
                    markedForCleanup = true;
                    offsetIndex.renameTo(new File(Utils.replaceSuffix(offsetIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                    timeIndex.renameTo(new File(Utils.replaceSuffix(timeIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                    txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                }
            } finally {
                entryLock.writeLock().unlock();
            }
        }

        public void cleanup() throws IOException {
            entryLock.writeLock().lock();
            try {
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
            } finally {
                entryLock.writeLock().unlock();
            }
        }

        @Override
        public void close() {
            entryLock.writeLock().lock();
            try {
                Utils.closeQuietly(offsetIndex, "OffsetIndex");
                Utils.closeQuietly(timeIndex, "TimeIndex");
                Utils.closeQuietly(txnIndex, "TransactionIndex");
            } finally {
                entryLock.writeLock().unlock();
            }
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "offsetIndex=" + offsetIndex.file().getName() +
                    ", timeIndex=" + timeIndex.file().getName() +
                    ", txnIndex=" + txnIndex.file().getName() +
                    '}';
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
     * @throws KafkaException Any other non IOExceptions are wrapped and thrown as KafkaException
     */
    private static void tryAll(List<StorageAction<Void, Exception>> actions) throws IOException {
        IOException firstIOException = null;
        List<Exception> exceptions = new ArrayList<>();
        for (StorageAction<Void, Exception> action : actions) {
            try {
                action.execute();
            } catch (IOException e) {
                if (firstIOException == null) {
                    firstIOException = e;
                } else {
                    exceptions.add(e);
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (firstIOException != null) {
            exceptions.forEach(firstIOException::addSuppressed);
            throw firstIOException;
        } else if (!exceptions.isEmpty()) {
            Iterator<Exception> iterator = exceptions.iterator();
            KafkaException kafkaException = new KafkaException(iterator.next());
            while (iterator.hasNext()) {
                kafkaException.addSuppressed(iterator.next());
            }
            throw kafkaException;
        }
    }

    private static Uuid remoteLogSegmentIdFromRemoteIndexFileName(String fileName) {
        int underscoreIndex = fileName.indexOf("_");
        int dotIndex = fileName.indexOf(".");
        return Uuid.fromString(fileName.substring(underscoreIndex + 1, dotIndex));
    }

    private static long offsetFromRemoteIndexFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf("_")));
    }

    /**
     * Generates prefix for file name for the on-disk representation of remote indexes.
     * <p>
     * Example of file name prefix is 45_fooid where 45 represents the base offset for the segment and
     * fooid represents the unique {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId}
     *
     * @param remoteLogSegmentMetadata remote segment for the remote indexes
     * @return string which should be used as prefix for on-disk representation of remote indexes
     */
    private static String generateFileNamePrefixForIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        long startOffset = remoteLogSegmentMetadata.startOffset();
        Uuid segmentId = remoteLogSegmentMetadata.remoteLogSegmentId().id();
        // uuid.toString uses URL encoding which is safe for filenames and URLs.
        return startOffset + "_" + segmentId.toString();
    }

    public static File remoteOffsetIndexFile(File dir, RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return new File(dir, remoteOffsetIndexFileName(remoteLogSegmentMetadata));
    }

    public static String remoteOffsetIndexFileName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        String prefix = generateFileNamePrefixForIndex(remoteLogSegmentMetadata);
        return prefix + LogFileUtils.INDEX_FILE_SUFFIX;
    }

    public static File remoteTimeIndexFile(File dir, RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return new File(dir, remoteTimeIndexFileName(remoteLogSegmentMetadata));
    }

    public static String remoteTimeIndexFileName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return generateFileNamePrefixForIndex(remoteLogSegmentMetadata) + TIME_INDEX_FILE_SUFFIX;
    }

    public static File remoteTransactionIndexFile(File dir, RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return new File(dir, remoteTransactionIndexFileName(remoteLogSegmentMetadata));
    }

    public static String remoteTransactionIndexFileName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return generateFileNamePrefixForIndex(remoteLogSegmentMetadata) + LogFileUtils.TXN_INDEX_FILE_SUFFIX;
    }

    // Visible for testing
    public static String remoteDeletedSuffixIndexFileName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return generateFileNamePrefixForIndex(remoteLogSegmentMetadata) + LogFileUtils.DELETED_FILE_SUFFIX;
    }

}