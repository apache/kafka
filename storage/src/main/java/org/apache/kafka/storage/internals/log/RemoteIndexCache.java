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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.storage.internals.log.LogFileUtils.INDEX_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.TIME_INDEX_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.TXN_INDEX_FILE_SUFFIX;

/**
 * This is a LRU cache of remote index files stored in `$logdir/remote-log-index-cache`. This is helpful to avoid
 * re-fetching the index files like offset, time indexes from the remote storage for every fetch call.
 */
public class RemoteIndexCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RemoteIndexCache.class);

    public static final String DIR_NAME = "remote-log-index-cache";

    private static final String TMP_FILE_SUFFIX = ".tmp";

    private final File cacheDir;
    private final LinkedBlockingQueue<Entry> expiredIndexes = new LinkedBlockingQueue<>();
    private final Object lock = new Object();
    private final RemoteStorageManager remoteStorageManager;
    private final Map<Uuid, Entry> entries;
    private final ShutdownableThread cleanerThread;

    private volatile boolean closed = false;

    public RemoteIndexCache(RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this(1024, remoteStorageManager, logDir);
    }

    /**
     * Created RemoteIndexCache with the given configs.
     *
     * @param maxSize              maximum number of segment index entries to be cached.
     * @param remoteStorageManager RemoteStorageManager instance, to be used in fetching indexes.
     * @param logDir               log directory
     */
    public RemoteIndexCache(int maxSize, RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this.remoteStorageManager = remoteStorageManager;
        cacheDir = new File(logDir, DIR_NAME);

        entries = new LinkedHashMap<Uuid, RemoteIndexCache.Entry>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Uuid, RemoteIndexCache.Entry> eldest) {
                if (this.size() > maxSize) {
                    RemoteIndexCache.Entry entry = eldest.getValue();
                    // Mark the entries for cleanup, background thread will clean them later.
                    try {
                        entry.markForCleanup();
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                    expiredIndexes.add(entry);
                    return true;
                } else {
                    return false;
                }
            }
        };

        init();

        // Start cleaner thread that will clean the expired entries.
        cleanerThread = createCLeanerThread();
        cleanerThread.start();
    }

    private ShutdownableThread createCLeanerThread() {
        ShutdownableThread thread = new ShutdownableThread("remote-log-index-cleaner") {
            public void doWork() {
                while (!closed) {
                    try {
                        Entry entry = expiredIndexes.take();
                        log.info("Cleaning up index entry $entry");
                        entry.cleanup();
                    } catch (InterruptedException ex) {
                        log.info("Cleaner thread was interrupted", ex);
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
        if (cacheDir.mkdir())
            log.info("Created Cache dir [{}] successfully", cacheDir);

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
            paths.forEach(path -> {

                String pathStr = path.getFileName().toString();
                String name = pathStr.substring(0, pathStr.lastIndexOf("_") + 1);

                // Create entries for each path if all the index files exist.
                int firstIndex = name.indexOf('_');
                int offset = Integer.parseInt(name.substring(0, firstIndex));
                Uuid uuid = Uuid.fromString(name.substring(firstIndex + 1, name.lastIndexOf('_')));

                if (!entries.containsKey(uuid)) {
                    File offsetIndexFile = new File(cacheDir, name + INDEX_FILE_SUFFIX);
                    File timestampIndexFile = new File(cacheDir, name + TIME_INDEX_FILE_SUFFIX);
                    File txnIndexFile = new File(cacheDir, name + TXN_INDEX_FILE_SUFFIX);

                    try {
                        if (offsetIndexFile.exists() && timestampIndexFile.exists() && txnIndexFile.exists()) {

                            OffsetIndex offsetIndex = new OffsetIndex(offsetIndexFile, offset, Integer.MAX_VALUE, false);
                            offsetIndex.sanityCheck();

                            TimeIndex timeIndex = new TimeIndex(timestampIndexFile, offset, Integer.MAX_VALUE, false);
                            timeIndex.sanityCheck();

                            TransactionIndex txnIndex = new TransactionIndex(offset, txnIndexFile);
                            txnIndex.sanityCheck();

                            Entry entry = new Entry(offsetIndex, timeIndex, txnIndex);
                            entries.put(uuid, entry);
                        } else {
                            // Delete all of them if any one of those indexes is not available for a specific segment id
                            LogFileUtils.tryAll(Arrays.asList(
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
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                }
            });
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
        if (closed) throw new IllegalStateException("Instance is already closed.");

        synchronized (lock) {
            return entries.computeIfAbsent(remoteLogSegmentMetadata.remoteLogSegmentId().id(), (Uuid uuid) -> {
                long startOffset = remoteLogSegmentMetadata.startOffset();
                // uuid.toString uses URL encoding which is safe for filenames and URLs.
                String fileName = startOffset + "_" + uuid.toString() + "_";

                try {
                    OffsetIndex offsetIndex = loadIndexFile(fileName, INDEX_FILE_SUFFIX, remoteLogSegmentMetadata, rlsMetadata -> {
                        try {
                            return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET);
                        } catch (RemoteStorageException e) {
                            throw new RuntimeException(e);
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
                        TransactionIndex index = null;
                        try {
                            index = new TransactionIndex(startOffset, file);
                        } catch (IOException e) {
                            throw new KafkaException(e);
                        }
                        index.sanityCheck();
                        return index;
                    });

                    return new Entry(offsetIndex, timeIndex, txnIndex);
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            });
        }
    }

    public int lookupOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) throws IOException {
        return getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position;
    }

    public int lookupTimestamp(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long timestamp, long startingOffset) throws IOException {
        return getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position;
    }

    public void close() {
        closed = true;
        try {
            cleanerThread.shutdown();
        } catch (InterruptedException e) {
            // ignore interrupted exception
        }

        // Close all the opened indexes.
        synchronized (lock) {
            entries.values().forEach(Entry::close);
        }
    }

    public Map<Uuid, Entry> entries() {
        return Collections.unmodifiableMap(entries);
    }

    public static class Entry {

        public final OffsetIndex offsetIndex;
        public final TimeIndex timeIndex;
        public final TransactionIndex txnIndex;

        public Entry(OffsetIndex offsetIndex, TimeIndex timeIndex, TransactionIndex txnIndex) {
            this.offsetIndex = offsetIndex;
            this.timeIndex = timeIndex;
            this.txnIndex = txnIndex;
        }

        private boolean markedForCleanup = false;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        public OffsetPosition lookupOffset(long targetOffset) throws IOException {
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
                    txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX)));
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void cleanup() throws IOException {
            markForCleanup();

            try {
                LogFileUtils.tryAll(Arrays.asList(() -> {
                    offsetIndex.deleteIfExists();
                    return null;
                }, () -> {
                    timeIndex.deleteIfExists();
                    return null;
                }, () -> {
                    txnIndex.deleteIfExists();
                    return null;
                }));
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        public void close() {
            Arrays.asList(offsetIndex, timeIndex).forEach(index -> {
                try {
                    index.close();
                } catch (Exception e) {
                    // ignore exception.
                }
            });

            Utils.closeQuietly(txnIndex, "Closing the transaction index.");
        }
    }
}
