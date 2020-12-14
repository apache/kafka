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

package org.apache.kafka.metalog;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataParser;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileLockInterruptionException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * The LocalLogManager has a test log that relies on a single local directory.  Each
 * process tries to lock the "leader" file in that directory.  Whatever process succeeds
 * in locking it is the leader, with the ability to write to the "log" file in that
 * directory.
 *
 * The LocalLogManager has three threads: LeadershipClaimerThread, ScribeThread, and
 * LogDirWatcherThread.
 *
 * The LeadershipClaimerThread tries to take the lock on the "leader" file.
 * If it succeeds, it will tell the ScribeThread to claim the leadership.
 * It also tells the ScribeThread when to renounce the leadership.
 *
 * The ScribeThread handles reading and writing the log.  When in follower mode, it will
 * read the latest messages in the log.  When in writer mode, it will write to the end of
 * the log.  In both cases, it will inform the listener of what event just happened.
 *
 * The WatcherThread watches the log directory and wakes up the ScribeThread when bytes
 * are added to the log file.  The ScribeThread will pause this thread when the scribe
 * is active, since its services are not needed at that point.
 */
public final class LocalLogManager implements MetaLogManager, AutoCloseable {
    /**
     * The global registry of locks on a particular path.
     */
    private static final LockRegistry LOCK_REGISTRY = new LockRegistry();

    /**
     * An empty byte buffer object.
     */
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private static final int DEFAULT_LOG_CHECK_INTERVAL_MS = 2;

    /**
     * A lock plus the number of threads waiting on it.  Waiters must be updated while
     * holding the lock of the LockRegistry itself.
     */
    static class LockData {
        final ReentrantLock lock;
        int waiters;

        LockData() {
            this.lock = new ReentrantLock();
            this.waiters = 0;
        }
    }

    /**
     * A registry for locks on paths.
     * <p>
     * We want a lock that works in a cross-process fashion, so that multiple processes
     * on the same machine can attempt to become the leader of our file-backed mock log.
     * <p>
     * FileChannel#lock is almost good enough to do the job on its own, but it has a
     * quirk: if two threads from the same process try to lock the same file, one of them
     * will get an OverlappingFileLockException.  This would be bad for the case where
     * we run multiple controllers in the same process in JUnit.  So we add another layer
     * of in-memory locking to ensure that we don't hit this case.
     */
    static class LockRegistry {
        private final Map<String, LockData> locks = new HashMap<>();

        void lock(String path, FileChannel channel, Runnable onTakeLock)
                throws IOException {
            try {
                LockData lockData = null;
                FileLock fileLock = null;
                synchronized (this) {
                    lockData = locks.computeIfAbsent(path, __ -> new LockData());
                    lockData.waiters++;
                }
                try {
                    lockData.lock.lockInterruptibly();
                    try {
                        fileLock = channel.lock();
                        onTakeLock.run();
                    } catch (FileLockInterruptionException e) {
                        throw new InterruptedException();
                    } finally {
                        Utils.closeQuietly(fileLock, "fileLock");
                        lockData.lock.unlock();
                    }
                } finally {
                    synchronized (this) {
                        lockData.waiters--;
                        if (lockData.waiters == 0) {
                            locks.remove(path, lockData);
                        }
                    }
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static int leaderSize(MetaLogLeader leader) {
        return Integer.BYTES + Long.BYTES;
    }

    static void writeLeader(ByteBuffer buf, MetaLogLeader leader) {
        buf.putInt(leader.nodeId());
        buf.putLong(leader.epoch());
    }

    static MetaLogLeader readLeader(ByteBuffer buf) {
        int nodeId = buf.getInt();
        long epoch = buf.getLong();
        return new MetaLogLeader(nodeId, epoch);
    }

    /**
     * The LeadershipClaimer continuously tries to claim the leadership of the log.
     * It always needs to be ready to receive InterruptedExceptions, which somewhat
     * limits what it can do.  For example, we don't want to write in this thread
     * since if we get interrupted in the middle, we might have a partial write.
     */
    class LeadershipClaimerThread extends KafkaThread {
        private final String leaderPath;
        private final FileChannel leaderChannel;
        private volatile boolean shuttingDown = false;
        private volatile WriterThread curWriterThread = null;

        LeadershipClaimerThread(Path leaderPath,
                                FileChannel leaderChannel) {
            super(threadNamePrefix + "LeadershipClaimerThread", true);
            this.leaderPath = leaderPath.toString();
            this.leaderChannel = leaderChannel;
        }

        @Override
        public void run() {
            try {
                log.debug("starting LeadershipClaimerThread");
                while (!shuttingDown) {
                    LOCK_REGISTRY.lock(leaderPath, leaderChannel,
                        () -> {
                            WriterThread thread = null;
                            try {
                                log.debug("starting WriterThread for {}", logPath);
                                thread = new WriterThread(
                                    FileChannel.open(logPath, CREATE, WRITE, READ));
                                thread.writeClaim();
                                thread.start();
                                curWriterThread = thread;
                                while (!shuttingDown) {
                                    synchronized (this) {
                                        this.wait();
                                    }
                                }
                            } catch (InterruptedException e) {
                                // Other threads send an InterruptedException to this thread
                                // in order to break us out of FileChannel#lock (if we
                                // haven't take the lock) or Object#wait (if we have).
                                // Once we catch the exception, we're done with it and
                                // can discard it.
                                log.trace("LeadershipClaimerThread received " +
                                    e.getClass().getSimpleName());
                            } catch (Exception e) {
                                log.warn("LeadershipClaimerThread received unexpected " +
                                    e.getClass().getSimpleName(), e);
                            } finally {
                                if (thread != null) {
                                    thread.shutdown();
                                    try {
                                        thread.join();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                curWriterThread = null;
                            }
                        });
                }
                log.debug("shutting down LeadershipClaimerThread.");
            } catch (Throwable e) {
                log.error("exiting LeadershipClaimer with error", e);
            } finally {
                Utils.closeQuietly(leaderChannel, leaderPath);
            }
        }

        void renounce(long epoch) {
            while (true) {
                WriterThread writerThread = curWriterThread;
                if (writerThread != null &&  writerThread.leader().epoch() >= epoch) {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    break;
                }
            }
            log.info("Interrupting LeadershipClaimerThread in order to " +
                "renounce epoch {}", epoch);
            this.interrupt();
        }

        void beginShutdown() {
            shuttingDown = true;
            this.interrupt();
        }

        public long scheduleWrite(long epoch, List<ApiMessageAndVersion> batch) {
            WriterThread writerThread = curWriterThread;
            if (writerThread != null) {
                return writerThread.scheduleWrite(epoch, batch);
            } else {
                return Long.MAX_VALUE;
            }
        }
    }

    private final static int FRAME_LENGTH = 4;

    class WriterThread extends KafkaThread {
        private final FileChannel logChannel;
        private final Scribe scribe;
        private final List<List<ApiMessageAndVersion>> toWrite;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition wakeCond = lock.newCondition();
        private MetaLogLeader leader = null;
        private boolean shuttingDown = false;
        private long nextWriteOffset = 0;

        WriterThread(FileChannel logChannel) {
            super(threadNamePrefix + "WriterThread", true);
            this.logChannel = logChannel;
            this.scribe = new Scribe(logChannel);
            this.toWrite = new ArrayList<>();
        }

        public void writeClaim() throws IOException {
            MetaLogLeader prevLeader = new MetaLogLeader(-1, -1);
            while (true) {
                ReadResult result = scribe.read();
                if (!result.batch.isEmpty()) {
                    nextWriteOffset = scribe.nextOffset;
                }
                if (result.newLeader != null) {
                    prevLeader = result.newLeader;
                }
                if (result.partialRead) {
                    throw new RuntimeException("Partial write found in log.");
                }
                if (result.eof) {
                    break;
                }
            }
            lock.lock();
            try {
                leader = new MetaLogLeader(nodeId, prevLeader.epoch() + 1);
            } finally {
                lock.unlock();
            }
            scribe.writeClaim(leader);
            log.debug("wrote claim {}", leader);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    List<ApiMessageAndVersion> nextBatch = Collections.emptyList();
                    lock.lock();
                    try {
                        if (shuttingDown) {
                            break;
                        }
                        while (true) {
                            if (shuttingDown) {
                                break;
                            }
                            if (!toWrite.isEmpty()) {
                                nextBatch = toWrite.remove(0);
                                break;
                            }
                            wakeCond.await();
                        }
                    } finally {
                        lock.unlock();
                    }
                    for (ApiMessageAndVersion messageAndVersion : nextBatch) {
                        scribe.writeMessage(messageAndVersion);
                    }
                }
            } catch (Throwable t) {
                log.error("exiting WriterThread with unexpected error", t);
            } finally {
                Utils.closeQuietly(logChannel, "logChannel");
            }
        }

        private MetaLogLeader leader() {
            lock.lock();
            try {
                return leader;
            } finally {
                lock.unlock();
            }
        }

        public void shutdown() {
            lock.lock();
            try {
                this.shuttingDown = true;
                wakeCond.signal();
            } finally {
                lock.unlock();
            }
        }

        public long scheduleWrite(long epoch, List<ApiMessageAndVersion> batch) {
            lock.lock();
            try {
                if (leader.epoch() != epoch) {
                    return Long.MAX_VALUE;
                }
                long returnOffset = nextWriteOffset;
                toWrite.add(batch);
                nextWriteOffset += batch.size();
                wakeCond.signal();
                return returnOffset;
            } finally {
                lock.unlock();
            }
        }
    }

    class ReaderThread extends KafkaThread {
        private final FileChannel logChannel;
        private final MetaLogListener listener;
        private MetaLogLeader leader = new MetaLogLeader(-1, -1);
        private final Scribe scribe;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition wakeCond = lock.newCondition();
        private boolean shuttingDown = false;

        ReaderThread(FileChannel logChannel,
                     MetaLogListener listener) {
            super(threadNamePrefix + "ReaderThread(" + listener.toString() + ")", true);
            this.logChannel = logChannel;
            this.listener = listener;
            this.scribe = new Scribe(logChannel);
        }

        @Override
        public void run() {
            try {
                log.debug("starting ReaderThread");
                while (true) {
                    ReadResult result;
                    if (scribe.nextOffset >= maxReadOffset) {
                        result = new ReadResult(Collections.emptyList(), false, true, null);
                    } else {
                        result = scribe.read();
                        if (!result.batch.isEmpty()) {
                            listener.handleCommits(scribe.nextOffset - 1, result.batch);
                        }
                        if (result.newLeader != null) {
                            if (leader.nodeId() == nodeId) {
                                listener.handleRenounce(leader.epoch());
                            }
                            listener.handleNewLeader(result.newLeader);
                            leader = result.newLeader;
                            managerLock.lock();
                            try {
                                if (leader.epoch() > latestLeader.epoch()) {
                                    latestLeader = leader;
                                }
                            } finally {
                                managerLock.unlock();
                            }
                        }
                    }
                    lock.lock();
                    try {
                        if (shuttingDown) {
                            break;
                        }
                        if (result.eof || result.partialRead) {
                            wakeCond.await(logCheckIntervalMs, TimeUnit.MILLISECONDS);
                        }
                        if (shuttingDown) {
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable t) {
                log.error("exiting ReaderThread with unexpected error", t);
            } finally {
                Utils.closeQuietly(logChannel, "logChannel");
            }
            listener.beginShutdown();
        }

        public void shutdown() {
            lock.lock();
            try {
                this.shuttingDown = true;
                wakeCond.signal();
            } finally {
                lock.unlock();
            }
        }

        public void wake() {
            lock.lock();
            try {
                wakeCond.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    static class ReadResult {
        private final List<ApiMessage> batch;
        private final boolean eof;
        private final boolean partialRead;
        private final MetaLogLeader newLeader;

        public ReadResult(List<ApiMessage> batch,
                          boolean eof,
                          boolean partialRead,
                          MetaLogLeader newLeader) {
            this.batch = batch;
            this.eof = eof;
            this.partialRead = partialRead;
            this.newLeader = newLeader;
        }

        public List<ApiMessage> batch() {
            return batch;
        }

        public boolean eof() {
            return eof;
        }

        public boolean partialRead() {
            return partialRead;
        }

        public MetaLogLeader newLeader() {
            return newLeader;
        }

        @Override
        public String toString() {
            return "ReadResult(batch=" + batch.stream().map(m -> m.toString()).
                collect(Collectors.joining(", ")) +
                ", eof=" + eof + ", partialRead=" + partialRead +
                ", newLeader=" + newLeader + ")";
        }
    }

    private static class Scribe {
        private final FileChannel channel;
        private long nextOffset = 0;
        private long fileOffset = 0;
        private final ByteBuffer frameBuffer;
        private ByteBuffer dataBuffer;
        private ReadResult next = null;

        Scribe(FileChannel channel) {
            this.channel = channel;
            this.frameBuffer = ByteBuffer.allocate(FRAME_LENGTH);
            this.dataBuffer = EMPTY;
        }

        public ReadResult read() throws IOException {
            List<ApiMessage> batch = new ArrayList<>();
            while (true) {
                if (next == null) {
                    next = readOne();
                }
                if (next.eof || next.partialRead || next.newLeader != null) {
                   if (!batch.isEmpty())  {
                       return new ReadResult(batch, false, false, null);
                   }
                   ReadResult result = next;
                   next = null;
                   return result;
                }
                batch.addAll(next.batch);
                next = null;
                if (batch.size() > 100) {
                    // TODO: preserve batching
                    return new ReadResult(batch, false, false, null);
                }
            }
        }

        private ReadResult readOne() throws IOException {
            frameBuffer.clear();
            int frameLength = readData(frameBuffer, fileOffset);
            if (frameLength <= 0) {
                return new ReadResult(Collections.emptyList(), true, frameLength < 0, null);
            }
            int frameInt = frameBuffer.getInt();
            int length = frameInt < 0 ? -frameInt : frameInt;
            setupDataBuffer(length);
            int dataLength = readData(dataBuffer, fileOffset + FRAME_LENGTH);
            if (dataLength <= 0) {
                return new ReadResult(Collections.emptyList(), true, true, null);
            }
            if (frameInt < 0) {
                ReadResult result = new ReadResult(Collections.emptyList(), false, false,
                    readLeader(dataBuffer));
                fileOffset += FRAME_LENGTH + dataLength;
                return result;
            }
            ApiMessage message = MetadataParser.read(dataBuffer);
            nextOffset++;
            fileOffset += FRAME_LENGTH + dataLength;
            return new ReadResult(Collections.singletonList(message), false, false, null);
        }

        private void writeMessage(ApiMessageAndVersion messageAndVersion)
                throws IOException {
            ObjectSerializationCache cache = new ObjectSerializationCache();
            int size = MetadataParser.size(messageAndVersion.message(),
                messageAndVersion.version(), cache);
            writeFrame(size);
            setupDataBuffer(size);
            MetadataParser.write(messageAndVersion.message(),
                messageAndVersion.version(), cache, dataBuffer);
            dataBuffer.flip();
            writeBuffer(dataBuffer);
        }

        private void writeClaim(MetaLogLeader claim) throws IOException {
            writeFrame(-leaderSize(claim));
            setupDataBuffer(leaderSize(claim));
            writeLeader(dataBuffer, claim);
            dataBuffer.flip();
            writeBuffer(dataBuffer);
        }

        /**
         * Read data from the log into a provided buffer.
         *
         * @param buf           The buffer to read the data into.
         * @param curOffset     The offset in the file to read from.
         *
         * @return              A negative number if we got a partial message.
         *                      0 if we hit EOF.
         *                      The positive size of what we read, if we read everything.
         */
        private int readData(ByteBuffer buf, long curOffset) throws IOException {
            int numRead = 0;
            while (buf.hasRemaining()) {
                int result = channel.read(buf, curOffset + buf.position());
                if (result < 0) {
                    return -numRead;
                }
                numRead += result;
            }
            buf.flip();
            return numRead;
        }

        /**
         * Set up the dataBuffer so that it is long enough to include the given number of
         * bytes.  Also clear it and set the limit.
         *
         * @param size          The requested size.
         */
        private void setupDataBuffer(int size) {
            if (size > MetadataParser.MAX_SERIALIZED_EVENT_SIZE) {
                throw new RuntimeException("Event size " + size + " is too large.");
            } else if (size < 0) {
                throw new RuntimeException("Event size " + size + " is negative.");
            }
            if (dataBuffer.capacity() < size) {
                int toAllocate = 64;
                while (toAllocate < size) {
                    toAllocate *= 2;
                }
                toAllocate =
                    Math.min(toAllocate, MetadataParser.MAX_SERIALIZED_EVENT_SIZE);
                dataBuffer = ByteBuffer.allocate(toAllocate);
            }
            dataBuffer.clear();
            dataBuffer.limit(size);
        }

        /**
         * Write a 4-byte frame to disk and advance the file offset.
         *
         * @param value         The frame value to write.
         */
        private void writeFrame(int value) throws IOException {
            frameBuffer.clear();
            frameBuffer.putInt(value);
            frameBuffer.flip();
            writeBuffer(frameBuffer);
        }

        /**
         * Write a buffer to disk and advance the file offset.
         *
         * @param buf           The buffer to write.
         */
        private void writeBuffer(ByteBuffer buf) throws IOException {
            int size = buf.remaining();
            while (buf.hasRemaining()) {
                channel.write(buf, fileOffset + buf.position());
            }
            fileOffset += size;
        }
    }

    class WatcherThread extends KafkaThread {
        private final Path basePath;
        private final WatchService watchService;
        private volatile boolean shuttingDown = false;

        WatcherThread(Path basePath,
                      WatchService watchService) {
            super(threadNamePrefix + "WatcherThread", true);
            this.basePath = basePath;
            this.watchService = watchService;
        }

        @Override
        public void run() {
            try {
                log.debug("starting WatcherThread for {}", basePath);
                while (!shuttingDown) {
                    try {
                        watchService.take();
                        Iterator<ReaderThread> iter = readerThreads.iterator();
                        while (iter.hasNext()) {
                            iter.next().wake();
                        }
                    } catch (InterruptedException e) {
                        log.trace("WatcherThread received InterruptedException.");
                    }
                }
                log.debug("shutting down WatcherThread.");
            } catch (Throwable e) {
                log.error("exiting WatcherThread with unexpected error", e);
            } finally {
                Utils.closeQuietly(watchService, "watchService");
            }
        }

        void beginShutdown() {
            shuttingDown = true;
            this.interrupt();
        }
    }

    private final Logger log;
    private final int nodeId;
    private final int logCheckIntervalMs;
    private final String threadNamePrefix;
    private final Path logPath;
    private final LeadershipClaimerThread leadershipClaimerThread;
    private final List<ReaderThread> readerThreads;
    private final WatcherThread watcherThread;
    private final ReentrantLock managerLock = new ReentrantLock();
    private boolean initialized = false;
    private boolean shuttingDown = false;
    private MetaLogLeader latestLeader = new MetaLogLeader(-1, -1);
    private volatile long maxReadOffset = Long.MAX_VALUE;

    public LocalLogManager(LogContext logContext,
                           int nodeId,
                           String basePath,
                           String threadNamePrefix) throws IOException {
        this(logContext, nodeId, basePath, threadNamePrefix, DEFAULT_LOG_CHECK_INTERVAL_MS);
    }

    public LocalLogManager(LogContext logContext,
                           int nodeId,
                           String basePath,
                           String threadNamePrefix,
                           int logCheckIntervalMs) throws IOException {
        FileChannel leaderChannel = null;
        FileChannel logChannel = null;
        WatchService watchService = null;
        this.log = logContext.logger(LocalLogManager.class);
        try {
            this.nodeId = nodeId;
            this.logCheckIntervalMs = logCheckIntervalMs;
            this.threadNamePrefix = threadNamePrefix;
            Path base = Paths.get(basePath);
            Files.createDirectories(base);
            Path realBase = base.toRealPath();
            Path leaderPath = realBase.resolve("leader");
            leaderChannel = FileChannel.open(leaderPath, CREATE, WRITE, READ);
            this.logPath = realBase.resolve("log");
            logChannel = FileChannel.open(logPath, CREATE, WRITE, READ);
            watchService = FileSystems.getDefault().newWatchService();
            this.leadershipClaimerThread =
                new LeadershipClaimerThread(leaderPath, leaderChannel);
            this.readerThreads = new ArrayList<>();
            this.watcherThread = new WatcherThread(realBase, watchService);
        } catch (Throwable t) {
            log.error("Error creating LocalFileMetaLog", t);
            Utils.closeQuietly(leaderChannel, "leaderChannel");
            Utils.closeQuietly(logChannel, "logPath");
            Utils.closeQuietly(watchService, "watchService");
            throw t;
        }
    }

    @Override
    public void initialize() throws Exception {
        managerLock.lock();
        try {
            if (initialized) {
                return;
            }
            initialized = true;
        } finally {
            managerLock.unlock();
        }
    }

    @Override
    public void register(MetaLogListener listener) throws Exception {
        managerLock.lock();
        try {
            if (!initialized || shuttingDown) {
                throw new RuntimeException("Invalid state");
            }
            ReaderThread thread = new ReaderThread(
                FileChannel.open(logPath, CREATE, WRITE, READ), listener);
            this.readerThreads.add(thread);
            thread.start();
        } finally {
            managerLock.unlock();
        }
    }

    @Override
    public long scheduleWrite(long epoch, List<ApiMessageAndVersion> batch) {
        return leadershipClaimerThread.scheduleWrite(epoch, batch);
    }

    @Override
    public void renounce(long epoch) {
        leadershipClaimerThread.renounce(epoch);
    }

    public void beginShutdown() {
        leadershipClaimerThread.beginShutdown();
        watcherThread.beginShutdown();
    }

    @Override
    public MetaLogLeader leader() {
        managerLock.lock();
        try {
            return latestLeader;
        } finally {
            managerLock.unlock();
        }
    }

    @Override
    public int nodeId() {
        return nodeId;
    }

    @Override
    public void close() throws InterruptedException {
        List<ReaderThread> curReaderThreads = new ArrayList<>();
        managerLock.lock();
        try {
            if (shuttingDown)
                return;
            shuttingDown = true;
            curReaderThreads.addAll(readerThreads);
            readerThreads.clear();
        } finally {
            managerLock.unlock();
        }
        beginShutdown();
        leadershipClaimerThread.join();
        watcherThread.join();
        for (ReaderThread readerThread : curReaderThreads) {
            readerThread.shutdown();
        }
        for (ReaderThread readerThread : curReaderThreads) {
            readerThread.join();
        }
        Utils.closeQuietly(leadershipClaimerThread.leaderChannel, "leaderChannel");
        Utils.closeQuietly(watcherThread.watchService, "watchService");
    }

    public List<MetaLogListener> listeners() {
        List<MetaLogListener> results = new ArrayList<>();
        managerLock.lock();
        try {
            for (ReaderThread readerThread : readerThreads) {
                results.add(readerThread.listener);
            }
        } finally {
            managerLock.unlock();
        }
        return results;
    }

    public void setMaxReadOffset(long newMaxReadOffset) {
        maxReadOffset = newMaxReadOffset;
    }
}
