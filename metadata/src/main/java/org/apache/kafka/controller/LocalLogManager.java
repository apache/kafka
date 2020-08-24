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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.StandardOpenOption.READ;

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
 *
 *                            |                |
 *                            | beginShutdown  | renounce
 *                            V                V
 *                 +-------------------------------------------+
 *                 |         LeadershipClaimerThread           |
 *                 +-------------------------------------------+
 *                      | blocking | blocking |
 *                      | claim    | renounce | beginShutdown
 *                      V          V          V
 *                +-------------------------------------------+      +----------+
 * maybeWrite --} |               ScribeThread                | ---} | Listener |
 *                +-------------------------------------------+      +----------+
 *                      |        |          ^       |
 *                      | pause  | unpause  | wake  | beginShutdown
 *                      V        V          |       V
 *                +-------------------------------------------+
 *                |               WatcherThread               |
 *                +-------------------------------------------+
 */
public final class LocalLogManager implements MetaLogManager {
    /**
     * The global registry of locks on a particular path.
     */
    private static final LockRegistry LOCK_REGISTRY = new LockRegistry();

    /**
     * An empty byte buffer object.
     */
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

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
     *
     * We want a lock that works in a cross-process fashion, so that multiple processes
     * on the same machine can attempt to become the leader of our file-backed mock log.
     *
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

    /**
     * The information that is stored inside the leader file.
     */
    public static class LeaderInfo {
        private final int nodeId;
        private final long epoch;

        public LeaderInfo(int nodeId, long epoch) {
            this.nodeId = nodeId;
            this.epoch = epoch;
        }

        public int nodeId() {
            return nodeId;
        }

        public long epoch() {
            return epoch;
        }

        static LeaderInfo read(ByteBuffer buf) {
            int nodeId = buf.getInt();
            long epoch = buf.getLong();
            return new LeaderInfo(nodeId, epoch);
        }

        int size() {
            return Integer.BYTES + Long.BYTES;
        }

        void write(ByteBuffer buf) {
            buf.putInt(nodeId);
            buf.putLong(epoch);
        }

        @Override
        public String toString() {
            return "LeaderInfo(nodeId=" + nodeId + ", epoch=" + epoch + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, epoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LeaderInfo)) return false;
            LeaderInfo other = (LeaderInfo) o;
            return other.nodeId == nodeId && other.epoch == epoch;
        }
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
        private volatile long claimedEpoch = -1;

        LeadershipClaimerThread(Path leaderPath,
                                FileChannel leaderChannel,
                                String threadNamePrefix) {
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
                            try {
                                claimedEpoch = scribeThread.blockingClaim();
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
                                    "InterruptedException.");
                            } finally {
                                try {
                                    scribeThread.blockingRenounce();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                claimedEpoch = -1;
                            }
                        });
                }
                log.debug("shutting down LeadershipClaimerThread.");
            } catch (Throwable e) {
                log.error("exiting LeadershipClaimer with error", e);
            } finally {
                Utils.closeQuietly(leaderChannel, leaderPath);
                scribeThread.beginShutdown();
            }
        }

        void renounce(long epoch) {
            if (claimedEpoch == epoch) {
                this.interrupt();
            }
        }

        void beginShutdown() {
            shuttingDown = true;
            this.interrupt();
        }
    }

    enum ScribeState {
        FOLLOWER,
        BECOMING_LEADER,
        LEADER;
    }

    private final static int FRAME_LENGTH = 4;

    class ScribeThread extends KafkaThread {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition wakeCond = lock.newCondition();
        private final Condition claimedCond = lock.newCondition();
        private final Condition renouncedCond = lock.newCondition();
        private final FileChannel logChannel;
        private final Listener listener;
        private final long logCheckIntervalMs;
        private final MetadataParser parser = new MetadataParser();
        private boolean shuttingDown = false;
        private boolean shouldLead = false;
        private long nextLogCheckMs = 0;
        private List<ApiMessageAndVersion> incomingWrites = null;
        private ScribeState state = ScribeState.FOLLOWER;
        private LeaderInfo leaderInfo = new LeaderInfo(-1, -1);
        private long index = 0;
        private long nextWriteIndex = 0;
        private long fileOffset = 0;
        private final ByteBuffer frameBuffer = ByteBuffer.allocate(FRAME_LENGTH);
        private ByteBuffer dataBuffer = EMPTY;

        ScribeThread(FileChannel logChannel,
                     Listener listener,
                     long logCheckIntervalMs,
                     String threadNamePrefix) {
            super(threadNamePrefix + "LeadershipClaimerThread", true);
            this.logChannel = logChannel;
            this.listener = listener;
            this.logCheckIntervalMs = logCheckIntervalMs;
        }

        @Override
        public void run() {
            try {
                log.debug("starting ScribeThread");
                LeaderInfo claim = null;
                while (true) {
                    List<ApiMessageAndVersion> toWrite = null;
                    long shouldRenounceEpoch = -1, shouldClaimEpoch = -1;
                    ScribeState curState;
                    lock.lock();
                    try {
                        if (shuttingDown) {
                            break;
                        }
                        if (shouldLead) {
                            // If shouldLead is true then we need to become a leader.
                            // This requires reading all the previous log entries, and
                            // writing out our claim to the log file.
                            if (state != ScribeState.LEADER) {
                                // If claim is non-null here, that means we are ready
                                // to become a leader.  Otherwise, we have to enter
                                // the BECOMING_LEADER state.
                                if (claim != null) {
                                    state = ScribeState.LEADER;
                                    incomingWrites = null;
                                    leaderInfo = claim;
                                    index = 0;
                                    nextWriteIndex = 0;
                                    shouldClaimEpoch = leaderInfo.epoch;
                                    claimedCond.signalAll();
                                } else {
                                    state = ScribeState.BECOMING_LEADER;
                                }
                            }
                        } else if (state != ScribeState.FOLLOWER) {
                            // If shouldLead is false, and we're not already a follower,
                            // become one immediately.
                            shouldRenounceEpoch = leaderInfo.epoch;
                            state = ScribeState.FOLLOWER;
                            nextLogCheckMs = 0;
                            renouncedCond.signalAll();
                        }
                        if (shouldClaimEpoch == -1 && shouldRenounceEpoch == -1) {
                            switch (state) {
                                case FOLLOWER:
                                    // Followers wait until they get woken up, or until a
                                    // few milliseconds have elapsed, before rechecking
                                    // the log.
                                    long now = SystemTime.SYSTEM.milliseconds();
                                    if (nextLogCheckMs > now) {
                                        wakeCond.await(nextLogCheckMs - now,
                                            TimeUnit.MILLISECONDS);
                                        now = SystemTime.SYSTEM.milliseconds();
                                    }
                                    nextLogCheckMs = now + logCheckIntervalMs;
                                    break;
                                case BECOMING_LEADER:
                                    // No need to wait here.
                                    break;
                                case LEADER:
                                    // Wait to be signalled before waking up.  The signal
                                    // may indicate that we should stop being the leader,
                                    // or that there are new writes to be done.
                                    if (incomingWrites == null && shouldLead) {
                                        wakeCond.await();
                                    }
                                    toWrite = (incomingWrites == null) ?
                                        Collections.emptyList() : incomingWrites;
                                    incomingWrites = null;
                                    break;
                            }
                        }
                        claim = null;
                    } finally {
                        curState = state;
                        lock.unlock();
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("ScribeThread state = {}, shouldRenounceEpoch = {}, " +
                            "shouldClaimEpoch = {}", curState, shouldRenounceEpoch,
                            shouldClaimEpoch);
                    }
                    if (shouldRenounceEpoch > -1L) {
                        listener.handleRenounce(shouldRenounceEpoch);
                    }
                    if (shouldClaimEpoch > -1L) {
                        listener.handleClaim(shouldClaimEpoch);
                    }
                    switch (curState) {
                        case FOLLOWER:
                        case BECOMING_LEADER:
                            // Read until we get EOF or a partial read.
                            int result;
                            do {
                                result = readNextMessage();
                                if (log.isTraceEnabled()) {
                                    if (result < 0) {
                                        log.trace("ScribeThread read partial message.");
                                    } else if (result == 0) {
                                        log.trace("ScribeThread hit EOF while reading.");
                                    } else {
                                        log.trace("ScribeThread read message of length " +
                                            result);
                                    }
                                }
                            } while (result > 0);
                            if (curState == ScribeState.BECOMING_LEADER && result == 0) {
                                // If the result is 0, that means did not read a partial
                                // record at the end.  So we should be ready to write our
                                // claim to the file.
                                claim = new LeaderInfo(nodeId, leaderInfo.epoch + 1);
                                writeClaim(claim);
                                log.debug("ScribeThread wrote claim {}", claim);
                            }
                            break;
                        case LEADER:
                            // Write out the messages that we were given earlier.
                            if (toWrite != null) {
                                for (ApiMessageAndVersion message : toWrite) {
                                    writeMessage(message);
                                    listener.handleCommit(leaderInfo.epoch, index,
                                        message.message());
                                    index++;
                                }
                            }
                            break;
                    }
                }
                log.debug("shutting down ScribeThread.");
                listener.beginShutdown();
            } catch (Throwable t) {
                log.error("exiting ScribeThread with unexpected error", t);
            } finally {
                Utils.closeQuietly(logChannel, "logChannel");
                lock.lock();
                try {
                    renouncedCond.signal();
                } finally {
                    lock.unlock();
                }
                watcherThread.beginShutdown();
                listener.beginShutdown();
            }
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

        private void writeClaim(LeaderInfo claim) throws IOException {
            writeFrame(-claim.size());
            setupDataBuffer(claim.size());
            claim.write(dataBuffer);
            dataBuffer.flip();
            writeBuffer(dataBuffer);
        }

        /**
         * Read a message from the log.  If it is a regular message, send it to the
         * log listener.  If it is a leader epoch message, update the leader epoch.
         *
         * @return  A negative number if we got a partial message.
         *          0 if we hit EOF.
         *          A positive size of what we read, if we read a message.
         */
        private int readNextMessage() throws IOException {
            frameBuffer.clear();
            int frameLength = readData(frameBuffer, fileOffset);
            if (frameLength <= 0) {
                return frameLength;
            }
            int frameInt = frameBuffer.getInt();
            int length = frameInt < 0 ? -frameInt : frameInt;
            setupDataBuffer(length);
            int dataLength = readData(dataBuffer, fileOffset + FRAME_LENGTH);
            if (dataLength < 0) {
                return -FRAME_LENGTH + dataLength;
            }
            if (frameInt < 0) {
                leaderInfo = LeaderInfo.read(dataBuffer);
            } else {
                ApiMessage message = MetadataParser.read(dataBuffer);
                listener.handleCommit(leaderInfo.epoch, index, message);
                index++;
            }
            fileOffset += FRAME_LENGTH + dataLength;
            return FRAME_LENGTH + dataLength;
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
                int result = logChannel.read(buf, curOffset + buf.position());
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
                logChannel.write(buf, fileOffset + buf.position());
            }
            fileOffset += size;
        }

        void beginShutdown() {
            lock.lock();
            try {
                shuttingDown = true;
                wakeCond.signal();
            } finally {
                lock.unlock();
            }
        }

        long blockingClaim() throws InterruptedException {
            lock.lock();
            try {
                if (log.isTraceEnabled()) {
                    log.trace("ScribeThread setting shouldLead to true.");
                }
                shouldLead = true;
                wakeCond.signal();
                do {
                    if (shuttingDown) throw new InterruptedException();
                    claimedCond.await();
                } while (state != ScribeState.LEADER);
                return leaderInfo.epoch();
            } finally {
                lock.unlock();
            }
        }

        void blockingRenounce() throws InterruptedException {
            lock.lock();
            try {
                shouldLead = false;
                wakeCond.signal();
                while (state != ScribeState.FOLLOWER) {
                    if (shuttingDown) throw new InterruptedException();
                    renouncedCond.await();
                }
            } finally {
                lock.unlock();
            }
        }

        long scheduleWrite(long epoch, ApiMessageAndVersion message) {
            lock.lock();
            try {
                if (shuttingDown || leaderInfo.epoch != epoch) {
                    return Long.MAX_VALUE;
                }
                if (incomingWrites == null) {
                    incomingWrites = new ArrayList<>();
                }
                incomingWrites.add(message);
                long curWriteIndex = nextWriteIndex;
                nextWriteIndex++;
                wakeCond.signal();
                return curWriteIndex;
            } finally {
                lock.unlock();
            }
        }

        void wake() {
            lock.lock();
            try {
                wakeCond.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    class WatcherThread extends KafkaThread {
        private final Path basePath;
        private final WatchService watchService;
        private volatile boolean shuttingDown = false;

        WatcherThread(Path basePath,
                      WatchService watchService,
                      String threadNamePrefix) {
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
                        scribeThread.wake();
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
    private final LeadershipClaimerThread leadershipClaimerThread;
    private final ScribeThread scribeThread;
    private final WatcherThread watcherThread;

    public LocalLogManager(LogContext logContext,
                           int nodeId,
                           String basePath,
                           String threadNamePrefix,
                           Listener listener,
                           int logCheckIntervalMs) throws IOException {
        FileChannel leaderChannel = null;
        FileChannel logChannel = null;
        WatchService watchService = null;
        LeadershipClaimerThread leadershipClaimerThread = null;
        ScribeThread scribeThread = null;
        WatcherThread watcherThread = null;
        this.log = logContext.logger(LocalLogManager.class);
        try {
            this.nodeId = nodeId;
            Path base = Paths.get(basePath);
            Files.createDirectories(base);
            Path realBase = base.toRealPath();
            Path leaderPath = realBase.resolve("leader");
            leaderChannel = FileChannel.open(leaderPath, CREATE, WRITE, READ);
            Path logPath = realBase.resolve("log");
            logChannel = FileChannel.open(logPath, CREATE, WRITE, READ);
            watchService = FileSystems.getDefault().newWatchService();
            this.leadershipClaimerThread = leadershipClaimerThread =
                new LeadershipClaimerThread(leaderPath, leaderChannel, threadNamePrefix);
            this.scribeThread = scribeThread =
                new ScribeThread(logChannel, listener, logCheckIntervalMs, threadNamePrefix);
            this.watcherThread = watcherThread =
                new WatcherThread(realBase, watchService, threadNamePrefix);
            this.leadershipClaimerThread.start();
            this.scribeThread.start();
            this.watcherThread.start();
        } catch (Throwable t) {
            log.error("Error creating LocalFileMetaLog", t);
            Utils.closeQuietly(leaderChannel, "leaderChannel");
            Utils.closeQuietly(logChannel, "logPath");
            Utils.closeQuietly(watchService, "watchService");
            if (leadershipClaimerThread != null) {
                leadershipClaimerThread.beginShutdown();
                try {
                    leadershipClaimerThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (scribeThread != null) {
                scribeThread.beginShutdown();
                try {
                    scribeThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (watcherThread != null) {
                watcherThread.beginShutdown();
                try {
                    watcherThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            throw t;
        }
    }

    @Override
    public long scheduleWrite(long epoch, ApiMessageAndVersion message) {
        return scribeThread.scheduleWrite(epoch, message);
    }

    @Override
    public void renounce(long epoch) {
        leadershipClaimerThread.renounce(epoch);
    }

    @Override
    public void beginShutdown() {
        leadershipClaimerThread.beginShutdown();
    }

    @Override
    public void close() throws InterruptedException {
        beginShutdown();
        leadershipClaimerThread.join();
        scribeThread.join();
        watcherThread.join();
    }
}
