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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;

/**
 * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
 * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
 * thread-safe.
 */
public class StateDirectory {

    private static final Pattern PATH_NAME = Pattern.compile("\\d+_\\d+");
    private static final Logger log = LoggerFactory.getLogger(StateDirectory.class);
    static final String LOCK_FILE_NAME = ".lock";

    private final Object taskDirCreationLock = new Object();
    private final Time time;
    private final String appId;
    private final File stateDir;
    private final boolean hasPersistentStores;
    private final HashMap<TaskId, FileChannel> channels = new HashMap<>();
    private final HashMap<TaskId, LockAndOwner> locks = new HashMap<>();

    private FileChannel globalStateChannel;
    private FileLock globalStateLock;

    private static class LockAndOwner {
        final FileLock lock;
        final String owningThread;

        LockAndOwner(final String owningThread, final FileLock lock) {
            this.owningThread = owningThread;
            this.lock = lock;
        }
    }

    /**
     * Ensures that the state base directory as well as the application's sub-directory are created.
     *
     * @param config              streams application configuration to read the root state directory path
     * @param time                system timer used to execute periodic cleanup procedure
     * @param hasPersistentStores only when the application's topology does have stores persisted on local file
     *                            system, we would go ahead and auto-create the corresponding application / task / store
     *                            directories whenever necessary; otherwise no directories would be created.
     *
     * @throws ProcessorStateException if the base state directory or application state directory does not exist
     *                                 and could not be created when hasPersistentStores is enabled.
     */
    public StateDirectory(final StreamsConfig config, final Time time, final boolean hasPersistentStores) {
        this.time = time;
        this.hasPersistentStores = hasPersistentStores;
        this.appId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final String stateDirName = config.getString(StreamsConfig.STATE_DIR_CONFIG);
        final File baseDir = new File(stateDirName);
        stateDir = new File(baseDir, appId);

        if (this.hasPersistentStores) {
            if (!baseDir.exists() && !baseDir.mkdirs()) {
                throw new ProcessorStateException(
                    String.format("base state directory [%s] doesn't exist and couldn't be created", stateDirName));
            }
            if (!stateDir.exists() && !stateDir.mkdir()) {
                throw new ProcessorStateException(
                    String.format("state directory [%s] doesn't exist and couldn't be created", stateDir.getPath()));
            }
            if (stateDirName.startsWith(System.getProperty("java.io.tmpdir"))) {
                log.warn("Using an OS temp directory in the state.dir property can cause failures with writing" +
                    " the checkpoint file due to the fact that this directory can be cleared by the OS." +
                    " Resolved state.dir: [" + stateDirName + "]");
            }
            // change the dir permission to "rwxr-x---" to avoid world readable
            configurePermissions(baseDir);
            configurePermissions(stateDir);
        }
    }
    
    private void configurePermissions(final File file) {
        final Path path = file.toPath();
        if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            final Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-x---");
            try {
                Files.setPosixFilePermissions(path, perms);
            } catch (final IOException e) {
                log.error("Error changing permissions for the directory {} ", path, e);
            }
        } else {
            boolean set = file.setReadable(true, true);
            set &= file.setWritable(true, true);
            set &= file.setExecutable(true, true);
            if (!set) {
                log.error("Failed to change permissions for the directory {}", file);
            }
        }
    }

    /**
     * Get or create the directory for the provided {@link TaskId}.
     * @return directory for the {@link TaskId}
     * @throws ProcessorStateException if the task directory does not exists and could not be created
     */
    public File directoryForTask(final TaskId taskId) {
        final File taskDir = new File(stateDir, taskId.toString());
        if (hasPersistentStores && !taskDir.exists()) {
            synchronized (taskDirCreationLock) {
                // to avoid a race condition, we need to check again if the directory does not exist:
                // otherwise, two threads might pass the outer `if` (and enter the `then` block),
                // one blocks on `synchronized` while the other creates the directory,
                // and the blocking one fails when trying to create it after it's unblocked
                if (!taskDir.exists() && !taskDir.mkdir()) {
                    throw new ProcessorStateException(
                        String.format("task directory [%s] doesn't exist and couldn't be created", taskDir.getPath()));
                }
            }
        }
        return taskDir;
    }

    /**
     * @return The File handle for the checkpoint in the given task's directory
     */
    File checkpointFileFor(final TaskId taskId) {
        return new File(directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    /**
     * Decide if the directory of the task is empty or not
     */
    boolean directoryForTaskIsEmpty(final TaskId taskId) {
        final File taskDir = directoryForTask(taskId);

        return taskDirEmpty(taskDir);
    }

    private boolean taskDirEmpty(final File taskDir) {
        final File[] storeDirs = taskDir.listFiles(pathname ->
            !pathname.getName().equals(LOCK_FILE_NAME) &&
                !pathname.getName().equals(CHECKPOINT_FILE_NAME));

        // if the task is stateless, storeDirs would be null
        return storeDirs == null || storeDirs.length == 0;
    }

    /**
     * Get or create the directory for the global stores.
     * @return directory for the global stores
     * @throws ProcessorStateException if the global store directory does not exists and could not be created
     */
    File globalStateDir() {
        final File dir = new File(stateDir, "global");
        if (hasPersistentStores && !dir.exists() && !dir.mkdir()) {
            throw new ProcessorStateException(
                String.format("global state directory [%s] doesn't exist and couldn't be created", dir.getPath()));
        }
        return dir;
    }

    private String logPrefix() {
        return String.format("stream-thread [%s]", Thread.currentThread().getName());
    }

    /**
     * Get the lock for the {@link TaskId}s directory if it is available
     * @param taskId task id
     * @return true if successful
     * @throws IOException if the file cannot be created or file handle cannot be grabbed, should be considered as fatal
     */
    synchronized boolean lock(final TaskId taskId) throws IOException {
        if (!hasPersistentStores) {
            return true;
        }

        final File lockFile;
        // we already have the lock so bail out here
        final LockAndOwner lockAndOwner = locks.get(taskId);
        if (lockAndOwner != null && lockAndOwner.owningThread.equals(Thread.currentThread().getName())) {
            log.trace("{} Found cached state dir lock for task {}", logPrefix(), taskId);
            return true;
        } else if (lockAndOwner != null) {
            // another thread owns the lock
            return false;
        }

        try {
            lockFile = new File(directoryForTask(taskId), LOCK_FILE_NAME);
        } catch (final ProcessorStateException e) {
            // directoryForTask could be throwing an exception if another thread
            // has concurrently deleted the directory
            return false;
        }

        final FileChannel channel;

        try {
            channel = getOrCreateFileChannel(taskId, lockFile.toPath());
        } catch (final NoSuchFileException e) {
            // FileChannel.open(..) could throw NoSuchFileException when there is another thread
            // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
            // file, in this case we will return immediately indicating locking failed.
            return false;
        }

        final FileLock lock = tryLock(channel);
        if (lock != null) {
            locks.put(taskId, new LockAndOwner(Thread.currentThread().getName(), lock));

            log.debug("{} Acquired state dir lock for task {}", logPrefix(), taskId);
        }
        return lock != null;
    }

    synchronized boolean lockGlobalState() throws IOException {
        if (!hasPersistentStores) {
            return true;
        }

        if (globalStateLock != null) {
            log.trace("{} Found cached state dir lock for the global task", logPrefix());
            return true;
        }

        final File lockFile = new File(globalStateDir(), LOCK_FILE_NAME);
        final FileChannel channel;
        try {
            channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (final NoSuchFileException e) {
            // FileChannel.open(..) could throw NoSuchFileException when there is another thread
            // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
            // file, in this case we will return immediately indicating locking failed.
            return false;
        }
        final FileLock fileLock = tryLock(channel);
        if (fileLock == null) {
            channel.close();
            return false;
        }
        globalStateChannel = channel;
        globalStateLock = fileLock;

        log.debug("{} Acquired global state dir lock", logPrefix());

        return true;
    }

    synchronized void unlockGlobalState() throws IOException {
        if (globalStateLock == null) {
            return;
        }
        globalStateLock.release();
        globalStateChannel.close();
        globalStateLock = null;
        globalStateChannel = null;

        log.debug("{} Released global state dir lock", logPrefix());
    }

    /**
     * Unlock the state directory for the given {@link TaskId}.
     */
    synchronized void unlock(final TaskId taskId) throws IOException {
        final LockAndOwner lockAndOwner = locks.get(taskId);
        if (lockAndOwner != null && lockAndOwner.owningThread.equals(Thread.currentThread().getName())) {
            locks.remove(taskId);
            lockAndOwner.lock.release();
            log.debug("{} Released state dir lock for task {}", logPrefix(), taskId);

            final FileChannel fileChannel = channels.remove(taskId);
            if (fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    public synchronized void clean() {
        // remove task dirs
        try {
            cleanRemovedTasksCalledByUser();
        } catch (final Exception e) {
            throw new StreamsException(e);
        }
        // remove global dir
        try {
            if (stateDir.exists()) {
                Utils.delete(globalStateDir().getAbsoluteFile());
            }
        } catch (final IOException exception) {
            log.error(
                String.format("%s Failed to delete global state directory of %s due to an unexpected exception",
                    logPrefix(), appId),
                exception
            );
            throw new StreamsException(exception);
        }
    }

    /**
     * Remove the directories for any {@link TaskId}s that are no-longer
     * owned by this {@link StreamThread} and aren't locked by either
     * another process or another {@link StreamThread}
     * @param cleanupDelayMs only remove directories if they haven't been modified for at least
     *                       this amount of time (milliseconds)
     */
    public synchronized void cleanRemovedTasks(final long cleanupDelayMs) {
        try {
            cleanRemovedTasksCalledByCleanerThread(cleanupDelayMs);
        } catch (final Exception cannotHappen) {
            throw new IllegalStateException("Should have swallowed exception.", cannotHappen);
        }
    }

    private void cleanRemovedTasksCalledByCleanerThread(final long cleanupDelayMs) {
        for (final File taskDir : listNonEmptyTaskDirectories()) {
            final String dirName = taskDir.getName();
            final TaskId id = TaskId.parse(dirName);
            if (!locks.containsKey(id)) {
                try {
                    if (lock(id)) {
                        final long now = time.milliseconds();
                        final long lastModifiedMs = taskDir.lastModified();
                        if (now > lastModifiedMs + cleanupDelayMs) {
                            log.info("{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                logPrefix(), dirName, id, now - lastModifiedMs, cleanupDelayMs);
                            Utils.delete(taskDir, Collections.singletonList(new File(taskDir, LOCK_FILE_NAME)));
                        }
                    }
                } catch (final OverlappingFileLockException | IOException exception) {
                    log.warn(
                        String.format("%s Swallowed the following exception during deletion of obsolete state directory %s for task %s:",
                            logPrefix(), dirName, id),
                        exception
                    );
                } finally {
                    try {
                        unlock(id);
                    } catch (final IOException exception) {
                        log.warn(
                            String.format("%s Swallowed the following exception during unlocking after deletion of obsolete " +
                                "state directory %s for task %s:", logPrefix(), dirName, id),
                            exception
                        );
                    }
                }
            }
        }
    }

    private void cleanRemovedTasksCalledByUser() throws Exception {
        for (final File taskDir : listAllTaskDirectories()) {
            final String dirName = taskDir.getName();
            final TaskId id = TaskId.parse(dirName);
            if (!locks.containsKey(id)) {
                try {
                    if (lock(id)) {
                        log.info("{} Deleting state directory {} for task {} as user calling cleanup.",
                            logPrefix(), dirName, id);
                        Utils.delete(taskDir, Collections.singletonList(new File(taskDir, LOCK_FILE_NAME)));
                    } else {
                        log.warn("{} Could not get lock for state directory {} for task {} as user calling cleanup.",
                            logPrefix(), dirName, id);
                    }
                } catch (final OverlappingFileLockException | IOException exception) {
                    log.error(
                        String.format("%s Failed to delete state directory %s for task %s with exception:",
                            logPrefix(), dirName, id),
                        exception
                    );
                    throw exception;
                } finally {
                    try {
                        unlock(id);
                        // for manual user call, stream threads are not running so it is safe to delete
                        // the whole directory
                        Utils.delete(taskDir);
                    } catch (final IOException exception) {
                        log.error(
                            String.format("%s Failed to release lock on state directory %s for task %s with exception:",
                                logPrefix(), dirName, id),
                            exception
                        );
                        throw exception;
                    }
                }
            }
        }
    }

    /**
     * List all of the task directories that are non-empty
     * @return The list of all the non-empty local directories for stream tasks
     */
    File[] listNonEmptyTaskDirectories() {
        final File[] taskDirectories;
        if (!hasPersistentStores || !stateDir.exists()) {
            taskDirectories = new File[0];
        } else {
            taskDirectories =
                stateDir.listFiles(pathname -> {
                    if (!pathname.isDirectory() || !PATH_NAME.matcher(pathname.getName()).matches()) {
                        return false;
                    } else {
                        return !taskDirEmpty(pathname);
                    }
                });
        }

        return taskDirectories == null ? new File[0] : taskDirectories;
    }

    /**
     * List all of the task directories
     * @return The list of all the existing local directories for stream tasks
     */
    File[] listAllTaskDirectories() {
        final File[] taskDirectories;
        if (!hasPersistentStores || !stateDir.exists()) {
            taskDirectories = new File[0];
        } else {
            taskDirectories =
                stateDir.listFiles(pathname -> pathname.isDirectory()
                                                   && PATH_NAME.matcher(pathname.getName()).matches());
        }

        return taskDirectories == null ? new File[0] : taskDirectories;
    }

    private FileChannel getOrCreateFileChannel(final TaskId taskId,
                                               final Path lockPath) throws IOException {
        if (!channels.containsKey(taskId)) {
            channels.put(taskId, FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        }
        return channels.get(taskId);
    }

    private FileLock tryLock(final FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (final OverlappingFileLockException e) {
            return null;
        }
    }

}
