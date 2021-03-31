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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;

/**
 * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
 * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
 * thread-safe.
 */
public class StateDirectory {

    private static final Pattern TASK_DIR_PATH_NAME = Pattern.compile("\\d+_\\d+");
    private static final Logger log = LoggerFactory.getLogger(StateDirectory.class);
    static final String LOCK_FILE_NAME = ".lock";

    /* The process file is used to persist the process id across restarts.
     * For compatibility reasons you should only ever add fields to the json schema
     */
    static final String PROCESS_FILE_NAME = "kafka-streams-process-metadata";

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class StateDirectoryProcessFile {
        @JsonProperty
        private final UUID processId;

        public StateDirectoryProcessFile() {
            this.processId = null;
        }

        StateDirectoryProcessFile(final UUID processId) {
            this.processId = processId;
        }
    }

    private final Object taskDirCreationLock = new Object();
    private final Time time;
    private final String appId;
    private final File stateDir;
    private final boolean hasPersistentStores;

    private final HashMap<TaskId, Thread> lockedTasksToOwner = new HashMap<>();

    private FileChannel stateDirLockChannel;
    private FileLock stateDirLock;

    private FileChannel globalStateChannel;
    private FileLock globalStateLock;

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
     * @return true if the state directory was successfully locked
     */
    private boolean lockStateDirectory() {
        final File lockFile = new File(stateDir, LOCK_FILE_NAME);
        try {
            stateDirLockChannel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            stateDirLock = tryLock(stateDirLockChannel);
        } catch (final IOException e) {
            log.error("Unable to lock the state directory due to unexpected exception", e);
            throw new ProcessorStateException("Failed to lock the state directory during startup", e);
        }

        return stateDirLock != null;
    }

    public UUID initializeProcessId() {
        if (!hasPersistentStores) {
            return UUID.randomUUID();
        }

        if (!lockStateDirectory()) {
            log.error("Unable to obtain lock as state directory is already locked by another process");
            throw new StreamsException("Unable to initialize state, this can happen if multiple instances of " +
                                           "Kafka Streams are running in the same state directory");
        }

        final File processFile = new File(stateDir, PROCESS_FILE_NAME);
        final ObjectMapper mapper = new ObjectMapper();

        try {
            if (processFile.exists()) {
                try {
                    final StateDirectoryProcessFile processFileData = mapper.readValue(processFile, StateDirectoryProcessFile.class);
                    log.info("Reading UUID from process file: {}", processFileData.processId);
                    if (processFileData.processId != null) {
                        return processFileData.processId;
                    }
                } catch (final Exception e) {
                    log.warn("Failed to read json process file", e);
                }
            }

            final StateDirectoryProcessFile processFileData = new StateDirectoryProcessFile(UUID.randomUUID());
            log.info("No process id found on disk, got fresh process id {}", processFileData.processId);

            mapper.writeValue(processFile, processFileData);
            return processFileData.processId;
        } catch (final IOException e) {
            log.error("Unable to read/write process file due to unexpected exception", e);
            throw new ProcessorStateException(e);
        }
    }

    /**
     * Get or create the directory for the provided {@link TaskId}.
     * @return directory for the {@link TaskId}
     * @throws ProcessorStateException if the task directory does not exists and could not be created
     */
    public File getOrCreateDirectoryForTask(final TaskId taskId) {
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
        return new File(getOrCreateDirectoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    /**
     * Decide if the directory of the task is empty or not
     */
    boolean directoryForTaskIsEmpty(final TaskId taskId) {
        final File taskDir = getOrCreateDirectoryForTask(taskId);

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
     */
    synchronized boolean lock(final TaskId taskId) {
        if (!hasPersistentStores) {
            return true;
        }

        final Thread lockOwner = lockedTasksToOwner.get(taskId);
        if (lockOwner != null) {
            if (lockOwner.equals(Thread.currentThread())) {
                log.trace("{} Found cached state dir lock for task {}", logPrefix(), taskId);
                // we already own the lock
                return true;
            } else {
                // another thread owns the lock
                return false;
            }
        } else if (!stateDir.exists()) {
            log.error("Tried to lock task directory for {} but the state directory does not exist", taskId);
            throw new IllegalStateException("The state directory has been deleted");
        } else {
            lockedTasksToOwner.put(taskId, Thread.currentThread());
            // make sure the task directory actually exists, and create it if not
            getOrCreateDirectoryForTask(taskId);
            return true;
        }
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
    synchronized void unlock(final TaskId taskId) {
        final Thread lockOwner = lockedTasksToOwner.get(taskId);
        if (lockOwner != null && lockOwner.equals(Thread.currentThread())) {
            lockedTasksToOwner.remove(taskId);
            log.debug("{} Released state dir lock for task {}", logPrefix(), taskId);
        }
    }

    public void close() {
        if (hasPersistentStores) {
            try {
                stateDirLock.release();
                stateDirLockChannel.close();

                stateDirLock = null;
                stateDirLockChannel = null;
            } catch (final IOException e) {
                log.error("Unexpected exception while unlocking the state dir", e);
                throw new StreamsException("Failed to release the lock on the state directory", e);
            }

            // all threads should be stopped and cleaned up by now, so none should remain holding a lock
            if (!lockedTasksToOwner.isEmpty()) {
                log.error("Some task directories still locked while closing state, this indicates unclean shutdown: {}", lockedTasksToOwner);
            }
            if (globalStateLock != null) {
                log.error("Global state lock is present while closing the state, this indicates unclean shutdown");
            }
        }
    }

    public synchronized void clean() {
        try {
            cleanRemovedTasksCalledByUser();
        } catch (final Exception e) {
            throw new StreamsException(e);
        }

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
            if (!lockedTasksToOwner.containsKey(id)) {
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
                } catch (final IOException exception) {
                    log.warn(
                        String.format("%s Swallowed the following exception during deletion of obsolete state directory %s for task %s:",
                            logPrefix(), dirName, id),
                        exception
                    );
                } finally {
                    unlock(id);
                }
            }
        }
    }

    private void cleanRemovedTasksCalledByUser() throws Exception {
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        for (final File taskDir : listAllTaskDirectories()) {
            final String dirName = taskDir.getName();
            final TaskId id = TaskId.parse(dirName);
            if (!lockedTasksToOwner.containsKey(id)) {
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
                    firstException.compareAndSet(null, exception);
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
                        firstException.compareAndSet(null, exception);
                    }
                }
            }
        }
        final Exception exception = firstException.get();
        if (exception != null) {
            throw exception;
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
                    if (!pathname.isDirectory() || !TASK_DIR_PATH_NAME.matcher(pathname.getName()).matches()) {
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
                                                   && TASK_DIR_PATH_NAME.matcher(pathname.getName()).matches());
        }

        return taskDirectories == null ? new File[0] : taskDirectories;
    }

    private FileLock tryLock(final FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (final OverlappingFileLockException e) {
            return null;
        }
    }

}
