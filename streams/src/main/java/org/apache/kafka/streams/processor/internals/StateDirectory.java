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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.parseTaskDirectoryName;

/**
 * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
 * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
 * thread-safe.
 */
public class StateDirectory implements AutoCloseable {

    private static final Pattern TASK_DIR_PATH_NAME = Pattern.compile("\\d+_\\d+");
    private static final Pattern NAMED_TOPOLOGY_DIR_PATH_NAME = Pattern.compile("__.+__"); // named topology dirs follow '__Topology-Name__'
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

        // required by jackson -- do not remove, your IDE may be warning that this is unused but it's lying to you
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
    private final boolean hasNamedTopologies;

    private final HashMap<TaskId, Thread> lockedTasksToOwner = new HashMap<>();

    private FileChannel stateDirLockChannel;
    private FileLock stateDirLock;

    private final StreamsConfig config;
    private final ConcurrentMap<TaskId, Task> tasksForLocalState = new ConcurrentHashMap<>();

    /**
     * Ensures that the state base directory as well as the application's sub-directory are created.
     *
     * @param config              streams application configuration to read the root state directory path
     * @param time                system timer used to execute periodic cleanup procedure
     * @param hasPersistentStores only when the application's topology does have stores persisted on local file
     *                            system, we would go ahead and auto-create the corresponding application / task / store
     *                            directories whenever necessary; otherwise no directories would be created.
     * @param hasNamedTopologies  whether this application is composed of independent named topologies
     *
     * @throws ProcessorStateException if the base state directory or application state directory does not exist
     *                                 and could not be created when hasPersistentStores is enabled.
     */
    public StateDirectory(final StreamsConfig config, final Time time, final boolean hasPersistentStores, final boolean hasNamedTopologies) {
        this.time = time;
        this.hasPersistentStores = hasPersistentStores;
        this.hasNamedTopologies = hasNamedTopologies;
        this.appId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.config = config;
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
            } else if (stateDir.exists() && !stateDir.isDirectory()) {
                throw new ProcessorStateException(
                    String.format("state directory [%s] can't be created as there is an existing file with the same name", stateDir.getPath()));
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
            throw new ProcessorStateException(String.format("Failed to lock the state directory [%s] during startup",
                stateDir.getAbsolutePath()), e);
        }
        return stateDirLock != null;
    }

    public void initializeStartupTasks(final TopologyMetadata topologyMetadata,
                                       final StreamsMetricsImpl streamsMetrics,
                                       final LogContext logContext) {
        final List<TaskDirectory> nonEmptyTaskDirectories = listNonEmptyTaskDirectories();
        if (hasPersistentStores && !nonEmptyTaskDirectories.isEmpty()) {
            final ThreadCache dummyCache = new ThreadCache(logContext, 0, streamsMetrics);
            final boolean eosEnabled = StreamsConfigUtils.eosEnabled(config);
            final boolean stateUpdaterEnabled = StreamsConfig.InternalConfig.stateUpdaterEnabled(config.originals());

            // discover all non-empty task directories in StateDirectory
            for (final TaskDirectory taskDirectory : nonEmptyTaskDirectories) {
                final String dirName = taskDirectory.file().getName();
                final TaskId id = parseTaskDirectoryName(dirName, taskDirectory.namedTopology());
                final ProcessorTopology subTopology = topologyMetadata.buildSubtopology(id);

                // we still check if the task's sub-topology is stateful, even though we know its directory contains state,
                // because it's possible that the topology has changed since that data was written, and is now stateless
                // this therefore prevents us from creating unnecessary Tasks just because of some left-over state
                if (subTopology.hasStateWithChangelogs()) {
                    final Set<TopicPartition> inputPartitions = topologyMetadata.nodeToSourceTopics(id).values().stream()
                            .flatMap(Collection::stream)
                            .map(t -> new TopicPartition(t, id.partition()))
                            .collect(Collectors.toSet());
                    final ProcessorStateManager stateManager = ProcessorStateManager.createStartupTaskStateManager(
                        id,
                        eosEnabled,
                        logContext,
                        this,
                        subTopology.storeToChangelogTopic(),
                        inputPartitions,
                        stateUpdaterEnabled
                    );

                    final InternalProcessorContext<Object, Object> context = new ProcessorContextImpl(
                        id,
                        config,
                        stateManager,
                        streamsMetrics,
                        dummyCache
                    );

                    final Task task = new StandbyTask(
                        id,
                        inputPartitions,
                        subTopology,
                        topologyMetadata.taskConfig(id),
                        streamsMetrics,
                        stateManager,
                        this,
                        dummyCache,
                        context
                    );

                    try {
                        task.initializeIfNeeded();

                        tasksForLocalState.put(id, task);
                    } catch (final TaskCorruptedException e) {
                        // Task is corrupt - wipe it out (under EOS) and don't initialize a Standby for it
                        task.suspend();
                        task.closeDirty();
                    }
                }
            }
        }
    }

    public boolean hasStartupTasks() {
        return !tasksForLocalState.isEmpty();
    }

    public Task removeStartupTask(final TaskId taskId) {
        final Task task = tasksForLocalState.remove(taskId);
        if (task != null) {
            lockedTasksToOwner.replace(taskId, Thread.currentThread());
        }
        return task;
    }

    public void closeStartupTasks() {
        closeStartupTasks(t -> true);
    }

    private void closeStartupTasks(final Predicate<Task> predicate) {
        if (!tasksForLocalState.isEmpty()) {
            // "drain" Tasks first to ensure that we don't try to close Tasks that another thread is attempting to close
            final Set<Task> drainedTasks = new HashSet<>(tasksForLocalState.size());
            for (final Map.Entry<TaskId, Task> entry : tasksForLocalState.entrySet()) {
                if (predicate.test(entry.getValue()) && tasksForLocalState.remove(entry.getKey()) != null) {
                    // only add to our list of drained Tasks if we exclusively "claimed" a Task from tasksForLocalState
                    // to ensure we don't accidentally try to drain the same Task multiple times from concurrent threads
                    drainedTasks.add(entry.getValue());
                }
            }

            // now that we have exclusive ownership of the drained tasks, close them
            for (final Task task : drainedTasks) {
                task.suspend();
                task.closeClean();
            }
        }
    }

    public UUID initializeProcessId() {
        if (!hasPersistentStores) {
            final UUID processId = UUID.randomUUID();
            log.info("Created new process id: {}", processId);
            return processId;
        }

        if (!lockStateDirectory()) {
            log.error("Unable to obtain lock as state directory is already locked by another process");
            throw new StreamsException(String.format("Unable to initialize state, this can happen if multiple instances of " +
                                           "Kafka Streams are running in the same state directory " +
                                           "(current state directory is [%s]", stateDir.getAbsolutePath()));
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
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     */
    public File getOrCreateDirectoryForTask(final TaskId taskId) {
        final File taskParentDir = getTaskDirectoryParentName(taskId);
        final File taskDir = new File(taskParentDir, StateManagerUtil.toTaskDirString(taskId));
        if (hasPersistentStores) {
            if (!taskDir.exists()) {
                synchronized (taskDirCreationLock) {
                    // to avoid a race condition, we need to check again if the directory does not exist:
                    // otherwise, two threads might pass the outer `if` (and enter the `then` block),
                    // one blocks on `synchronized` while the other creates the directory,
                    // and the blocking one fails when trying to create it after it's unblocked
                    if (!taskParentDir.exists() && !taskParentDir.mkdir()) {
                        throw new ProcessorStateException(
                            String.format("Parent [%s] of task directory [%s] doesn't exist and couldn't be created",
                                taskParentDir.getPath(), taskDir.getPath()));
                    }
                    if (!taskDir.exists() && !taskDir.mkdir()) {
                        throw new ProcessorStateException(
                            String.format("task directory [%s] doesn't exist and couldn't be created", taskDir.getPath()));
                    }
                }
            } else if (!taskDir.isDirectory()) {
                throw new ProcessorStateException(
                    String.format("state directory [%s] can't be created as there is an existing file with the same name", taskDir.getPath()));
            }
        }
        return taskDir;
    }

    private File getTaskDirectoryParentName(final TaskId taskId) {
        final String namedTopology = taskId.topologyName();
        if (namedTopology != null) {
            if (!hasNamedTopologies) {
                throw new IllegalStateException("Tried to lookup taskId with named topology, but StateDirectory thinks hasNamedTopologies = false");
            }
            return new File(stateDir, getNamedTopologyDirName(namedTopology));
        } else {
            return stateDir;
        }
    }

    private String getNamedTopologyDirName(final String topologyName) {
        return "__" + topologyName + "__";
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

        return taskDirIsEmpty(taskDir);
    }

    private boolean taskDirIsEmpty(final File taskDir) {
        final File[] storeDirs = taskDir.listFiles(pathname ->
                !pathname.getName().equals(CHECKPOINT_FILE_NAME));

        boolean taskDirEmpty = true;

        // if the task is stateless, storeDirs would be null
        if (storeDirs != null && storeDirs.length > 0) {
            for (final File file : storeDirs) {
                // We removed the task directory locking but some upgrading applications may still have old lock files on disk,
                // we just lazily delete those in this method since it's the only thing that would be affected by these
                if (file.getName().endsWith(LOCK_FILE_NAME)) {
                    if (!file.delete()) {
                        // If we hit an error deleting this just ignore it and move on, we'll retry again at some point
                        log.warn("Error encountered deleting lock file in {}", taskDir);
                    }
                } else {
                    // If it's not a lock file then the directory is not empty,
                    // but finish up the loop in case there's a lock file left to delete
                    log.trace("TaskDir {} was not empty, found {}", taskDir, file);
                    taskDirEmpty = false;
                }
            }
        }
        return taskDirEmpty;
    }

    /**
     * Get or create the directory for the global stores.
     * @return directory for the global stores
     * @throws ProcessorStateException if the global store directory does not exists and could not be created
     */
    File globalStateDir() {
        final File dir = new File(stateDir, "global");
        if (hasPersistentStores) {
            if (!dir.exists() && !dir.mkdir()) {
                throw new ProcessorStateException(
                    String.format("global state directory [%s] doesn't exist and couldn't be created", dir.getPath()));
            } else if (dir.exists() && !dir.isDirectory()) {
                throw new ProcessorStateException(
                    String.format("global state directory [%s] can't be created as there is an existing file with the same name", dir.getPath()));
            }
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
            return true;
        }
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

    /**
     * Expose for tests.
     */
    Thread lockOwner(final TaskId taskId) {
        return lockedTasksToOwner.get(taskId);
    }

    @Override
    public void close() {
        if (hasPersistentStores) {
            closeStartupTasks();
            try {
                stateDirLock.release();
                stateDirLockChannel.close();

                stateDirLock = null;
                stateDirLockChannel = null;
            } catch (final IOException e) {
                log.error("Unexpected exception while unlocking the state dir", e);
                throw new StreamsException(String.format("Failed to release the lock on the state directory [%s]", stateDir.getAbsolutePath()), e);
            }

            // all threads should be stopped and cleaned up by now, so none should remain holding a lock
            if (!lockedTasksToOwner.isEmpty()) {
                log.error("Some task directories still locked while closing state, this indicates unclean shutdown: {}", lockedTasksToOwner);
            }
        }
    }

    public synchronized void clean() {
        try {
            cleanStateAndTaskDirectoriesCalledByUser();
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

        try {
            if (hasPersistentStores && stateDir.exists() && !stateDir.delete()) {
                log.warn(
                    String.format("%s Failed to delete state store directory of %s for it is not empty",
                        logPrefix(), stateDir.getAbsolutePath())
                );
            }
        } catch (final SecurityException exception) {
            log.error(
                String.format("%s Failed to delete state store directory of %s due to an unexpected exception",
                    logPrefix(), stateDir.getAbsolutePath()),
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
        for (final TaskDirectory taskDir : listAllTaskDirectories()) {
            final String dirName = taskDir.file().getName();
            final TaskId id = parseTaskDirectoryName(dirName, taskDir.namedTopology());
            if (!lockedTasksToOwner.containsKey(id)) {
                try {
                    if (lock(id)) {
                        final long now = time.milliseconds();
                        final long lastModifiedMs = taskDir.file().lastModified();
                        if (now - cleanupDelayMs > lastModifiedMs) {
                            log.info("{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                logPrefix(), dirName, id, now - lastModifiedMs, cleanupDelayMs);
                            Utils.delete(taskDir.file());
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
        // Ok to ignore returned exception as it should be swallowed
        maybeCleanEmptyNamedTopologyDirs(true);
    }

    /**
     * Cleans up any leftover named topology directories that are empty, if any exist
     * @param logExceptionAsWarn if true, an exception will be logged as a warning
     *                       if false, an exception will be logged as error
     * @return the first IOException to be encountered
     */
    private IOException maybeCleanEmptyNamedTopologyDirs(final boolean logExceptionAsWarn) {
        if (!hasNamedTopologies) {
            return null;
        }

        final AtomicReference<IOException> firstException = new AtomicReference<>(null);
        final File[] namedTopologyDirs = stateDir.listFiles(pathname ->
                pathname.isDirectory() && NAMED_TOPOLOGY_DIR_PATH_NAME.matcher(pathname.getName()).matches()
        );
        if (namedTopologyDirs != null) {
            for (final File namedTopologyDir : namedTopologyDirs) {
                closeStartupTasks(task -> task.id().topologyName().equals(parseNamedTopologyFromDirectory(namedTopologyDir.getName())));
                final File[] contents = namedTopologyDir.listFiles();
                if (contents != null && contents.length == 0) {
                    try {
                        Utils.delete(namedTopologyDir);
                    } catch (final IOException exception) {
                        if (logExceptionAsWarn) {
                            log.warn(
                                String.format("%sSwallowed the following exception during deletion of named topology directory %s",
                                    logPrefix(), namedTopologyDir.getName()),
                                exception
                            );
                        } else {
                            log.error(
                                String.format("%s Failed to delete named topology directory %s with exception:",
                                    logPrefix(), namedTopologyDir.getName()),
                                exception
                            );
                        }
                        firstException.compareAndSet(null, exception);
                    }
                }
            }
        }
        return firstException.get();
    }

    /**
     * Clears out any local state found for the given NamedTopology after it was removed
     *
     * @throws StreamsException if cleanup failed
     */
    public void clearLocalStateForNamedTopology(final String topologyName) {
        final File namedTopologyDir = new File(stateDir, getNamedTopologyDirName(topologyName));
        if (!namedTopologyDir.exists() || !namedTopologyDir.isDirectory()) {
            log.debug("Tried to clear out the local state for NamedTopology {} but none was found", topologyName);
        }
        try {
            closeStartupTasks(task -> task.id().topologyName().equals(topologyName));
            Utils.delete(namedTopologyDir);
        } catch (final IOException e) {
            log.error("Hit an unexpected error while clearing local state for topology " + topologyName, e);
            throw new StreamsException("Unable to delete state for the named topology " + topologyName,
                                       e, new TaskId(-1, -1, topologyName)); // use dummy taskid to report source topology for this error
        }
    }

    private void cleanStateAndTaskDirectoriesCalledByUser() throws Exception {
        if (!lockedTasksToOwner.isEmpty()) {
            log.warn("Found some still-locked task directories when user requested to cleaning up the state, "
                + "since Streams is not running any more these will be ignored to complete the cleanup");
        }
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        for (final TaskDirectory taskDir : listAllTaskDirectories()) {
            final String dirName = taskDir.file().getName();
            final TaskId id = parseTaskDirectoryName(dirName, taskDir.namedTopology());
            try {
                log.info("{} Deleting task directory {} for {} as user calling cleanup.",
                    logPrefix(), dirName, id);

                if (lockedTasksToOwner.containsKey(id)) {
                    log.warn("{} Task {} in state directory {} was still locked by {}",
                        logPrefix(), dirName, id, lockedTasksToOwner.get(id));
                }
                Utils.delete(taskDir.file());
            } catch (final IOException exception) {
                log.error(
                    String.format("%s Failed to delete task directory %s for %s with exception:",
                        logPrefix(), dirName, id),
                    exception
                );
                firstException.compareAndSet(null, exception);
            }
        }

        firstException.compareAndSet(null, maybeCleanEmptyNamedTopologyDirs(false));

        final Exception exception = firstException.get();
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * List all of the task directories that are non-empty
     * @return The list of all the non-empty local directories for stream tasks
     */
    List<TaskDirectory> listNonEmptyTaskDirectories() {
        return listTaskDirectories(pathname -> {
            if (!pathname.isDirectory() || !TASK_DIR_PATH_NAME.matcher(pathname.getName()).matches()) {
                return false;
            } else {
                return !taskDirIsEmpty(pathname);
            }
        });
    }

    /**
     * List all of the task directories along with their parent directory if they belong to a named topology
     * @return The list of all the existing local directories for stream tasks
     */
    List<TaskDirectory> listAllTaskDirectories() {
        return listTaskDirectories(pathname -> pathname.isDirectory() && TASK_DIR_PATH_NAME.matcher(pathname.getName()).matches());
    }

    private List<TaskDirectory> listTaskDirectories(final FileFilter filter) {
        final List<TaskDirectory> taskDirectories = new ArrayList<>();
        if (hasPersistentStores && stateDir.exists()) {
            if (hasNamedTopologies) {
                for (final File namedTopologyDir : listNamedTopologyDirs()) {
                    final String namedTopology = parseNamedTopologyFromDirectory(namedTopologyDir.getName());
                    final File[] taskDirs = namedTopologyDir.listFiles(filter);
                    if (taskDirs != null) {
                        taskDirectories.addAll(Arrays.stream(taskDirs)
                            .map(f -> new TaskDirectory(f, namedTopology)).collect(Collectors.toList()));
                    }
                }
            } else {
                final File[] taskDirs =
                    stateDir.listFiles(filter);
                if (taskDirs != null) {
                    taskDirectories.addAll(Arrays.stream(taskDirs)
                                               .map(f -> new TaskDirectory(f, null)).collect(Collectors.toList()));
                }
            }
        }

        return taskDirectories;
    }

    private List<File> listNamedTopologyDirs() {
        final File[] namedTopologyDirectories = stateDir.listFiles(f -> f.getName().startsWith("__") &&  f.getName().endsWith("__"));
        return namedTopologyDirectories != null ? Arrays.asList(namedTopologyDirectories) : Collections.emptyList();
    }

    private String parseNamedTopologyFromDirectory(final String dirName) {
        return dirName.substring(2, dirName.length() - 2);
    }

    private FileLock tryLock(final FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (final OverlappingFileLockException e) {
            return null;
        }
    }

    public static class TaskDirectory {
        private final File file;
        private final String namedTopology; // may be null if hasNamedTopologies = false

        TaskDirectory(final File file, final String namedTopology) {
            this.file = file;
            this.namedTopology = namedTopology;
        }

        public File file() {
            return file;
        }

        public String namedTopology() {
            return namedTopology;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskDirectory that = (TaskDirectory) o;
            return file.equals(that.file) &&
                Objects.equals(namedTopology, that.namedTopology);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, namedTopology);
        }
    }
}
