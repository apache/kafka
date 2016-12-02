/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
 * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
 * thread-safe.
 */
public class StateDirectory {

    static final String LOCK_FILE_NAME = ".lock";
    private static final Logger log = LoggerFactory.getLogger(StateDirectory.class);

    private final File stateDir;
    private final HashMap<TaskId, FileChannel> channels = new HashMap<>();
    private final HashMap<TaskId, FileLock> locks = new HashMap<>();

    public StateDirectory(final String applicationId, final String stateDirConfig) {
        final File baseDir = new File(stateDirConfig);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new ProcessorStateException(String.format("state directory [%s] doesn't exist and couldn't be created",
                                                            stateDirConfig));
        }
        stateDir = new File(baseDir, applicationId);
        if (!stateDir.exists() && !stateDir.mkdir()) {
            throw new ProcessorStateException(String.format("state directory [%s] doesn't exist and couldn't be created",
                                                            stateDir.getPath()));
        }

    }

    /**
     * Get or create the directory for the {@link TaskId}
     * @param taskId
     * @return directory for the {@link TaskId}
     */
    public File directoryForTask(final TaskId taskId) {
        final File taskDir = new File(stateDir, taskId.toString());
        if (!taskDir.exists() && !taskDir.mkdir()) {
            throw new ProcessorStateException(String.format("task directory [%s] doesn't exist and couldn't be created",
                                                            taskDir.getPath()));
        }
        return taskDir;
    }

    /**
     * Get the lock for the {@link TaskId}s directory if it is available
     * @param taskId
     * @param retry
     * @return true if successful
     * @throws IOException
     */
    public boolean lock(final TaskId taskId, int retry) throws IOException {
        // we already have the lock so bail out here
        if (locks.containsKey(taskId)) {
            return true;
        }
        final File lockFile = new File(directoryForTask(taskId), LOCK_FILE_NAME);
        final FileChannel channel = getOrCreateFileChannel(taskId, lockFile.toPath());

        FileLock lock = tryAcquireLock(channel);
        while (lock == null && retry > 0) {
            try {
                Thread.sleep(200);
            } catch (Exception ex) {
                // do nothing
            }
            retry--;
            lock = tryAcquireLock(channel);
        }
        if (lock != null) {
            locks.put(taskId, lock);
        }
        return lock != null;
    }

    /**
     * Unlock the state directory for the given {@link TaskId}
     * @param taskId
     * @throws IOException
     */
    public void unlock(final TaskId taskId) throws IOException {
        final FileLock lock = locks.remove(taskId);
        if (lock != null) {
            lock.release();
            final FileChannel fileChannel = channels.remove(taskId);
            if (fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    /**
     * Remove the directories for any {@link TaskId}s that are no-longer
     * owned by this {@link StreamThread} and aren't locked by either
     * another process or another {@link StreamThread}
     */
    public void cleanRemovedTasks() {
        final File[] taskDirs = listTaskDirectories();
        if (taskDirs == null || taskDirs.length == 0) {
            return; // nothing to do
        }

        for (File taskDir : taskDirs) {
            final String dirName = taskDir.getName();
            TaskId id = TaskId.parse(dirName);
            if (!locks.containsKey(id)) {
                try {
                    if (lock(id, 0)) {
                        log.info("Deleting obsolete state directory {} for task {}", dirName, id);
                        Utils.delete(taskDir);
                    }
                } catch (OverlappingFileLockException e) {
                    // locked by another thread
                } catch (IOException e) {
                    log.error("Failed to lock the state directory due to an unexpected exception", e);
                } finally {
                    try {
                        unlock(id);
                    } catch (IOException e) {
                        log.error("Failed to release the state directory lock");
                    }
                }
            }
        }

    }

    /**
     * List all of the task directories
     * @return The list of all the existing local directories for stream tasks
     */
    public File[] listTaskDirectories() {
        return stateDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(final File pathname) {
                final String name = pathname.getName();
                return pathname.isDirectory() && name.matches("\\d+_\\d+");
            }
        });
    }

    private FileChannel getOrCreateFileChannel(final TaskId taskId, final Path lockPath) throws IOException {
        if (!channels.containsKey(taskId)) {
            channels.put(taskId, FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        }
        return channels.get(taskId);
    }

    private FileLock tryAcquireLock(final FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }
}
