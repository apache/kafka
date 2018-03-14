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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class AssignedStreamsTasks extends AssignedTasks<StreamTask> implements RestoringTasks {
    private final Logger log;
    private final TaskAction<StreamTask> maybeCommitAction;
    private int committed = 0;

    AssignedStreamsTasks(final LogContext logContext) {
        super(logContext, "stream task");

        this.log = logContext.logger(getClass());

        maybeCommitAction = new TaskAction<StreamTask>() {
            @Override
            public String name() {
                return "maybeCommit";
            }

            @Override
            public void apply(final StreamTask task) {
                if (task.commitNeeded()) {
                    committed++;
                    task.commit();
                    log.debug("Committed active task {} per user request in", task.id());
                }
            }
        };
    }

    @Override
    public StreamTask restoringTaskFor(final TopicPartition partition) {
        return restoringByPartition.get(partition);
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommit() {
        committed = 0;
        applyToRunningTasks(maybeCommitAction);
        return committed;
    }

    /**
     * Returns a map of offsets up to which the records can be deleted; this function should only be called
     * after the commit call to make sure all consumed offsets are actually committed as well
     */
    Map<TopicPartition, Long> recordsToDelete() {
        final Map<TopicPartition, Long> recordsToDelete = new HashMap<>();
        for (final StreamTask task : running.values()) {
            recordsToDelete.putAll(task.purgableOffsets());
        }

        return recordsToDelete;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process() {
        int processed = 0;

        final Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final StreamTask task = it.next().getValue();

            try {
                if (task.isProcessable() && task.process()) {
                    processed++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                    "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }

        return processed;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        int punctuated = 0;
        final Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final StreamTask task = it.next().getValue();
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                    "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    void migrateStoreForTasks(final Collection<TaskId> storePrepareTasksIds) {
        Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<TaskId, StreamTask> next = it.next();
            final TaskId taskId = next.getKey();
            if (storePrepareTasksIds.contains(taskId)) {
                final StreamTask task = next.getValue();
                log.debug("Closing stream task {} for store upgrade", taskId);
                try {
                    task.close(true, false);
                } catch (final Exception e) {
                    log.error("Failed to remove suspended stream task {} due to the following error:", taskId, e);
                    continue;
                } finally {
                    it.remove();
                }


                final TaskId prepareTaskId = new TaskId(taskId);
                final File taskDir = task.stateDirectory.directoryForTask(taskId);
                final File preparedTaskDir = task.stateDirectory.directoryForTask(prepareTaskId);

                migrateStores(taskDir, preparedTaskDir);

                log.debug("Deleting task directory {}", preparedTaskDir);
                if (!preparedTaskDir.delete()) {
                    log.error("Failed to delete the task directory {}.", preparedTaskDir);
                }
            }
        }


        it = created.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<TaskId, StreamTask> next = it.next();
            final TaskId taskId = next.getKey();
            if (storePrepareTasksIds.contains(taskId)) {
                final StreamTask task = next.getValue();

                final TaskId prepareTaskId = new TaskId(taskId);
                final File taskDir = task.stateDirectory.directoryForTask(taskId);
                final File preparedTaskDir = task.stateDirectory.directoryForTask(prepareTaskId);

                migrateStores(taskDir, preparedTaskDir);

                log.debug("Deleting task directory {}", preparedTaskDir);
                if (!preparedTaskDir.delete()) {
                    log.error("Failed to delete the task directory {}.", preparedTaskDir);
                }
            }
        }
    }

    private void migrateStores(final File taskDir,
                               final File preparedTaskDir) {
        final String[] preparedStores = preparedTaskDir.list();
        if (preparedStores == null) {
            log.warn("Task directory {} with prepared stores is empty.", preparedTaskDir);
        } else {
            for (final String storeName : preparedStores) {
                if (storeName.equals(".lock")) {
                    final File lockFile = new File(preparedTaskDir, ".lock");
                    if (!lockFile.delete()) {
                        log.error("Failed to delete lock file {}.", lockFile);
                    }
                    continue;
                }
                if (storeName.equals("rocksdb")) {
                    final File prepareRocksDbDirectory = new File(preparedTaskDir, "rocksdb");
                    migrateStores(new File(taskDir, "rocksdb"), prepareRocksDbDirectory);
                    if (!prepareRocksDbDirectory.delete()) {
                        log.error("Failed to delete directory {}.", prepareRocksDbDirectory);
                    }
                    continue;
                }
                final File oldStoreDirectory = new File(taskDir, storeName);
                final File preparedStoreDirectory = new File(preparedTaskDir, storeName);

                try {
                    log.debug("Deleting old store directory {}", oldStoreDirectory);
                    Utils.delete(oldStoreDirectory);
                } catch (final IOException e) {
                    log.error("Failed to delete the store directory {}.", oldStoreDirectory, e);
                }

                if (!preparedStoreDirectory.renameTo(oldStoreDirectory)) {
                    log.error("Failed to move prepared store {} from {} into task directory {}.", oldStoreDirectory, preparedTaskDir, taskDir);
                }
            }
        }
    }
}
