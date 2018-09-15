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
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class AssignedStreamsTasks extends AssignedTasks<StreamTask> implements RestoringTasks {
    AssignedStreamsTasks(final LogContext logContext) {
        super(logContext, "stream task");
    }

    @Override
    public StreamTask restoringTaskFor(final TopicPartition partition) {
        return restoringByPartition.get(partition);
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitPerUserRequested() {
        int committed = 0;
        RuntimeException firstException = null;

        for (final Iterator<StreamTask> it = running().iterator(); it.hasNext(); ) {
            final StreamTask task = it.next();
            try {
                if (task.commitRequested() && task.commitNeeded()) {
                    task.commit();
                    committed++;
                    log.debug("Committed active task {} per user request in", task.id());
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to commit {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException t) {
                log.error("Failed to commit StreamTask {} due to the following error:",
                        task.id(),
                        t);
                if (firstException == null) {
                    firstException = t;
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }

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
    int process(final long now) {
        int processed = 0;

        final Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final StreamTask task = it.next().getValue();
            try {
                if (task.isProcessable(now) && task.process()) {
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

}
