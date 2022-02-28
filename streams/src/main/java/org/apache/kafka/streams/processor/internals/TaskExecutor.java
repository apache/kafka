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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;

/**
 * Single-threaded executor class for the active tasks assigned to this thread.
 */
public class TaskExecutor {

    private final Logger log;

    private final boolean hasNamedTopologies;
    private final ProcessingMode processingMode;
    private final Tasks tasks;
    private final TaskExecutionMetadata taskExecutionMetadata;

    public TaskExecutor(final Tasks tasks,
                        final TaskExecutionMetadata taskExecutionMetadata,
                        final ProcessingMode processingMode,
                        final boolean hasNamedTopologies,
                        final LogContext logContext) {
        this.tasks = tasks;
        this.taskExecutionMetadata = taskExecutionMetadata;
        this.processingMode = processingMode;
        this.hasNamedTopologies = hasNamedTopologies;
        this.log = logContext.logger(getClass());
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     * @throws StreamsException      if any task threw an exception while processing
     */
    int process(final int maxNumRecords, final Time time) {
        int totalProcessed = 0;
        Task lastProcessed = null;

        for (final Task task : tasks.activeTasks()) {
            final long now = time.milliseconds();
            try {
                if (taskExecutionMetadata.canProcessTask(task, now)) {
                    lastProcessed = task;
                    totalProcessed += processTask(task, maxNumRecords, now, time);
                }
            } catch (final Throwable t) {
                taskExecutionMetadata.registerTaskError(task, t, now);
                tasks.removeTaskFromCuccessfullyProcessedBeforeClosing(lastProcessed);
                commitSuccessfullyProcessedTasks();
                throw t;
            }
        }

        return totalProcessed;
    }

    private long processTask(final Task task, final int maxNumRecords, final long begin, final Time time) {
        int processed = 0;
        long now = begin;

        final long then = now;
        try {
            while (processed < maxNumRecords && task.process(now)) {
                task.clearTaskTimeout();
                processed++;
            }
            // TODO: enable regardless of whether using named topologies
            if (processed > 0 && hasNamedTopologies && processingMode != EXACTLY_ONCE_V2) {
                log.trace("Successfully processed task {}", task.id());
                tasks.addToSuccessfullyProcessed(task);
            }
        } catch (final TimeoutException timeoutException) {
            // TODO consolidate TimeoutException retries with general error handling
            task.maybeInitTaskTimeoutOrThrow(now, timeoutException);
            log.error(
                String.format(
                    "Could not complete processing records for %s due to the following exception; will move to next task and retry later",
                    task.id()),
                timeoutException
            );
        } catch (final TaskMigratedException e) {
            log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
            throw e;
        } catch (final StreamsException e) {
            log.error(String.format("Failed to process stream task %s due to the following error:", task.id()), e);
            e.setTaskId(task.id());
            throw e;
        } catch (final RuntimeException e) {
            log.error(String.format("Failed to process stream task %s due to the following error:", task.id()), e);
            throw new StreamsException(e, task.id());
        } finally {
            now = time.milliseconds();
            task.recordProcessBatchTime(now - then);
        }
        return processed;
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @throws TimeoutException if committing offsets failed due to TimeoutException (non-EOS)
     * @throws TaskCorruptedException if committing offsets failed due to TimeoutException (EOS)
     * @param consumedOffsetsAndMetadata an empty map that will be filled in with the prepared offsets
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commitTasksAndMaybeUpdateCommittableOffsets(final Collection<Task> tasksToCommit,
                                                    final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadata) {
        int committed = 0;
        for (final Task task : tasksToCommit) {
            // we need to call commitNeeded first since we need to update committable offsets
            if (task.commitNeeded()) {
                final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = task.prepareCommit();
                if (!offsetAndMetadata.isEmpty()) {
                    consumedOffsetsAndMetadata.put(task, offsetAndMetadata);
                }
            }
        }

        commitOffsetsOrTransaction(consumedOffsetsAndMetadata);

        for (final Task task : tasksToCommit) {
            if (task.commitNeeded()) {
                task.clearTaskTimeout();
                ++committed;
                task.postCommit(false);
            }
        }
        return committed;
    }

    /**
     * Caution: do not invoke this directly if it's possible a rebalance is occurring, as the commit will fail. If
     * this is a possibility, prefer the {@link #commitTasksAndMaybeUpdateCommittableOffsets} instead.
     *
     * @throws TaskMigratedException   if committing offsets failed due to CommitFailedException (non-EOS)
     * @throws TimeoutException        if committing offsets failed due to TimeoutException (non-EOS)
     * @throws TaskCorruptedException  if committing offsets failed due to TimeoutException (EOS)
     */
    void commitOffsetsOrTransaction(final Map<Task, Map<TopicPartition, OffsetAndMetadata>> offsetsPerTask) {
        log.debug("Committing task offsets {}", offsetsPerTask.entrySet().stream().collect(Collectors.toMap(t -> t.getKey().id(), Entry::getValue))); // avoid logging actual Task objects

        final Set<TaskId> corruptedTasks = new HashSet<>();

        if (!offsetsPerTask.isEmpty()) {
            if (processingMode == EXACTLY_ONCE_ALPHA) {
                for (final Map.Entry<Task, Map<TopicPartition, OffsetAndMetadata>> taskToCommit : offsetsPerTask.entrySet()) {
                    final Task task = taskToCommit.getKey();
                    try {
                        tasks.streamsProducerForTask(task.id())
                            .commitTransaction(taskToCommit.getValue(), tasks.mainConsumer().groupMetadata());
                        updateTaskCommitMetadata(taskToCommit.getValue());
                    } catch (final TimeoutException timeoutException) {
                        log.error(
                            String.format("Committing task %s failed.", task.id()),
                            timeoutException
                        );
                        corruptedTasks.add(task.id());
                    }
                }
            } else {
                final Map<TopicPartition, OffsetAndMetadata> allOffsets = offsetsPerTask.values().stream()
                    .flatMap(e -> e.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                if (processingMode == EXACTLY_ONCE_V2) {
                    try {
                        tasks.threadProducer().commitTransaction(allOffsets, tasks.mainConsumer().groupMetadata());
                        updateTaskCommitMetadata(allOffsets);
                    } catch (final TimeoutException timeoutException) {
                        log.error(
                            String.format("Committing task(s) %s failed.",
                                offsetsPerTask
                                    .keySet()
                                    .stream()
                                    .map(t -> t.id().toString())
                                    .collect(Collectors.joining(", "))),
                            timeoutException
                        );
                        offsetsPerTask
                            .keySet()
                            .forEach(task -> corruptedTasks.add(task.id()));
                    }
                } else {
                    try {
                        tasks.mainConsumer().commitSync(allOffsets);
                        updateTaskCommitMetadata(allOffsets);
                    } catch (final CommitFailedException error) {
                        throw new TaskMigratedException("Consumer committing offsets failed, " +
                            "indicating the corresponding thread is no longer part of the group", error);
                    } catch (final TimeoutException timeoutException) {
                        log.error(
                            String.format("Committing task(s) %s failed.",
                                offsetsPerTask
                                    .keySet()
                                    .stream()
                                    .map(t -> t.id().toString())
                                    .collect(Collectors.joining(", "))),
                            timeoutException
                        );
                        throw timeoutException;
                    } catch (final KafkaException error) {
                        throw new StreamsException("Error encountered committing offsets via consumer", error);
                    }
                }
            }

            if (!corruptedTasks.isEmpty()) {
                throw new TaskCorruptedException(corruptedTasks);
            }
        }
    }

    private void updateTaskCommitMetadata(final Map<TopicPartition, OffsetAndMetadata> allOffsets) {
        for (final Task task: tasks.activeTasks()) {
            if (task instanceof StreamTask) {
                for (final TopicPartition topicPartition : task.inputPartitions()) {
                    if (allOffsets.containsKey(topicPartition)) {
                        ((StreamTask) task).updateCommittedOffsets(topicPartition, allOffsets.get(topicPartition).offset());
                    }
                }
            }
        }
    }

    private void commitSuccessfullyProcessedTasks() {
        if (!tasks.successfullyProcessed().isEmpty()) {
            log.info("Streams encountered an error when processing tasks." +
                " Will commit all previously successfully processed tasks {}",
                tasks.successfullyProcessed().stream().map(Task::id));
            commitTasksAndMaybeUpdateCommittableOffsets(tasks.successfullyProcessed(), new HashMap<>());
        }
        tasks.clearSuccessfullyProcessed();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        int punctuated = 0;

        for (final Task task : tasks.activeTasks()) {
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                    "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final StreamsException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                e.setTaskId(task.id());
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw new StreamsException(e, task.id());
            }
        }

        return punctuated;
    }
}
