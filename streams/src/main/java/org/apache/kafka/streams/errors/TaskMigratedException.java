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
package org.apache.kafka.streams.errors;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

/**
 * Indicates that a task got migrated to another thread.
 * Thus, the task raising this exception can be cleaned up and closed as "zombie".
 */
public class TaskMigratedException extends StreamsException {

    private final static long serialVersionUID = 1L;

    private final TaskId taskId;

    public TaskMigratedException(final TaskId taskId,
                                 final TopicPartition topicPartition,
                                 final long endOffset,
                                 final long pos) {
        this(taskId, String.format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
            topicPartition,
            endOffset,
            pos), null);
    }

    public TaskMigratedException(final TaskId taskId) {
        this(taskId, String.format("Task %s is unexpectedly closed during processing", taskId), null);
    }

    public TaskMigratedException(final TaskId taskId,
                                 final Throwable throwable) {
        this(taskId, String.format("Client request for task %s has been fenced due to a rebalance", taskId), throwable);
    }

    public TaskMigratedException(final TaskId taskId,
                                 final String message,
                                 final Throwable throwable) {
        super(message, throwable);
        this.taskId = taskId;
    }

    public TaskId migratedTaskId() {
        return taskId;
    }

    // this is for unit test only
    public TaskMigratedException() {
        this(null, "A task has been migrated unexpectedly", null);
    }
}
