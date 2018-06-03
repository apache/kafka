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

package org.apache.kafka.connect.health;

import java.util.Objects;

/**
 * Describes the state, IDs, and any errors of a connector task.
 */
public class TaskState extends AbstractState {

    private final int taskId;

    /**
     * Provides an instance of {@link TaskState}.
     *
     * @param taskId   the id associated with the connector task
     * @param state    the status of the task, may not be {@code null} or empty
     * @param workerId id of the worker the task is associated with, may not be {@code null} or empty
     * @param trace    error message if that task had failed or errored out, may be {@code null} or empty
     */
    public TaskState(int taskId, String state, String workerId, String trace) {
        super(state, workerId, trace);
        this.taskId = taskId;
    }

    /**
     * Provides the ID of the task.
     *
     * @return the task ID
     */
    public int taskId() {
        return taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaskState taskState = (TaskState) o;

        return taskId == taskState.taskId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}
