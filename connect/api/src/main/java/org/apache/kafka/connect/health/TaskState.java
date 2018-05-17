/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.health;

import java.util.Objects;

public class TaskState extends AbstractState implements Comparable<TaskState> {

    private final int taskId;

    public TaskState(int taskId, String state, String worker, String msg) {
        super(state, worker, msg);
        this.taskId = taskId;
    }

    public int taskId() {
        return taskId;
    }

    @Override
    public int compareTo(TaskState that) {
        return Integer.compare(this.taskId, that.taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TaskState)) {
            return false;
        }
        TaskState other = (TaskState) o;
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}
