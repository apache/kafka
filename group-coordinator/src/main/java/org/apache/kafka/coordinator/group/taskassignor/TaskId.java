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
package org.apache.kafka.coordinator.group.taskassignor;

import java.util.Objects;

public final class TaskId {

    private final String subtopologyId;
    private final int partition;

    public TaskId(final String subtopologyId, final int partition) {
        this.subtopologyId = subtopologyId;
        this.partition = partition;
    }

    public String subtopologyId() {
        return subtopologyId;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TaskId taskId = (TaskId) o;
        return partition == taskId.partition && Objects.equals(subtopologyId, taskId.subtopologyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            subtopologyId,
            partition
        );
    }

    @Override
    public String toString() {
        return "TaskId{" +
            "subtopologyId='" + subtopologyId + '\'' +
            ", partition=" + partition +
            '}';
    }

    public int compareTo(final TaskId other) {
        return this.hashCode() - other.hashCode();
    }
}
