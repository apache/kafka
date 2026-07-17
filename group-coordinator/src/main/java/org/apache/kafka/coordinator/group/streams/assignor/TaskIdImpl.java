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
package org.apache.kafka.coordinator.group.streams.assignor;

import java.util.Comparator;
import java.util.Objects;

/**
 * The identifier for a task, consisting of the subtopology ID and the partition. This is an internal
 * helper used by the built-in assignors; the public assignor API addresses tasks by subtopology ID
 * and partition directly.
 *
 * @param subtopologyId The unique identifier of the subtopology.
 * @param partition     The partition of the input topics this task is processing.
 */
public record TaskIdImpl(String subtopologyId, int partition) implements Comparable<TaskIdImpl> {

    public TaskIdImpl {
        Objects.requireNonNull(subtopologyId);
    }

    @Override
    public int compareTo(final TaskIdImpl other) {
        return Comparator.comparing(TaskIdImpl::subtopologyId)
            .thenComparingInt(TaskIdImpl::partition)
            .compare(this, other);
    }

    @Override
    public String toString() {
        return subtopologyId + '_' + partition;
    }

}
