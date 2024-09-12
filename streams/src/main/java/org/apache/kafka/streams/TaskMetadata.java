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
package org.apache.kafka.streams;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Metadata of a task.
 */
public interface TaskMetadata {

    /**
     * Task ID of the task.
     *
     * @return task ID consisting of subtopology and partition ID
     */
    TaskId taskId();

    /**
     * Source topic partitions of the task.
     *
     * @return source topic partitions
     */
    Set<TopicPartition> topicPartitions();

    /**
     * Offsets of the source topic partitions committed so far by the task.
     *
     * @return map from source topic partitions to committed offsets
     */
    Map<TopicPartition, Long> committedOffsets();

    /**
     * End offsets of the source topic partitions of the task.
     *
     * @return map source topic partition to end offsets
     */
    Map<TopicPartition, Long> endOffsets();

    /**
     * Time task idling started. If the task is not currently idling it will return empty.
     *
     * @return time when task idling started, empty {@code Optional} if the task is currently not idling
     */
    Optional<Long> timeCurrentIdlingStarted();

    /**
     * Compares the specified object with this TaskMetadata. Returns {@code true} if and only if the specified object is
     * also a TaskMetadata and both {@code taskId()} and {@code topicPartitions()} are equal.
     *
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     */
    boolean equals(final Object o);

    /**
     * Returns the hash code value for this TaskMetadata. The hash code of a list is defined to be the result of the following calculation:
     * <pre>
     * {@code
     * Objects.hash(taskId(), topicPartitions());
     * }
     * </pre>
     *
     * @return a hash code value for this object.
     */
    int hashCode();

}
