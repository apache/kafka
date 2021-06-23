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
 * Represents the state of a single task running within a {@link KafkaStreams} application.
 */
public interface TaskMetadata {

    /**
     * @return the basic task metadata such as subtopology and partition id
     */
    TaskId taskId();

    /**
     * This function will return a set of the current TopicPartitions
     * @return set of topic partitions
     */
    Set<TopicPartition> topicPartitions();

    /**
     * This function will return a map of TopicPartitions and the highest committed offset seen so far
     * @return map with an entry for all topic partitions with the committed offset as a value
     */
    Map<TopicPartition, Long> committedOffsets();

    /**
     * This function will return a map of TopicPartitions and the highest offset seen so far in the Topic
     * @return map with an entry for all topic partitions with the highest offset as a value
     */
    Map<TopicPartition, Long> endOffsets();

    /**
     * This function will return the time task idling started, if the task is not currently idling it will return empty
     * @return A filled {@code Optional} with the time where task idling started, and empty {@code Optional} otherwise
     */
    Optional<Long> timeCurrentIdlingStarted();

    /**
     * Compares the specified object with this TaskMetadata. Returns {@code true} if and only if the specified object is
     * also a TaskMetadata and both {@code taskId()} and {@code topicPartitions()} are equal.
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
     * @return a hash code value for this object.
     */
    int hashCode();

}
