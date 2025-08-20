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
package org.apache.kafka.streams.processor.assignment;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;

/**
 * This is a simple container class used during the assignment process to distinguish
 * TopicPartitions type. Since the assignment logic can depend on the type of topic we're
 * looking at, and the rack information of the partition, this container class should have
 * everything necessary to make informed task assignment decisions.
 */
public interface TaskTopicPartition {
    /**
     *
     * @return the {@code TopicPartition} for this task.
     */
    TopicPartition topicPartition();

    /**
     *
     * @return whether the underlying topic is a source topic or not. Source changelog topics
     *         are both source topics and changelog topics.
     */
    boolean isSource();

    /**
     *
     * @return whether the underlying topic is a changelog topic or not. Source changelog topics
     *         are both source topics and changelog topics.
     */
    boolean isChangelog();

    /**
     *
     * @return the broker rack ids on which this topic partition resides. If no information could
     *         be found, this will return an empty optional value.
     */
    Optional<Set<String>> rackIds();
}
