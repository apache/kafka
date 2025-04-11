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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.errors.TaskIdFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The task ID representation composed as subtopology plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskId.class);

    public static final String NAMED_TOPOLOGY_DELIMITER = "__";

    /** The ID of the subtopology. */
    private final int subtopology;
    /** The ID of the partition. */
    private final int partition;

    /** The namedTopology that this task belongs to, or null if it does not belong to one */
    private final String topologyName;

    public TaskId(final int subtopology, final int partition) {
        this(subtopology, partition, null);
    }

    public TaskId(final int subtopology, final int partition, final String topologyName) {
        this.subtopology = subtopology;
        this.partition = partition;
        if (topologyName != null && topologyName.length() == 0) {
            LOG.warn("Empty string passed in for task's namedTopology, since NamedTopology name cannot be empty, we "
                         + "assume this task does not belong to a NamedTopology and downgrade this to null");
            this.topologyName = null;
        } else {
            this.topologyName = topologyName;
        }
    }

    public int subtopology() {
        return subtopology;
    }

    public int partition() {
        return partition;
    }

    /**
     * Experimental feature -- will return null
     */
    public String topologyName() {
        return topologyName;
    }

    @Override
    public String toString() {
        return topologyName != null ? topologyName + NAMED_TOPOLOGY_DELIMITER + subtopology + "_" + partition : subtopology + "_" + partition;
    }

    /**
     * @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
     */
    public static TaskId parse(final String taskIdStr) {
        try {
            final int namedTopologyDelimiterIndex = taskIdStr.indexOf(NAMED_TOPOLOGY_DELIMITER);
            // If there is no copy of the NamedTopology delimiter, this task has no named topology and only one `_` char
            if (namedTopologyDelimiterIndex < 0) {
                final int index = taskIdStr.indexOf('_');

                final int topicGroupId = Integer.parseInt(taskIdStr.substring(0, index));
                final int partition = Integer.parseInt(taskIdStr.substring(index + 1));

                return new TaskId(topicGroupId, partition);
            } else {
                final int topicGroupIdIndex = namedTopologyDelimiterIndex + 2;
                final int subtopologyPartitionDelimiterIndex = taskIdStr.indexOf('_', topicGroupIdIndex);

                final String namedTopology = taskIdStr.substring(0, namedTopologyDelimiterIndex);
                final int topicGroupId = Integer.parseInt(taskIdStr.substring(topicGroupIdIndex, subtopologyPartitionDelimiterIndex));
                final int partition = Integer.parseInt(taskIdStr.substring(subtopologyPartitionDelimiterIndex + 1));

                return new TaskId(topicGroupId, partition, namedTopology);
            }
        } catch (final Exception e) {
            throw new TaskIdFormatException(taskIdStr);
        }
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

        if (subtopology != taskId.subtopology || partition != taskId.partition) {
            return false;
        }

        if (topologyName != null && taskId.topologyName != null) {
            return topologyName.equals(taskId.topologyName);
        } else {
            return topologyName == null && taskId.topologyName == null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtopology, partition, topologyName);
    }

    @Override
    public int compareTo(final TaskId other) {
        if (topologyName != null && other.topologyName != null) {
            final int comparingNamedTopologies = topologyName.compareTo(other.topologyName);
            if (comparingNamedTopologies != 0) {
                return comparingNamedTopologies;
            }
        } else if (topologyName != null || other.topologyName != null) {
            LOG.error("Tried to compare this = {} with other = {}, but only one had a valid named topology", this, other);
            throw new IllegalStateException("Can't compare a TaskId with a namedTopology to one without");
        }
        final int comparingTopicGroupId = Integer.compare(this.subtopology, other.subtopology);
        return comparingTopicGroupId != 0 ? comparingTopicGroupId : Integer.compare(this.partition, other.partition);
    }
}
