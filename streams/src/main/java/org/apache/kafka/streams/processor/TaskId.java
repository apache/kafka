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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.MIN_NAMED_TOPOLOGY_VERSION;

/**
 * The task ID representation composed as topic group ID plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskId.class);

    /** The ID of the topic group. */
    public final int topicGroupId;
    /** The ID of the partition. */
    public final int partition;
    /** The namedTopology that this task belongs to, or null if it does not belong to one */
    protected final String namedTopology;

    public TaskId(final int topicGroupId, final int partition) {
        this(topicGroupId, partition, null);
    }

    public TaskId(final int topicGroupId, final int partition, final String namedTopology) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
        if (namedTopology != null && namedTopology.length() == 0) {
            LOG.warn("Empty string passed in for task's namedTopology, since NamedTopology name cannot be empty, we "
                         + "assume this task does not belong to a NamedTopology and downgrade this to null");
            this.namedTopology = null;
        } else {
            this.namedTopology = namedTopology;
        }
    }

    @Override
    public String toString() {
        return namedTopology != null ? namedTopology + "_" + topicGroupId + "_" + partition : topicGroupId + "_" + partition;
    }

    public String toTaskDirString() {
        return topicGroupId + "_" + partition;
    }

    /**
     *  Parse the task directory name (of the form topicGroupId_partition) and construct the TaskId with the
     *  optional namedTopology (may be null)
     *
     *  @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
     */
    public static TaskId parseTaskDirectoryName(final String taskIdStr, final String namedTopology) {
        final int index = taskIdStr.indexOf('_');
        if (index <= 0 || index + 1 >= taskIdStr.length()) {
            throw new TaskIdFormatException(taskIdStr);
        }

        try {
            final int topicGroupId = Integer.parseInt(taskIdStr.substring(0, index));
            final int partition = Integer.parseInt(taskIdStr.substring(index + 1));

            return new TaskId(topicGroupId, partition, namedTopology);
        } catch (final Exception e) {
            throw new TaskIdFormatException(taskIdStr);
        }
    }

    /**
     * @throws IOException if cannot write to output stream
     */
    public void writeTo(final DataOutputStream out, final int version) throws IOException {
        out.writeInt(topicGroupId);
        out.writeInt(partition);
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            if (namedTopology != null) {
                out.writeInt(namedTopology.length());
                out.writeChars(namedTopology);
            } else {
                out.writeInt(0);
            }
        }
    }

    /**
     * @throws IOException if cannot read from input stream
     */
    public static TaskId readFrom(final DataInputStream in, final int version) throws IOException {
        final int topicGroupId = in.readInt();
        final int partition = in.readInt();
        final String namedTopology;
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            final int numNamedTopologyChars = in.readInt();
            final StringBuilder namedTopologyBuilder = new StringBuilder();
            for (int i = 0; i < numNamedTopologyChars; ++i) {
                namedTopologyBuilder.append(in.readChar());
            }
            namedTopology = namedTopologyBuilder.toString();
        } else {
            namedTopology = null;
        }
        return new TaskId(topicGroupId, partition, getNamedTopologyOrElseNull(namedTopology));
    }

    public void writeTo(final ByteBuffer buf, final int version) {
        buf.putInt(topicGroupId);
        buf.putInt(partition);
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            if (namedTopology != null) {
                buf.putInt(namedTopology.length());
                for (final char c : namedTopology.toCharArray()) {
                    buf.putChar(c);
                }
            } else {
                buf.putInt(0);
            }
        }
    }

    public static TaskId readFrom(final ByteBuffer buf, final int version) {
        final int topicGroupId = buf.getInt();
        final int partition = buf.getInt();
        final String namedTopology;
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            final int numNamedTopologyChars = buf.getInt();
            final StringBuilder namedTopologyBuilder = new StringBuilder();
            for (int i = 0; i < numNamedTopologyChars; ++i) {
                namedTopologyBuilder.append(buf.getChar());
            }
            namedTopology = namedTopologyBuilder.toString();
        } else {
            namedTopology = null;
        }
        return new TaskId(topicGroupId, partition, getNamedTopologyOrElseNull(namedTopology));
    }

    /**
     * @return the namedTopology name, or null if the passed in namedTopology is null or the empty string
     */
    private static String getNamedTopologyOrElseNull(final String namedTopology) {
        return (namedTopology == null || namedTopology.length() == 0) ?
            null :
            namedTopology;
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

        if (topicGroupId != taskId.topicGroupId || partition != taskId.partition) {
            return false;
        }

        if (namedTopology != null && taskId.namedTopology != null) {
            return namedTopology.equals(taskId.namedTopology);
        } else {
            return namedTopology == null && taskId.namedTopology == null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicGroupId, partition, namedTopology);
    }

    @Override
    public int compareTo(final TaskId other) {
        if (namedTopology != null && other.namedTopology != null) {
            final int comparingNamedTopologies = namedTopology.compareTo(other.namedTopology);
            if (comparingNamedTopologies != 0) {
                return comparingNamedTopologies;
            }
        } else if (namedTopology != null || other.namedTopology != null) {
            LOG.error("Tried to compare this = {} with other = {}, but only one had a valid named topology", this, other);
            throw new IllegalStateException("Can't compare a TaskId with a namedTopology to one without");
        }
        final int comparingTopicGroupId = Integer.compare(this.topicGroupId, other.topicGroupId);
        return comparingTopicGroupId != 0 ? comparingTopicGroupId : Integer.compare(this.partition, other.partition);
    }
}
