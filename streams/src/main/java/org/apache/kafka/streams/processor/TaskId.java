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

/**
 * The task ID representation composed as topic group ID plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    /** The ID of the topic group. */
    public final int topicGroupId;
    /** The ID of the partition. */
    public final int partition;
    /** Relevant information regarding TaskId */
    public final StreamTaskMetadata metadata;

    public TaskId(final int topicGroupId, final int partition) {
        this(topicGroupId, partition, new StreamTaskMetadata(-1, -1));
    }

    public TaskId(final int topicGroupId, 
                  final int partition, 
                  final StreamTaskMetadata metadata) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
        this.metadata = metadata;
    }

    public String toString() {
        return topicGroupId + "_" + partition;
    }

    /**
     * @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
     */
    public static TaskId parse(final String taskIdStr) {
        final int index = taskIdStr.indexOf('_');
        if (index <= 0 || index + 1 >= taskIdStr.length()) throw new TaskIdFormatException(taskIdStr);

        try {
            final int topicGroupId = Integer.parseInt(taskIdStr.substring(0, index));
            final int partition = Integer.parseInt(taskIdStr.substring(index + 1));

            return new TaskId(topicGroupId, partition);
        } catch (final Exception e) {
            throw new TaskIdFormatException(taskIdStr);
        }
    }

    /**
     * @throws IOException if cannot write to output stream
     */
    public void writeTo(final DataOutputStream out, final int usedVersion) throws IOException {
        out.writeInt(topicGroupId);
        out.writeInt(partition);
        if (usedVersion == 4) {
            metadata.writeTo(out);
        }
    }

    /**
     * @throws IOException if cannot read from input stream
     */
    public static TaskId readFrom(final DataInputStream in, final int usedVersion) throws IOException {
        final int groupId = in.readInt();
        final int partition = in.readInt();
        if (usedVersion == 4) {
            final StreamTaskMetadata metadata = StreamTaskMetadata.readFrom(in);
            return new TaskId(groupId, partition, metadata);
        }
        return new TaskId(groupId, partition);
    }

    public void writeTo(final ByteBuffer buf, final int version) {
        buf.putInt(topicGroupId);
        buf.putInt(partition);
        if (version == 4) {
            metadata.writeTo(buf);
        }
    }

    public static TaskId readFrom(final ByteBuffer buf, final int version) {
        final int groupId = buf.getInt();
        final int partition = buf.getInt();
        if (version == 4) {
            final StreamTaskMetadata metadata = StreamTaskMetadata.readFrom(buf);
            return new TaskId(groupId, partition, metadata);
        }
        return new TaskId(groupId, partition);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;

        if (o instanceof TaskId) {
            final TaskId other = (TaskId) o;
            return other.topicGroupId == this.topicGroupId && 
                   other.partition == this.partition && 
                   other.numberOfInputPartitions() == numberOfInputPartitions() &&
                   other.numberOfStateStores() == numberOfStateStores();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final long n = ((long) topicGroupId << 32) | (long) partition;
        return (int) (n % 0xFFFFFFFFL);
    }

    @Override
    public int compareTo(final TaskId other) {
        final int compare = Integer.compare(this.topicGroupId, other.topicGroupId);
        return compare != 0 ? compare : Integer.compare(this.partition, other.partition);
    }

    public int numberOfStateStores() {
        return metadata.numberOfStateStores();
    }

    public int numberOfInputPartitions() {
        return metadata.numberOfPartitions();
    }
}
