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

    public TaskId(final int topicGroupId, final int partition) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
    }

    public String toString() {
        return topicGroupId + "_" + partition;
    }

    /**
     * @throws TaskIdFormatException if the string is not a valid {@link TaskId}
     */
    public static TaskId parse(final String string) {
        final int index = string.indexOf('_');
        if (index <= 0 || index + 1 >= string.length()) throw new TaskIdFormatException(string);

        try {
            final int topicGroupId = Integer.parseInt(string.substring(0, index));
            final int partition = Integer.parseInt(string.substring(index + 1));

            return new TaskId(topicGroupId, partition);
        } catch (final Exception e) {
            throw new TaskIdFormatException(string);
        }
    }

    /**
     * @throws IOException if cannot write to output stream
     */
    public void writeTo(final DataOutputStream out) throws IOException {
        out.writeInt(topicGroupId);
        out.writeInt(partition);
    }

    /**
     * @throws IOException if cannot read from input stream
     */
    public static TaskId readFrom(final DataInputStream in) throws IOException {
        return new TaskId(in.readInt(), in.readInt());
    }

    public void writeTo(final ByteBuffer buf) {
        buf.putInt(topicGroupId);
        buf.putInt(partition);
    }

    public static TaskId readFrom(final ByteBuffer buf) {
        return new TaskId(buf.getInt(), buf.getInt());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;

        if (o instanceof TaskId) {
            final TaskId other = (TaskId) o;
            return other.topicGroupId == this.topicGroupId && other.partition == this.partition;
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
        return
            this.topicGroupId < other.topicGroupId ? -1 :
                (this.topicGroupId > other.topicGroupId ? 1 :
                    (this.partition < other.partition ? -1 :
                        (this.partition > other.partition ? 1 :
                            0)));
    }
}
