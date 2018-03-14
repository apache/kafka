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

/**
 * The task ID representation composed as topic group ID plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    /** The ID of the topic group. */
    public final int topicGroupId;
    /** The ID of the partition. */
    public final int partition;
    public final boolean isPrepareStoreForUpgradeTask;

    public TaskId(final TaskId taskId) {
        this(taskId.topicGroupId, taskId.partition, true);
    }

    public TaskId(final int topicGroupId,
                  final int partition) {
        this(topicGroupId, partition, false);
    }

    public TaskId(final int topicGroupId,
                  final int partition,
                   final boolean isPrepareStoreForUpgradeTask) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
        this.isPrepareStoreForUpgradeTask = isPrepareStoreForUpgradeTask;
    }

    @Override
    public String toString() {
        if (isPrepareStoreForUpgradeTask) {
            return topicGroupId + "_" + partition + "_prepare";
        } else {
            return topicGroupId + "_" + partition;
        }
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
     * @throws TaskIdFormatException if the string is not a valid {@link TaskId}
     */
    public static TaskId parsePrepare(final String string) {
        final String[] tokens = string.split("_");
        if (tokens.length != 3 || tokens[0].length() == 0 || tokens[1].length() == 0 || !tokens[2].equals("prepare")) {
            throw new TaskIdFormatException(string);
        }

        try {
            final int topicGroupId = Integer.parseInt(tokens[0]);
            final int partition = Integer.parseInt(tokens[1]);

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
            return other.topicGroupId == this.topicGroupId
                && other.partition == this.partition
                && other.isPrepareStoreForUpgradeTask == this.isPrepareStoreForUpgradeTask;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicGroupId, partition, isPrepareStoreForUpgradeTask);
    }

    @Override
    public int compareTo(final TaskId other) {
        return
            this.topicGroupId < other.topicGroupId ? -1 :
                (this.topicGroupId > other.topicGroupId ? 1 :
                    (this.partition < other.partition ? -1 :
                        (this.partition > other.partition ? 1 :
                            this.isPrepareStoreForUpgradeTask == other.isPrepareStoreForUpgradeTask ? 0 :
                                this.isPrepareStoreForUpgradeTask ? 1 : -1)));
    }

}
