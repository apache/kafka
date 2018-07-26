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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.streams.errors.TaskIdFormatException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Map;
import java.util.HashMap;

/**
 * The task ID representation composed as topic group ID plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    /** The ID of the topic group. */
    public final int topicGroupId;
    /** The ID of the partition. */
    public final int partition;
    /** The number of State Stores in the task. */
    private int numberOfStateStores;
    /** The number of Partitions in the task.*/
    private int numberOfInputPartitions;
    /** The beginning offset of the log being processed. */
    private Map<TopicPartition, Long> beginningOffsets;
    /** The last committed offset of the log being processed. */
    private Map<TopicPartition, Long> lastCommittedOffsets;
    /** The end offset of the log being processed. */
    private Map<TopicPartition, Long> endOffsets;
    
    public TaskId(int topicGroupId, int partition) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
        this.numberOfStateStores = 0;
        this.numberOfInputPartitions = 0;
        this.beginningOffsets = new HashMap<>();
        this.lastCommittedOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
    }

    public String toString() {
        return topicGroupId + "_" + partition;
    }

    /**
     * @throws TaskIdFormatException if the string is not a valid {@link TaskId}
     */
    public static TaskId parse(String string) {
        int index = string.indexOf('_');
        if (index <= 0 || index + 1 >= string.length()) throw new TaskIdFormatException(string);

        try {
            int topicGroupId = Integer.parseInt(string.substring(0, index));
            int partition = Integer.parseInt(string.substring(index + 1));

            return new TaskId(topicGroupId, partition);
        } catch (Exception e) {
            throw new TaskIdFormatException(string);
        }
    }

    /**
     * @throws IOException if cannot write to output stream
     */
    public void writeTo(DataOutputStream out, int usedVersion) throws IOException {
        out.writeInt(topicGroupId);
        out.writeInt(partition);
        if (usedVersion == 4) {
            out.writeInt(numberOfStateStores);
            out.writeInt(numberOfInputPartitions);
            final int offsetsSize = beginningOffsets.size();
            out.writeInt(offsetsSize);
            for (final Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                final int topicSize = entry.getKey().topic().length();
                out.writeInt(topicSize);
                out.writeChars(entry.getKey().topic());
                out.writeInt(entry.getKey().partition());
                out.writeLong(entry.getValue());
                out.writeLong(lastCommittedOffsets.get(entry.getKey()));
                out.writeLong(endOffsets.get(entry.getKey()));
            }
        }
    }

    /**
     * @throws IOException if cannot read from input stream
     */
    public static TaskId readFrom(DataInputStream in, int usedVersion) throws IOException {
        final TaskId taskId = new TaskId(in.readInt(), in.readInt());
        if (usedVersion == 4) {
            taskId.setNumberOfStateStores(in.readInt());
            taskId.setNumberOfInputPartitions(in.readInt());
            final int offsetsSize = in.readInt();
            for (int i = 0; i < offsetsSize; i++) {
                final int topicLength = in.readInt();
                String topicName = "";
                for (int j = 0; j < topicLength; j++) {
                    topicName += in.readChar();
                }
                final int partitionId = in.readInt();
                final TopicPartition partition = new TopicPartition(topicName, partitionId);
                taskId.addBeginningOffset(partition, in.readInt());
                taskId.addLastCommittedOffset(partition, in.readInt());
                taskId.addEndOffset(partition, in.readInt());
            }
        }
        return taskId;
    }

    public void writeTo(ByteBuffer buf, final int version) {
        buf.putInt(topicGroupId);
        buf.putInt(partition);
        if (version == 4) {
            buf.putInt(numberOfStateStores);
            buf.putInt(numberOfInputPartitions);
            final int offsetsSize = beginningOffsets.size();
            buf.putInt(offsetsSize);
            for (final Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                final int topicSize = entry.getKey().topic().length();
                buf.putInt(topicSize);
                for (int i = 0; i < topicSize; i++) {
                    buf.putChar(entry.getKey().topic().charAt(i));
                }
                buf.putInt(entry.getKey().partition());
                buf.putLong(entry.getValue());
                buf.putLong(lastCommittedOffsets.get(entry.getKey()));
                buf.putLong(endOffsets.get(entry.getKey()));
            }
        }
    }

    public static TaskId readFrom(ByteBuffer buf, final int version) {
        final TaskId result = new TaskId(buf.getInt(), buf.getInt());
        if (version == 4) {
            result.setNumberOfStateStores(buf.getInt());
            result.setNumberOfInputPartitions(buf.getInt());
            final int offsetsSize = buf.getInt();
            for (int i = 0; i < offsetsSize; i++) {
                final int topicLength = buf.getInt();
                String topicName = "";
                for (int j = 0; j < topicLength; j++) {
                    topicName += buf.getChar();
                }
                final int partitionId = buf.getInt();
                final TopicPartition partition = new TopicPartition(topicName, partitionId);
                result.addBeginningOffset(partition, buf.getInt());
                result.addLastCommittedOffset(partition, buf.getInt());
                result.addEndOffset(partition, buf.getInt());
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o instanceof TaskId) {
            TaskId other = (TaskId) o;
            return other.topicGroupId == this.topicGroupId && other.partition == this.partition && 
                   other.numberOfInputPartitions == this.numberOfInputPartitions &&
                   other.numberOfStateStores == this.numberOfStateStores;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        long n = ((long) topicGroupId << 32) | (long) partition;
        return (int) (n % 0xFFFFFFFFL);
    }

    @Override
    public int compareTo(TaskId other) {
        return
            this.topicGroupId < other.topicGroupId ? -1 :
                (this.topicGroupId > other.topicGroupId ? 1 :
                    (this.partition < other.partition ? -1 :
                        (this.partition > other.partition ? 1 :
                            0)));
    }

    public int numberOfStateStores() {
        return numberOfStateStores;
    }

    public void setNumberOfStateStores(final int numberOfStateStores) {
        this.numberOfStateStores = numberOfStateStores;
    }

    public int numberOfInputPartitions() {
        return numberOfInputPartitions;
    }

    public void setNumberOfInputPartitions(final int numberOfInputPartitions) {
        this.numberOfInputPartitions = numberOfInputPartitions;
    }

    public Map<TopicPartition, Long> beginningOffsets() { return beginningOffsets; }

    public void setBeginningOffset(final Map<TopicPartition, Long> beginningOffsets) {
        this.beginningOffsets = beginningOffsets;
    }

    public Map<TopicPartition, Long> lastCommittedOffset() { return lastCommittedOffsets; }

    public void setLastCommittedOffset(final Map<TopicPartition, Long> lastCommittedOffsets) {
        this.lastCommittedOffsets = lastCommittedOffsets;
    }

    public Map<TopicPartition, Long> endOffsets() { return endOffsets; }

    public void setEndOffsets(final Map<TopicPartition, Long> endOffsets) {
        this.endOffsets = endOffsets;
    }

    public void addBeginningOffset(TopicPartition partition, long beginningOffset) {
        this.beginningOffsets.put(partition, beginningOffset);
    }

    public void addLastCommittedOffset(TopicPartition partition, long lastCommittedOffset) {
        this.lastCommittedOffsets.put(partition, lastCommittedOffset);
    }

    public void addEndOffset(TopicPartition partition, long endOffset) {
        this.endOffsets.put(partition, endOffset);
    }

    public int getByteBufferLength() {
        // topicGroupId, partitionId, stateStoreCount, inputPartitionCount, keyCount (type integer)
        int initialBase = 20;
        // represents number of characters per string (type integer)
        initialBase += beginningOffsets.size() << 2;
        // adds how long each string will be
        for (final TopicPartition partition : beginningOffsets.keySet()) {
            initialBase += partition.topic().length() * 2;
        }
        // beginningOffset, lastCommittedOffset, endOffset (all type long)
        initialBase += beginningOffsets.size() * 3 * 8;
        return initialBase;
    }
}
