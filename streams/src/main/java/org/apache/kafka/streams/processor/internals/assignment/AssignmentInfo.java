/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.record.ByteBufferInputStream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssignmentInfo {

    private static final Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    public final int version;
    public final List<TaskId> activeTasks; // each element corresponds to a partition
    public final Map<TaskId, Set<TopicPartition>> standbyTasks;

    public AssignmentInfo(List<TaskId> activeTasks, Map<TaskId, Set<TopicPartition>> standbyTasks) {
        this(1, activeTasks, standbyTasks);
    }

    protected AssignmentInfo(int version, List<TaskId> activeTasks, Map<TaskId, Set<TopicPartition>> standbyTasks) {
        this.version = version;
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
     * IO exception during encoding
     */
    public ByteBuffer encode() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        try {
            if (version == 1) {
                // Encode version
                out.writeInt(1);
                // Encode active tasks
                out.writeInt(activeTasks.size());
                for (TaskId id : activeTasks) {
                    id.writeTo(out);
                }
                // Encode standby tasks
                out.writeInt(standbyTasks.size());
                for (Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
                    TaskId id = entry.getKey();
                    id.writeTo(out);

                    Set<TopicPartition> partitions = entry.getValue();
                    out.writeInt(partitions.size());
                    for (TopicPartition partition : partitions) {
                        out.writeUTF(partition.topic());
                        out.writeInt(partition.partition());
                    }
                }

                out.flush();
                out.close();

                return ByteBuffer.wrap(baos.toByteArray());

            } else {
                TaskAssignmentException ex = new TaskAssignmentException("Unable to encode assignment data: version=" + version);
                log.error(ex.getMessage(), ex);
                throw ex;
            }
        } catch (IOException ex) {
            throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
        }
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
     */
    public static AssignmentInfo decode(ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();
        DataInputStream in = new DataInputStream(new ByteBufferInputStream(data));

        try {
            // Decode version
            int version = in.readInt();
            if (version == 1) {
                // Decode active tasks
                int count = in.readInt();
                List<TaskId> activeTasks = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    activeTasks.add(TaskId.readFrom(in));
                }
                // Decode standby tasks
                count = in.readInt();
                Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>(count);
                for (int i = 0; i < count; i++) {
                    TaskId id = TaskId.readFrom(in);

                    int numPartitions = in.readInt();
                    Set<TopicPartition> partitions = new HashSet<>(numPartitions);
                    for (int j = 0; j < numPartitions; j++) {
                        partitions.add(new TopicPartition(in.readUTF(), in.readInt()));
                    }
                    standbyTasks.put(id, partitions);
                }

                return new AssignmentInfo(activeTasks, standbyTasks);

            } else {
                TaskAssignmentException ex = new TaskAssignmentException("Unknown assignment data version: " + version);
                log.error(ex.getMessage(), ex);
                throw ex;
            }
        } catch (IOException ex) {
            throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
        }
    }

    @Override
    public int hashCode() {
        return version ^ activeTasks.hashCode() ^ standbyTasks.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AssignmentInfo) {
            AssignmentInfo other = (AssignmentInfo) o;
            return this.version == other.version &&
                    this.activeTasks.equals(other.activeTasks) &&
                    this.standbyTasks.equals(other.standbyTasks);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + version + ", active tasks=" + activeTasks.size() + ", standby tasks=" + standbyTasks.size() + "]";
    }

}
