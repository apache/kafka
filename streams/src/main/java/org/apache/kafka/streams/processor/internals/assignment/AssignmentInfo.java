/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.streams.state.HostInfo;
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
    /**
     * A new field was added, partitionsByHostState. CURRENT_VERSION
     * is required so we can decode the previous version. For example, this may occur
     * during a rolling upgrade
     */
    private static final int CURRENT_VERSION = 2;
    public final int version;
    public final List<TaskId> activeTasks; // each element corresponds to a partition
    public final Map<TaskId, Set<TopicPartition>> standbyTasks;
    public final Map<HostInfo, Set<TopicPartition>> partitionsByHostState;

    public AssignmentInfo(List<TaskId> activeTasks, Map<TaskId, Set<TopicPartition>> standbyTasks,
                          Map<HostInfo, Set<TopicPartition>> hostState) {
        this(CURRENT_VERSION, activeTasks, standbyTasks, hostState);
    }

    protected AssignmentInfo(int version, List<TaskId> activeTasks, Map<TaskId, Set<TopicPartition>> standbyTasks,
                             Map<HostInfo, Set<TopicPartition>> hostState) {
        this.version = version;
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.partitionsByHostState = hostState;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
     * IO exception during encoding
     */
    public ByteBuffer encode() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        try {
            // Encode version
            out.writeInt(version);
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
                writeTopicPartitions(out, partitions);
            }
            out.writeInt(partitionsByHostState.size());
            for (Map.Entry<HostInfo, Set<TopicPartition>> entry : partitionsByHostState
                    .entrySet()) {
                final HostInfo hostInfo = entry.getKey();
                out.writeUTF(hostInfo.host());
                out.writeInt(hostInfo.port());
                writeTopicPartitions(out, entry.getValue());
            }

            out.flush();
            out.close();

            return ByteBuffer.wrap(baos.toByteArray());
        } catch (IOException ex) {
            throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
        }
    }

    private void writeTopicPartitions(DataOutputStream out, Set<TopicPartition> partitions) throws IOException {
        out.writeInt(partitions.size());
        for (TopicPartition partition : partitions) {
            out.writeUTF(partition.topic());
            out.writeInt(partition.partition());
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
            if (version < 0 || version > CURRENT_VERSION) {
                TaskAssignmentException ex = new TaskAssignmentException("Unknown assignment data version: " + version);
                log.error(ex.getMessage(), ex);
                throw ex;
            }

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
                standbyTasks.put(id, readTopicPartitions(in));
            }

            Map<HostInfo, Set<TopicPartition>> hostStateToTopicPartitions = new HashMap<>();
            if (version == CURRENT_VERSION) {
                int numEntries = in.readInt();
                for (int i = 0; i < numEntries; i++) {
                    HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
                    hostStateToTopicPartitions.put(hostInfo, readTopicPartitions(in));
                }
            }

            return new AssignmentInfo(activeTasks, standbyTasks, hostStateToTopicPartitions);


        } catch (IOException ex) {
            throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
        }
    }

    private static Set<TopicPartition> readTopicPartitions(DataInputStream in) throws IOException {
        int numPartitions = in.readInt();
        Set<TopicPartition> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++) {
            partitions.add(new TopicPartition(in.readUTF(), in.readInt()));
        }
        return partitions;
    }

    @Override
    public int hashCode() {
        return version ^ activeTasks.hashCode() ^ standbyTasks.hashCode() ^ partitionsByHostState.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AssignmentInfo) {
            AssignmentInfo other = (AssignmentInfo) o;
            return this.version == other.version &&
                    this.activeTasks.equals(other.activeTasks) &&
                    this.standbyTasks.equals(other.standbyTasks) &&
                    this.partitionsByHostState.equals(other.partitionsByHostState);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + version + ", active tasks=" + activeTasks.size() + ", standby tasks=" + standbyTasks.size() + "]";
    }

}
