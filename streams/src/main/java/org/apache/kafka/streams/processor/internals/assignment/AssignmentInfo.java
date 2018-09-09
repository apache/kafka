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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TaskMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssignmentInfo {

    private static final Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    public static final int LATEST_SUPPORTED_VERSION = 4;
    static final int UNKNOWN = -1;

    private final int usedVersion;
    private final int latestSupportedVersion;
    private int errCode;
    private List<TaskId> activeTasks;
    private List<TaskMetadata> activeTaskMetadata;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;
    private Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHost;

    // used for decoding; don't apply version checks
    private AssignmentInfo(final int version,
                           final int latestSupportedVersion) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.errCode = 0;
    }

    public AssignmentInfo(final int usedVersion,
                          final List<TaskMetadata> activeTaskMetadata,
                          final Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata,
                          final Map<HostInfo, Set<TopicPartition>> hostState) {
        this(usedVersion, LATEST_SUPPORTED_VERSION, null, null, activeTaskMetadata, standbyTaskMetadata, hostState, 0);
    }

    public AssignmentInfo(final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TopicPartition>> hostState) {
        this(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, 0);
    }

    public AssignmentInfo() {
        this(LATEST_SUPPORTED_VERSION,
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            0);
    }

    public AssignmentInfo(final int version,
                          final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TopicPartition>> hostState,
                          final int errCode) {
        this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, null, null, hostState, errCode);
        if (version < 1 || version > LATEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                + "; was: " + version);
        }
    }

    // for testing only; don't apply version checks
    AssignmentInfo(final int version,
                   final int latestSupportedVersion,
                   final List<TaskId> activeTasks,
                   final Map<TaskId, Set<TopicPartition>> standbyTasks,
                   final List<TaskMetadata> activeTaskMetadata,
                   final Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata,
                   final Map<HostInfo, Set<TopicPartition>> hostState,
                   final int errCode) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        if (version < 4) {
            this.activeTasks = activeTasks == null ? castActiveTaskToTaskId(activeTaskMetadata) : activeTasks;
            this.standbyTasks = standbyTasks == null ? castStandbyToTaskId(standbyTaskMetadata) : standbyTasks;
        } else {
            this.activeTaskMetadata = activeTaskMetadata == null ? castActiveTaskToTaskMetadata(activeTasks) : activeTaskMetadata;
            this.standbyTaskMetadata = standbyTaskMetadata == null ? castStandbyToTaskMetadata(standbyTasks) : standbyTaskMetadata;
        }
        this.partitionsByHost = hostState;
        this.errCode = errCode;
    }

    private Map<TaskMetadata, Set<TopicPartition>> castStandbyToTaskMetadata(
            final Map<TaskId, Set<TopicPartition>> standbyTasksMap) {
        final Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> metadata : standbyTasksMap.entrySet()) {
            standbyTaskMetadata.put(new TaskMetadata(metadata.getKey(), 0, 0), 
                                    metadata.getValue());
        }
        return standbyTaskMetadata;
    }

    private Map<TaskId, Set<TopicPartition>> castStandbyToTaskId(
            final Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata) {
        final Map<TaskId, Set<TopicPartition>> standbyTaskMap = new HashMap<>();
        for (final Map.Entry<TaskMetadata, Set<TopicPartition>> metadata : standbyTaskMetadata.entrySet()) {
            standbyTaskMap.put(metadata.getKey().taskId, 
                               metadata.getValue());
        }
        return standbyTaskMap;
    }

    private List<TaskMetadata> castActiveTaskToTaskMetadata(final List<TaskId> activeTaskList) {
        final List<TaskMetadata> result = new ArrayList<>();
        for (final TaskId taskId : activeTaskList) {
            result.add(new TaskMetadata(taskId, 0, 0));
        }
        return result;
    }

    private List<TaskId> castActiveTaskToTaskId(final List<TaskMetadata> activeTaskList) {
        final List<TaskId> result = new ArrayList<>();
        for (final TaskMetadata taskId : activeTaskList) {
            result.add(taskId.taskId);
        }
        return result;
    }

    public int version() {
        return usedVersion;
    }

    public int errCode() {
        return errCode;
    }

    public int latestSupportedVersion() {
        return latestSupportedVersion;
    }

    public List<TaskId> activeTasks() {
        return activeTasks;
    }

    public List<TaskMetadata> activeTaskMetadata() {
        return activeTaskMetadata;
    }

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }

    public Map<TaskMetadata, Set<TopicPartition>> standbyTaskMetadata() {
        return standbyTaskMetadata;
    }

    public Map<HostInfo, Set<TopicPartition>> partitionsByHost() {
        return partitionsByHost;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
     * IO exception during encoding
     */
    public ByteBuffer encode() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final DataOutputStream out = new DataOutputStream(baos)) {
            switch (usedVersion) {
                case 1:
                    encodeVersionOne(out);
                    break;
                case 2:
                    encodeVersionTwo(out);
                    break;
                case 3:
                    encodeVersionThree(out);
                    break;
                case 4:
                    encodeVersionFour(out);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata version: " + usedVersion
                        + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
            }

            out.flush();
            out.close();

            return ByteBuffer.wrap(baos.toByteArray());
        } catch (final IOException ex) {
            throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
        }
    }

    private void encodeVersionOne(final DataOutputStream out) throws IOException {
        out.writeInt(1); // version
        encodeActiveAndStandbyTaskAssignment(out);
    }

    private void encodeActiveAndStandbyTaskAssignment(final DataOutputStream out) throws IOException {
        this.encodeActiveAndStandbyTaskAssignment(out, false);
    }

    private void encodeActiveAndStandbyTaskAssignment(final DataOutputStream out, final boolean isVersionFourOrAbove) throws IOException {
        // encode active tasks*
        if (isVersionFourOrAbove) {
            out.writeInt(activeTaskMetadata.size());
            for (final TaskMetadata metadata : activeTaskMetadata) {
                metadata.writeTo(out);
            } 
        } else {
            out.writeInt(activeTasks.size());
            for (final TaskId id : activeTasks) {
                id.writeTo(out);
            }
        }

        // encode standby tasks
        if (isVersionFourOrAbove) {
            out.writeInt(standbyTaskMetadata.size());
            for (final Map.Entry<TaskMetadata, Set<TopicPartition>> entry : standbyTaskMetadata.entrySet()) {
                entry.getKey().writeTo(out);

                final Set<TopicPartition> partitions = entry.getValue();
                writeTopicPartitions(out, partitions);
            }
        } else {
            out.writeInt(standbyTasks.size());
            for (final Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
                entry.getKey().writeTo(out);

                final Set<TopicPartition> partitions = entry.getValue();
                writeTopicPartitions(out, partitions);
            }
        }
    }

    private void encodeVersionTwo(final DataOutputStream out) throws IOException {
        out.writeInt(2); // version
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
    }

    private void encodePartitionsByHost(final DataOutputStream out) throws IOException {
        // encode partitions by host
        out.writeInt(partitionsByHost.size());
        for (final Map.Entry<HostInfo, Set<TopicPartition>> entry : partitionsByHost.entrySet()) {
            final HostInfo hostInfo = entry.getKey();
            out.writeUTF(hostInfo.host());
            out.writeInt(hostInfo.port());
            writeTopicPartitions(out, entry.getValue());
        }
    }

    private void writeTopicPartitions(final DataOutputStream out,
                                      final Set<TopicPartition> partitions) throws IOException {
        out.writeInt(partitions.size());
        for (final TopicPartition partition : partitions) {
            out.writeUTF(partition.topic());
            out.writeInt(partition.partition());
        }
    }

    private void encodeVersionThree(final DataOutputStream out) throws IOException {
        out.writeInt(3);
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
    }

    private void encodeVersionFour(final DataOutputStream out) throws IOException {
        out.writeInt(4);
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out, true);
        encodePartitionsByHost(out);
        out.writeInt(errCode);
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
     */
    public static AssignmentInfo decode(final ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        try (final DataInputStream in = new DataInputStream(new ByteBufferInputStream(data))) {
            final AssignmentInfo assignmentInfo;

            final int usedVersion = in.readInt();

            final int latestSupportedVersion;
          
            switch (usedVersion) {
                case 1:
                    assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                    decodeVersionOneData(assignmentInfo, in);
                    break;
                case 2:
                    assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                    decodeVersionTwoData(assignmentInfo, in);
                    break;
                case 3:
                    latestSupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
                    decodeVersionThreeData(assignmentInfo, in);
                    break;
                case 4:
                    latestSupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
                    decodeVersionFourData(assignmentInfo, in);
                    break;
                default:
                    final TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode assignment data: " +
                        "used version: " + usedVersion + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
                    log.error(fatalException.getMessage(), fatalException);
                    throw fatalException;
            }

            return assignmentInfo;
        } catch (final IOException ex) {
            throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
        }
    }

    private static void decodeVersionOneData(final AssignmentInfo assignmentInfo,
                                             final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in, 1);
        decodeStandbyTasks(assignmentInfo, in, 1);
        assignmentInfo.partitionsByHost = new HashMap<>();
    }

    private static void decodeActiveTasks(final AssignmentInfo assignmentInfo,
                                          final DataInputStream in,
                                          final int usedVersion) throws IOException {
        final int count = in.readInt();
        assignmentInfo.activeTasks = usedVersion >= 4 ? null : new ArrayList<TaskId>(count);
        assignmentInfo.activeTaskMetadata = usedVersion >= 4 ? new ArrayList<TaskMetadata>(count) : null;
        for (int i = 0; i < count; i++) {
            if (usedVersion >= 4) {
                assignmentInfo.activeTaskMetadata.add(TaskMetadata.readFrom(in));
            } else {
                assignmentInfo.activeTasks.add(TaskId.readFrom(in));
            }
        }
    }

    private static void decodeStandbyTasks(final AssignmentInfo assignmentInfo,
                                           final DataInputStream in,
                                           final int usedVersion) throws IOException {
        final int count = in.readInt();
        assignmentInfo.standbyTasks = usedVersion >= 4 ? null : new HashMap<>(count);
        assignmentInfo.standbyTaskMetadata = usedVersion >= 4 ? new HashMap<>(count) : null;
        for (int i = 0; i < count; i++) {
            if (usedVersion >= 4) {
                final TaskMetadata metadata = TaskMetadata.readFrom(in);
                assignmentInfo.standbyTaskMetadata.put(metadata, readTopicPartitions(in));
            } else {
                final TaskId id = TaskId.readFrom(in);
                assignmentInfo.standbyTasks.put(id, readTopicPartitions(in));
            }
            
        }
    }

    private static void decodeVersionTwoData(final AssignmentInfo assignmentInfo,
                                             final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in, 2);
        decodeStandbyTasks(assignmentInfo, in, 2);
        decodeGlobalAssignmentData(assignmentInfo, in);
    }

    private static void decodeGlobalAssignmentData(final AssignmentInfo assignmentInfo,
                                                   final DataInputStream in) throws IOException {
        assignmentInfo.partitionsByHost = new HashMap<>();
        final int numEntries = in.readInt();
        for (int i = 0; i < numEntries; i++) {
            final HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
            assignmentInfo.partitionsByHost.put(hostInfo, readTopicPartitions(in));
        }
    }

    private static Set<TopicPartition> readTopicPartitions(final DataInputStream in) throws IOException {
        final int numPartitions = in.readInt();
        final Set<TopicPartition> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++) {
            partitions.add(new TopicPartition(in.readUTF(), in.readInt()));
        }
        return partitions;
    }

    private static void decodeVersionThreeData(final AssignmentInfo assignmentInfo,
                                               final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in, 3);
        decodeStandbyTasks(assignmentInfo, in, 3);
        decodeGlobalAssignmentData(assignmentInfo, in);
    }

    private static void decodeVersionFourData(final AssignmentInfo assignmentInfo,
                                              final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in, 4);
        decodeStandbyTasks(assignmentInfo, in, 4);
        decodeGlobalAssignmentData(assignmentInfo, in);
        assignmentInfo.errCode = in.readInt();
    }

    @Override
    public int hashCode() {
        return usedVersion ^ latestSupportedVersion ^ activeTasks.hashCode() ^ standbyTasks.hashCode()
            ^ partitionsByHost.hashCode() ^ errCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof AssignmentInfo) {
            final AssignmentInfo other = (AssignmentInfo) o;
            return usedVersion == other.usedVersion &&
                    latestSupportedVersion == other.latestSupportedVersion &&
                    errCode == other.errCode &&
                    activeTasks.equals(other.activeTasks) &&
                    standbyTasks.equals(other.standbyTasks) &&
                    partitionsByHost.equals(other.partitionsByHost);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + usedVersion
            + ", supported version=" + latestSupportedVersion
            + ", active tasks=" + activeTasks
            + ", standby tasks=" + standbyTasks
            + ", global assignment=" + partitionsByHost + "]";
    }

}
