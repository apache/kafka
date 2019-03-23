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
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AssignmentInfo {

    private static final Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    public static final int LATEST_SUPPORTED_VERSION = 5;
    static final int UNKNOWN = -1;

    private final int usedVersion;
    private final int latestSupportedVersion;
    private int errCode;
    private List<TaskId> activeTasks;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;
    private Map<HostInfo, Set<TaskId>> tasksByHost;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHost;

    // used for decoding; don't apply version checks
    private AssignmentInfo(final int version,
                           final int latestSupportedVersion) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.errCode = 0;
    }

    public AssignmentInfo(final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TaskId>> tasksByHost) {
        this(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, tasksByHost, Collections.emptyMap(), 0);
    }

    public AssignmentInfo() {
        this(LATEST_SUPPORTED_VERSION,
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            0);
    }

    public Map<HostInfo, Set<TopicPartition>> getPartitionsByHost() {
        return partitionsByHost;
    }

    public AssignmentInfo(final int version,
                          final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TaskId>> tasksByHost,
                          final int errCode) {
        this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, tasksByHost, Collections.emptyMap(), errCode);
        if (version < 1 || version > LATEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                    + "; was: " + version);
        }
    }

    public AssignmentInfo(final int version,
                          final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TaskId>> tasksByHost,
                          final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                          final int errCode) {
        this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, tasksByHost, partitionsByHost, errCode);
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
                   final Map<HostInfo, Set<TaskId>> tasksByHost,
                   final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                   final int errCode) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.tasksByHost = tasksByHost;
        this.partitionsByHost = partitionsByHost;
        this.errCode = errCode;
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

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }

    public Map<HostInfo, Set<TaskId>> tasksByHost() {
        return tasksByHost;
    }

    private byte[] toBytes(final int i) {
        final byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) i /*>> 0*/;
        return result;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
     * IO exception during encoding
     */
    public ByteBuffer encode() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(toBytes(usedVersion));
        } catch (final IOException ex) {
            throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
        }

        final DataOutputStream out;
        try {
            if (usedVersion >= 5) {
                out = new DataOutputStream(new GZIPOutputStream(baos));
            } else {
                out = new DataOutputStream(baos);
            }
        } catch (final IOException ex) {
            throw new IllegalStateException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
        }
        try {
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
                case 5:
                    encodeVersionFive(out);
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
        encodeActiveAndStandbyTaskAssignment(out);
    }

    private void encodeActiveAndStandbyTaskAssignment(final DataOutputStream out) throws IOException {
        // encode active tasks
        out.writeInt(activeTasks.size());
        for (final TaskId id : activeTasks) {
            id.writeTo(out);
        }

        // encode standby tasks
        out.writeInt(standbyTasks.size());
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
            final TaskId id = entry.getKey();
            id.writeTo(out);

            final Set<TopicPartition> partitions = entry.getValue();
            writeTopicPartitions(out, partitions);
        }
    }

    private void encodeVersionTwo(final DataOutputStream out) throws IOException {
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

    private void encodeTasksByHost(final DataOutputStream out) throws IOException {
        // encode tasks by host
        out.writeInt(tasksByHost.size());
        for (final Map.Entry<HostInfo, Set<TaskId>> entry : tasksByHost.entrySet()) {
            final HostInfo hostInfo = entry.getKey();
            out.writeUTF(hostInfo.host());
            out.writeInt(hostInfo.port());
            writeTaskIds(out, entry.getValue());
        }
    }

    private void writeTaskIds(final DataOutputStream out, final Set<TaskId> partitions) throws IOException {
        out.writeInt(partitions.size());
        for (final TaskId partition : partitions) {
            out.writeInt(partition.topicGroupId);
            out.writeInt(partition.partition);
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
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
    }

    private void encodeVersionFour(final DataOutputStream out) throws IOException {
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
        out.writeInt(errCode);
    }

    private void encodeVersionFive(final DataOutputStream out) throws IOException {
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodeTasksByHost(out);
        out.writeInt(errCode);
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
     */
    public static AssignmentInfo decode(final ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        try  {
            final ByteBufferInputStream bios = new ByteBufferInputStream(data);
            final DataInputStream in = new DataInputStream(bios);
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
                case 5:
                    final DataInputStream compressedIn = new DataInputStream(new GZIPInputStream(bios));
                    latestSupportedVersion = compressedIn.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
                    decodeVersionFiveData(assignmentInfo, compressedIn);
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
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        assignmentInfo.tasksByHost = new HashMap<>();
        assignmentInfo.partitionsByHost = new HashMap<>();
    }

    private static void decodeActiveTasks(final AssignmentInfo assignmentInfo,
                                          final DataInputStream in) throws IOException {
        final int count = in.readInt();
        assignmentInfo.activeTasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            assignmentInfo.activeTasks.add(TaskId.readFrom(in));
        }
    }

    private static void decodeStandbyTasks(final AssignmentInfo assignmentInfo,
                                           final DataInputStream in) throws IOException {
        final int count = in.readInt();
        assignmentInfo.standbyTasks = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            final TaskId id = TaskId.readFrom(in);
            assignmentInfo.standbyTasks.put(id, readTopicPartitions(in));
        }
    }

    private static void decodeVersionTwoData(final AssignmentInfo assignmentInfo,
                                             final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        assignmentInfo.tasksByHost = new HashMap<>();
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

    private static void decodeGlobalTasksAssignmentData(final AssignmentInfo assignmentInfo,
                                                   final DataInputStream in) throws IOException {
        assignmentInfo.tasksByHost = new HashMap<>();
        final int numEntries = in.readInt();
        for (int i = 0; i < numEntries; i++) {
            final HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
            assignmentInfo.tasksByHost.put(hostInfo, readTaskIds(in));
        }
    }

    private static Set<TaskId> readTaskIds(final DataInputStream in) throws IOException {
        final int numPartitions = in.readInt();
        final Set<TaskId> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++) {
            partitions.add(new TaskId(in.readInt(), in.readInt()));
        }
        return partitions;
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
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        assignmentInfo.tasksByHost = new HashMap<>();
        decodeGlobalAssignmentData(assignmentInfo, in);
    }

    private static void decodeVersionFourData(final AssignmentInfo assignmentInfo,
                                              final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        assignmentInfo.tasksByHost = new HashMap<>();
        decodeGlobalAssignmentData(assignmentInfo, in);
        assignmentInfo.errCode = in.readInt();
    }

    private static void decodeVersionFiveData(final AssignmentInfo assignmentInfo,
                                              final DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        decodeGlobalTasksAssignmentData(assignmentInfo, in);
        assignmentInfo.partitionsByHost = new HashMap<>();
        assignmentInfo.errCode = in.readInt();
    }

    @Override
    public int hashCode() {
        final int result = usedVersion ^ latestSupportedVersion ^ activeTasks.hashCode() ^ standbyTasks.hashCode() ^ errCode;
        return result ^ tasksByHost.hashCode() ^ partitionsByHost.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof AssignmentInfo) {
            final AssignmentInfo other = (AssignmentInfo) o;
            final boolean result = usedVersion == other.usedVersion &&
                    latestSupportedVersion == other.latestSupportedVersion &&
                    errCode == other.errCode &&
                    activeTasks.equals(other.activeTasks) &&
                    standbyTasks.equals(other.standbyTasks) &&
                    partitionsByHost.equals(other.partitionsByHost);

            return result && tasksByHost.equals(other.tasksByHost);
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
            + ", tasksByHosts=" + tasksByHost
            + ", partitionsByHost=" + partitionsByHost + "]";
    }

}
